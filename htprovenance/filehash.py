"""In-job file hashing: record what a job read and what it wrote, by content.

HTCondor's history records file names (TransferInput, TransferOutput, Cmd,
ContainerImage) but not contents, so a reused or overwritten name can't
anchor lineage. The job's sandbox is the only place that sees the bytes, so
the job's own script calls this twice:

    htprovenance-hash inputs  [PATH...]   # after inputs arrive
    htprovenance-hash outputs [PATH...]   # before the job exits

Each call appends one files.inputs / files.outputs event to the job's
per-job NDJSON file (the one autocapture writes job.assigned to), carrying
cluster_id, proc_id and schedd so `db build` can file the hashes under the
job in the file_edges table.

With no PATH arguments the lists come from the job ad: TransferInput, the
executable (Cmd) and the container image for inputs; TransferOutput for
outputs, or -- when the job leaves output transfer at its default -- every
top-level file that is new or changed since the inputs call. PATH arguments
replace the TransferInput / TransferOutput list; the executable and
container are still recorded.

Uses only the standard library, so a job without htprovenance installed can
transfer the wheel and run it in place:

    PYTHONPATH=htprovenance-<version>-py3-none-any.whl \\
        python3 -m htprovenance.filehash inputs

Where htprovenance is installed in the job's Python environment, neither
call is needed: the interpreter-startup hook (htprovenance/_autorun.py)
calls start_automatic_recording(), which hashes inputs when the job's first
Python process starts and outputs whenever one of its Python processes
exits. Automatic recording writes nothing to the job's stdout or stderr.

Best-effort like autocapture: main() never raises and always exits 0.
Missing or unreadable files are recorded with a status and reported on
stderr.
"""

from __future__ import annotations

import argparse
import atexit
import json
import os
import sys
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import urlsplit

from htprovenance.autocapture import (
    execute_node_event_log_path,
    execute_node_log_dir,
    execute_node_ndjson_stem,
    execute_node_run_id,
    submit_host_capture_target,
)
from htprovenance.events import emit_event
from htprovenance.post import parse_classad, schedd_from_global_job_id
from htprovenance.sidecar import sha256_file

_CONTAINER_IMAGE_AD_KEYS = ("ContainerImage", "SingularityImage", "DockerImage")
_CONDOR_SANDBOX_FILES = frozenset({"_condor_stdout", "_condor_stderr"})
_INPUTS_MARKER_FILENAME = ".htprovenance_inputs_hashed"


def _warn(message: str) -> None:
    print(f"htprovenance-hash: {message}", file=sys.stderr)


def hash_file(path: Path, role: str, **extra: str) -> dict:
    """Return one file entry: path, role, and sha256/size/mtime_ns when readable.

    status is "ok", "missing" or "unreadable"; the latter two carry no hash.
    """
    entry: dict = {"path": str(path), "role": role, **extra}
    try:
        stat = path.stat()
        entry.update(
            sha256=sha256_file(path),
            size=stat.st_size,
            mtime_ns=stat.st_mtime_ns,
            status="ok",
        )
    except FileNotFoundError:
        entry["status"] = "missing"
    except OSError as exc:
        entry.update(status="unreadable", error=str(exc))
    return entry


def _report_problems(entries: list[dict]) -> None:
    """Print one stderr line for each file that could not be hashed."""
    for entry in entries:
        status, role, path = entry.get("status"), entry["role"], entry["path"]
        if status == "missing":
            _warn(f"{role} file {path} does not exist")
        elif status == "unreadable":
            _warn(f"cannot read {role} file {path}: {entry.get('error')}")
        elif status == "unresolved":
            _warn(f"cannot tell which sandbox files came from {path!r}; pass them as paths")


def _expand(path: Path) -> list[Path]:
    """Return the files under path if it is a directory, else path itself."""
    if path.is_dir():
        return sorted(p for p in path.rglob("*") if p.is_file())
    return [path]


def _split_list(value: object) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def sandbox_path(transfer_entry: str) -> Path | None:
    """Return where a TransferInput entry lands in the sandbox, or None if unknowable.

    HTCondor puts each transferred file or directory at the top level under
    its base name; a URL lands under the base name of its path. An entry
    ending in "/" transfers a directory's contents, whose names aren't in
    the job ad.
    """
    if "://" in transfer_entry:
        name = Path(urlsplit(transfer_entry).path).name
    elif transfer_entry.endswith("/"):
        return None
    else:
        name = Path(transfer_entry).name
    return Path(name) if name else None


def _transfer_input_entries(ad: dict) -> list[dict]:
    entries = []
    for source in _split_list(ad.get("TransferInput")):
        path = sandbox_path(source)
        if path is None:
            entries.append({"path": source, "role": "input", "status": "unresolved"})
            continue
        entries.extend(hash_file(p, "input", source=source) for p in _expand(path))
    return entries


def _executable_entry(ad: dict) -> dict | None:
    cmd = ad.get("Cmd")
    if not cmd:
        return None
    if str(ad.get("TransferExecutable", "true")).lower() == "false":
        candidates = [Path(cmd)]
    else:
        candidates = [Path(Path(cmd).name), Path("condor_exec.exe")]
    path = next((p for p in candidates if p.is_file()), candidates[0])
    return hash_file(path, "executable", source=str(cmd))


def _container_entry(ad: dict) -> dict | None:
    """Return the container image entry: hashed when the image file is reachable.

    Otherwise the image reference is recorded as given, without a hash. A
    tag such as docker://python:3.12 can point at different images over
    time, so a reference alone does not pin the software.
    """
    image = next((ad[key] for key in _CONTAINER_IMAGE_AD_KEYS if ad.get(key)), None)
    if image is None:
        return None
    image = str(image)
    for path in (Path(Path(image).name), Path(image)):
        if "://" not in image and path.is_file():
            return hash_file(path, "container", source=image)
    return {"path": image, "role": "container", "status": "reference"}


def input_entries(ad: dict, paths: list[str]) -> list[dict]:
    """Return the entries for a files.inputs event."""
    if paths:
        entries = [hash_file(p, "input") for raw in paths for p in _expand(Path(raw))]
    else:
        entries = _transfer_input_entries(ad)
    for extra in (_executable_entry(ad), _container_entry(ad)):
        if extra is not None:
            entries.append(extra)
    return entries


def _latest_files(log_path: Path, event_type: str) -> list[dict]:
    """Return the file entries of this job's latest event of event_type, [] if none."""
    files: list[dict] = []
    try:
        lines = log_path.read_text().splitlines()
    except OSError:
        return files
    for line in lines:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("type") == event_type:
            files = event.get("files", [])
    return files


def _size_and_mtime(entries: list[dict]) -> dict[str, tuple[int, int]]:
    return {e["path"]: (e["size"], e["mtime_ns"]) for e in entries if e.get("status") == "ok"}


def _default_output_paths(log_path: Path) -> list[Path]:
    """Return the files HTCondor's default output transfer would send back.

    That is every top-level file that is new or changed. Files recorded by
    the inputs call with the same size and mtime are unchanged; HTCondor's
    own stdout/stderr files and dotfiles are not the job's outputs.
    """
    unchanged = _size_and_mtime(_latest_files(log_path, "files.inputs"))
    paths = []
    for path in sorted(Path().iterdir()):
        if not path.is_file() or path.name.startswith("."):
            continue
        if path.name in _CONDOR_SANDBOX_FILES:
            continue
        stat = path.stat()
        if unchanged.get(str(path)) == (stat.st_size, stat.st_mtime_ns):
            continue
        paths.append(path)
    return paths


def _output_remaps(ad: dict) -> dict[str, str]:
    remaps = {}
    for pair in str(ad.get("TransferOutputRemaps") or "").split(";"):
        name, sep, destination = pair.partition("=")
        if sep and name.strip():
            remaps[name.strip()] = destination.strip()
    return remaps


def _destination_url(path: Path, ad: dict, remaps: dict[str, str]) -> str | None:
    """Return where output transfer sends path, when the job ad names a destination."""
    if str(path) in remaps:
        return remaps[str(path)]
    output_destination = ad.get("OutputDestination")
    if output_destination:
        return f"{str(output_destination).rstrip('/')}/{path}"
    return None


def _hash_unless_unchanged(path: Path, previous: dict[str, dict], **extra: str) -> dict:
    """Return path's output entry, reusing the previous hash if size and mtime match."""
    old = previous.get(str(path))
    if old is not None and old.get("status") == "ok":
        try:
            stat = path.stat()
        except OSError:
            return hash_file(path, "output", **extra)
        if (old["size"], old["mtime_ns"]) == (stat.st_size, stat.st_mtime_ns):
            kept = {key: old[key] for key in ("sha256", "size", "mtime_ns", "status")}
            return {"path": str(path), "role": "output", **extra, **kept}
    return hash_file(path, "output", **extra)


def output_entries(
    ad: dict, paths: list[str], log_path: Path, previous: list[dict] | None = None
) -> list[dict]:
    """Return the entries for a files.outputs event.

    The job's own NDJSON file is left out: it is still being appended to.
    Files in `previous` (an earlier files.outputs event's entries) whose size
    and mtime have not changed keep their hash without being read again.
    """
    if paths:
        candidates = [p for raw in paths for p in _expand(Path(raw))]
    elif "TransferOutput" in ad:
        listed = _split_list(ad["TransferOutput"])
        candidates = [p for raw in listed for p in _expand(Path(raw))]
    else:
        candidates = _default_output_paths(log_path)

    remaps = _output_remaps(ad)
    own_log = log_path.resolve()
    known = {entry["path"]: entry for entry in previous or []}
    entries = []
    for path in candidates:
        if path.resolve() == own_log:
            continue
        destination = _destination_url(path, ad, remaps)
        extra = {"destination_url": destination} if destination else {}
        entries.append(_hash_unless_unchanged(path, known, **extra))
    return entries


def _job_ad() -> dict:
    job_ad_path = os.environ.get("_CONDOR_JOB_AD")
    if not job_ad_path:
        raise RuntimeError("$_CONDOR_JOB_AD is not set; not inside an HTCondor job")
    return parse_classad(job_ad_path)


def _emit(kind: str, ad: dict, entries: list[dict]) -> None:
    run_id = execute_node_run_id(ad)
    emit_event(
        f"files.{kind}",
        run_id,
        log_dir=execute_node_log_dir(),
        ndjson_stem=execute_node_ndjson_stem(run_id, ad),
        source="htprovenance_hash",
        cluster_id=ad.get("ClusterId"),
        proc_id=ad.get("ProcId"),
        schedd=schedd_from_global_job_id(str(ad.get("GlobalJobId", ""))),
        files=entries,
    )


def record(kind: str, paths: list[str], *, quiet: bool = False) -> list[dict]:
    """Hash this job's inputs or outputs and append a files.<kind> event.

    Files that could not be hashed are reported on stderr unless quiet.
    Returns the file entries written to the event.

    Raises:
        RuntimeError: outside an HTCondor job ($_CONDOR_JOB_AD is not set),
            where there is no job to file the hashes under.
    """
    ad = _job_ad()
    if kind == "inputs":
        entries = input_entries(ad, paths)
    else:
        entries = output_entries(ad, paths, execute_node_event_log_path(ad))
    _emit(kind, ad, entries)
    if not quiet:
        _report_problems(entries)
    return entries


@contextmanager
def _in_sandbox() -> Generator[None, None, None]:
    """Run the block with the job's sandbox as the working directory.

    The job ad's file names are relative to the sandbox, and the job's own
    code may have changed directory by the time its Python process exits.
    """
    scratch_dir = os.environ.get("_CONDOR_SCRATCH_DIR")
    if not scratch_dir:
        yield
        return
    original = Path.cwd()
    os.chdir(scratch_dir)
    try:
        yield
    finally:
        os.chdir(original)


def _claim_inputs_marker() -> bool:
    """Return True for the first caller in this sandbox, False for every later one."""
    try:
        Path(_INPUTS_MARKER_FILENAME).open("x").close()
    except FileExistsError:
        return False
    return True


def _record_outputs_at_exit() -> None:
    """Record this job's outputs as they stand when a Python process exits.

    A job can run many Python processes, so this runs many times: files
    unchanged since the last files.outputs event are not read again, and no
    event is written when nothing has changed.
    """
    try:
        with _in_sandbox():
            ad = _job_ad()
            log_path = execute_node_event_log_path(ad)
            previous = _latest_files(log_path, "files.outputs")
            entries = output_entries(ad, [], log_path, previous)
            if entries != previous:
                _emit("outputs", ad, entries)
    except Exception:  # noqa: BLE001, S110 -- deliberately blind and silent:
        # runs inside the job's own Python process, which must exit exactly
        # as it would have without htprovenance installed.
        pass


def start_automatic_recording() -> None:
    """Hash this job's inputs now, and its outputs when this process exits.

    Called at interpreter startup by htprovenance._autorun. Inputs are
    hashed by the job's first Python process only. Does nothing on the
    access point (DAGMan, SERVICE nodes), writes nothing to stdout or
    stderr, and never raises.
    """
    try:
        ad = _job_ad()
        if submit_host_capture_target(ad) is not None:
            return
        with _in_sandbox():
            if _claim_inputs_marker():
                record("inputs", [], quiet=True)
        atexit.register(_record_outputs_at_exit)
    except Exception:  # noqa: BLE001, S110 -- deliberately blind and silent:
        # runs at the start of every Python process in the job, before the
        # job's own code; a failure here must not be visible to it.
        pass


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="htprovenance-hash",
        description="Record SHA-256 hashes of this HTCondor job's input or output "
        "files in its provenance log. Always exits 0.",
    )
    parser.add_argument("kind", choices=["inputs", "outputs"])
    parser.add_argument(
        "paths",
        nargs="*",
        help="Files or directories to hash instead of the job ad's "
        "TransferInput / TransferOutput list.",
    )
    try:
        args = parser.parse_args()
        entries = record(args.kind, args.paths)
    except SystemExit:
        return  # argparse already printed the usage error; the job must go on
    except Exception as exc:  # noqa: BLE001 -- best-effort: hashing must never
        # change the exit code of the job script that calls it.
        _warn(f"nothing recorded: {exc}")
        return
    hashed = sum(1 for entry in entries if entry.get("status") == "ok")
    print(f"htprovenance-hash: recorded {hashed} of {len(entries)} {args.kind} file(s)")


if __name__ == "__main__":
    main()
