"""Zero-config execute-node provenance auto-capture.

Detects that the current process is running inside an HTCondor job and, if
so, emits a job.assigned event recording where it runs -- with no call
required from the training script itself. htprovenance.watcher reads that
event back (see execute_node_event_log_path) to stamp checkpoint sidecars.
See htprovenance/_autorun.py for the interpreter-startup hook that makes this
actually zero-config (a .pth file, installed by this package's build hook --
see hatch_build.py -- runs _autorun at the start of every Python process in an
environment where this package is installed).

Detection reuses $_CONDOR_JOB_AD, the same env var htprovenance.jobad
already relies on to locate the job's ClassAd snapshot (see its module
docstring) -- one detection signal, not two independently-drifting ones.

run_id for an execute-node job is $PROVENANCE_RUN_ID when set (mldag's
generated DAGs set it). Otherwise -- a plain condor_submit, or a hand-written
DAG -- it is the job's JobBatchName, else cluster-<ClusterId>. Every
execute-node job writes its own <run_id>.<cluster>.<proc>.ndjson, so jobs
returning output to a shared directory never overwrite each other's file (nor
the access point's <run_id>.ndjson). mldag's jobs write it under
output/provenance; other jobs at the top level of their sandbox, so default
output transfer brings it back. See execute_node_run_id /
execute_node_log_dir / execute_node_ndjson_stem.

Processes on the access point see $_CONDOR_JOB_AD too: DAGMan's PRE/POST
scripts inherit DAGMan's own (scheduler-universe) ad, and SERVICE nodes run
as local-universe jobs. Those are recorded as dagman.started /
service.started under run_id dagman-<DAGMan cluster>, not as job.assigned,
so they never pose as a training job.

Idempotency: HTCondor keeps one job pinned to one sandbox directory for its
whole lifetime, and a single job can start many Python interpreters (a
wrapper script's own `python3 --version` probe, the training process itself,
subprocesses it launches, ...). A marker file in the sandbox stops every
invocation after the first from re-emitting job.assigned.

Best-effort by design (AC#7): capture_and_emit() never raises. A missing
$_CONDOR_JOB_AD, an unreadable ClassAd, no nvidia-smi/no GPU, a read-only
sandbox -- none of it should ever be able to affect the training job's own
behavior, output, or exit code. Contrast this with the hand-rolled heredocs
this module replaces in pretrain_local.sh/pretrain.sh, which ran their whole
capture step as `_provenance_capture_and_emit() { ... } || exit 1` -- any
uncaught exception in there aborted the job outright.
"""

from __future__ import annotations

import glob
import os
import re
import subprocess
import sys
from pathlib import Path

from htprovenance.events import _DEFAULT_LOG_DIR, emit_event, event_log_path
from htprovenance.jobad import capture_job_ad_fields
from htprovenance.post import parse_classad

_MARKER_FILENAME = ".htprovenance_autocapture_done"

# Universes whose jobs run on the access point rather than an execute node.
# Their child processes inherit $_CONDOR_JOB_AD too: DAGMan's PRE/POST scripts
# see DAGMan's own ad, and a SERVICE node sees its local-universe ad. Those
# are recorded per DAGMan instead of as a training job's job.assigned.
_SCHEDULER_UNIVERSE = 7
_LOCAL_UNIVERSE = 12
_SUBMIT_HOST_EVENT_TYPES = {
    _SCHEDULER_UNIVERSE: "dagman.started",
    _LOCAL_UNIVERSE: "service.started",
}

_GPU_INFO_DEFAULTS = {"gpu_count": 0, "gpu_model": "none", "gpu_id": "none"}


def _run(cmd: list[str]) -> str:
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=10, check=False
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return result.stdout.strip() if result.returncode == 0 else ""


def detect_condor_job() -> bool:
    """Best-effort HTCondor-job detection via $_CONDOR_JOB_AD presence."""
    return bool(os.environ.get("_CONDOR_JOB_AD"))


def _ad_int(ad: dict, key: str) -> int | None:
    """Return an integer ClassAd attribute, or None if absent or not an integer.

    Accepts "7810859.0"-style job IDs by keeping the cluster part.
    """
    value = ad.get(key)
    if value is None:
        return None
    try:
        return int(str(value).split(".")[0])
    except ValueError:
        return None


def submit_host_capture_target(ad: dict) -> tuple[str, str, int | None] | None:
    """Return (event_type, run_id, dagman_cluster_id) for an access-point job ad, or None.

    DAGMan itself (scheduler universe) and its SERVICE nodes (local universe)
    both go to dagman-<DAGMan cluster>, so one file holds everything captured
    on the access point for one DAG run. A local-universe job not started by
    DAGMan goes to local-<its own cluster>.
    """
    universe = _ad_int(ad, "JobUniverse")
    if universe not in _SUBMIT_HOST_EVENT_TYPES:
        return None
    cluster_id = _ad_int(ad, "ClusterId")
    event_type = _SUBMIT_HOST_EVENT_TYPES[universe]
    if universe == _SCHEDULER_UNIVERSE:
        return event_type, f"dagman-{cluster_id}", cluster_id
    dagman_cluster_id = _ad_int(ad, "DAGManJobId")
    if dagman_cluster_id is not None:
        return event_type, f"dagman-{dagman_cluster_id}", dagman_cluster_id
    return event_type, f"local-{cluster_id}", None


def execute_node_run_id(ad: dict) -> str:
    """Return the run_id for an execute-node job.

    Resolution order: $PROVENANCE_RUN_ID, then the job's JobBatchName (several
    submits sharing a batch name group under one run_id), then
    cluster-<ClusterId> (one condor_submit is one run). "unknown" only when
    the job ad is unreadable.
    """
    run_id = os.environ.get("PROVENANCE_RUN_ID")
    if run_id:
        return run_id
    batch_name = ad.get("JobBatchName")
    if batch_name:
        return str(batch_name)
    cluster_id = _ad_int(ad, "ClusterId")
    if cluster_id is not None:
        return f"cluster-{cluster_id}"
    return "unknown"


def execute_node_log_dir() -> Path:
    """Return the directory an execute-node job writes its NDJSON into.

    $PROVENANCE_LOG_DIR if set. Otherwise output/provenance for jobs with
    $PROVENANCE_RUN_ID (mldag's DAGs, unchanged), and the top level of the
    sandbox for any other job: HTCondor's default output transfer returns new
    top-level files but not new subdirectories, so a plain condor_submit job
    gets its provenance back without listing it in transfer_output_files.
    """
    log_dir = os.environ.get("PROVENANCE_LOG_DIR")
    if log_dir:
        return Path(log_dir)
    if os.environ.get("PROVENANCE_RUN_ID"):
        return Path(_DEFAULT_LOG_DIR)
    return Path()


def execute_node_event_log_path(ad: dict) -> Path:
    """Return the NDJSON file this execute-node job's job.assigned event is written to.

    Derived from the same environment and job ad as the write, so code running
    later in the same job (htprovenance.watcher) can find the event again.
    """
    run_id = execute_node_run_id(ad)
    stem = execute_node_ndjson_stem(run_id, ad) or run_id
    return event_log_path(stem, execute_node_log_dir())


_UNSAFE_FILENAME_CHARS = re.compile(r"[^A-Za-z0-9._-]+")
_MAX_STEM_RUN_ID_CHARS = 200


def execute_node_ndjson_stem(run_id: str, ad: dict) -> str | None:
    """Return the NDJSON file stem for an execute-node job, or None to use run_id.

    <run_id>.<cluster>.<proc>: every job writes its own sandbox copy, and
    output transfer replaces a same-named file in the directory it returns
    to, so a per-run name would let each job's file clobber the previous
    jobs' events and the access point's own <run_id>.ndjson. The cluster is
    needed because a run_id (a DAG run, a JobBatchName) spans several
    clusters. run_id can be free text (JobBatchName), so it's made
    filename-safe here; the event itself keeps the original. None (use
    run_id) only when the job ad lacks ClusterId/ProcId.
    """
    cluster_id = _ad_int(ad, "ClusterId")
    proc_id = _ad_int(ad, "ProcId")
    if cluster_id is None or proc_id is None:
        return None
    return f"{filename_safe_run_id(run_id)}.{cluster_id}.{proc_id}"


def filename_safe_run_id(run_id: str) -> str:
    """Return run_id as it appears at the start of a per-job NDJSON file name.

    Characters other than letters, digits, '.', '_' and '-' become '_', the
    result is capped in length, and leading dots are dropped so the file is
    never hidden. Never contains glob metacharacters.
    """
    safe_run_id = _UNSAFE_FILENAME_CHARS.sub("_", run_id)[:_MAX_STEM_RUN_ID_CHARS]
    return safe_run_id.lstrip(".") or "_"


def default_gpu_info() -> tuple[dict, str]:
    """Best-effort, torch-free GPU model/count/id.

    Tries nvidia-smi first (a plain CLI call, no Python GPU library needed).
    Falls back to /proc/driver/nvidia/gpus/*/information -- the same file
    pretrain_local.sh's/pretrain.sh's heredocs already parsed as their own
    fallback for GPU UUID when torch lacked a `.uuid` attribute, generalized
    here to also recover the model name and to work as the primary path for
    any caller that doesn't have torch at all.

    Returns (gpu_info, source) where source records which mechanism found it
    (or "none" if neither did), for the event's site_info_source field.
    """
    smi_output = _run(["nvidia-smi", "--query-gpu=name,uuid", "--format=csv,noheader"])
    lines = [line for line in smi_output.splitlines() if line.strip()]
    if lines:
        name, _, uuid = lines[0].partition(",")
        return (
            {
                "gpu_count": len(lines),
                "gpu_model": name.strip(),
                "gpu_id": uuid.strip(),
            },
            "nvidia_smi",
        )

    info_files = sorted(glob.glob("/proc/driver/nvidia/gpus/*/information"))
    if not info_files:
        return dict(_GPU_INFO_DEFAULTS), "none"
    try:
        text = Path(info_files[0]).read_text()
    except OSError:
        return dict(_GPU_INFO_DEFAULTS), "none"
    model_match = re.search(r"Model:\s+(.+)", text)
    uuid_match = re.search(r"GPU UUID:\s+(\S+)", text)
    return (
        {
            "gpu_count": len(info_files),
            "gpu_model": model_match.group(1).strip() if model_match else "unknown",
            "gpu_id": uuid_match.group(1) if uuid_match else "unknown",
        },
        "proc_driver_nvidia",
    )


def gather_site_info(
    gpu_info: dict | None = None, gpu_info_source: str = "torch_cuda"
) -> tuple[dict, str]:
    """Return (site_info, site_info_source).

    site_info is exactly the {hostname, slot, gpu_model, gpu_count, gpu_id}
    shape watcher.py's _load_site_info splits out of the job.assigned event. Pass
    gpu_info to supply caller-detected values (e.g. pretrain_local.sh's own
    torch.cuda introspection, more precise than this module's nvidia-smi/proc
    fallback) instead of auto-detecting.
    """
    hostname = _run(["hostname", "-f"]) or _run(["hostname"]) or "unknown"
    slot = os.environ.get("_CONDOR_SLOT", "unknown")
    if gpu_info is not None:
        gpu, source = {**_GPU_INFO_DEFAULTS, **gpu_info}, gpu_info_source
    else:
        gpu, source = default_gpu_info()
    return {"hostname": hostname, "slot": slot, **gpu}, source


def _htprovenance_version() -> str:
    # HTPROVENANCE_VERSION is baked into every job's environment by daggen.py at DAG
    # generation time -- prefer it so the recorded version matches exactly
    # what was submitted, even if a different htprovenance happens to be importable
    # in the job's Python environment. Falls back to the installed package's
    # own version for jobs not launched through a generated DAG.
    version = os.environ.get("HTPROVENANCE_VERSION")
    if version:
        return version
    try:
        from importlib.metadata import PackageNotFoundError
        from importlib.metadata import version as pkg_version

        return pkg_version("htprovenance")
    except PackageNotFoundError:
        return "unknown"


def gather_env_info() -> dict:
    smi_output = _run(["nvidia-smi"])
    cuda_match = re.search(r"CUDA Version:\s*([\d.]+)", smi_output)
    return {
        "python": sys.version.split()[0],
        "cuda": cuda_match.group(1) if cuda_match else "unknown",
        "code_commit": _run(["git", "rev-parse", "--short", "HEAD"]) or "unknown",
        "htprovenance_version": _htprovenance_version(),
    }


def capture_and_emit(
    *, gpu_info: dict | None = None, fields_file: str | Path | None = None
) -> dict | None:
    """Best-effort: record where this HTCondor job is running.

    On an execute node, emits job.assigned under the run_id from
    execute_node_run_id. For DAGMan and its SERVICE nodes on the access
    point, emits dagman.started / service.started under
    dagman-<DAGMan cluster> instead, without probing GPUs.

    Returns the emitted payload, or None if this isn't (detectably) an
    HTCondor job, this job was already captured (idempotency marker), or
    capture failed for any reason -- callers don't need to check the return
    value to stay safe; it's provided for callers that want to inspect what
    was captured (e.g. tests, or a caller printing it for debugging).
    """
    try:
        if not detect_condor_job():
            return None
        ad = parse_classad(os.environ["_CONDOR_JOB_AD"])
        target = submit_host_capture_target(ad)
        if target is not None:
            log_dir = Path(os.environ.get("PROVENANCE_LOG_DIR", _DEFAULT_LOG_DIR))
            return _capture_submit_host(ad, target, log_dir, fields_file)
        return _capture_execute_node(ad, execute_node_log_dir(), gpu_info, fields_file)
    except Exception:  # noqa: BLE001 -- deliberately blind: AC#7's whole point
        # is that any failure here (best-effort) must never affect the
        # training job's own behavior, output, or exit code.
        return None


def main() -> None:
    import argparse
    import json

    parser = argparse.ArgumentParser(
        description="Best-effort execute-node provenance auto-capture: emit "
        "a job.assigned event. Safe to call "
        "unconditionally -- no-ops outside an HTCondor job or if this job's "
        "sandbox was already captured."
    )
    parser.add_argument("--gpu-model", help="Override auto-detected GPU model name")
    parser.add_argument(
        "--gpu-count", type=int, help="Override auto-detected GPU count"
    )
    parser.add_argument("--gpu-id", help="Override auto-detected GPU UUID")
    parser.add_argument(
        "--fields-file",
        default=None,
        help="YAML file mapping ClassAd attributes to provenance schema keys "
        "(see htprovenance.post.load_classad_field_mapping).",
    )
    args = parser.parse_args()

    gpu_info = None
    if any(v is not None for v in (args.gpu_model, args.gpu_count, args.gpu_id)):
        gpu_info = {
            "gpu_model": args.gpu_model if args.gpu_model is not None else "unknown",
            "gpu_count": args.gpu_count if args.gpu_count is not None else 0,
            "gpu_id": args.gpu_id if args.gpu_id is not None else "unknown",
        }

    payload = capture_and_emit(gpu_info=gpu_info, fields_file=args.fields_file)
    print(json.dumps(payload) if payload is not None else "{}")


if __name__ == "__main__":
    main()


def _capture_execute_node(
    ad: dict, log_dir: Path, gpu_info: dict | None, fields_file: str | Path | None
) -> dict | None:
    marker = Path(_MARKER_FILENAME)
    if marker.exists():
        return None
    run_id = execute_node_run_id(ad)
    site_info, site_info_source = gather_site_info(gpu_info)
    payload = {**site_info, **gather_env_info(), **capture_job_ad_fields(fields_file)}
    emit_event(
        "job.assigned",
        run_id,
        log_dir=log_dir,
        ndjson_stem=execute_node_ndjson_stem(run_id, ad),
        source="execute_node_autocapture",
        site_info_source=site_info_source,
        **payload,
    )
    marker.write_text("")
    return payload


def _capture_submit_host(
    ad: dict, target: tuple[str, str, int | None], log_dir: Path, fields_file: str | Path | None
) -> dict | None:
    # DAGMan and its SERVICE nodes share the DAG directory, so each HTCondor
    # job gets its own marker rather than the first one blocking the rest.
    marker = Path(f"{_MARKER_FILENAME}.{_ad_int(ad, 'ClusterId')}")
    if marker.exists():
        return None
    event_type, run_id, dagman_cluster_id = target
    site_info, _ = gather_site_info(gpu_info={}, gpu_info_source="none")
    payload = {
        "hostname": site_info["hostname"],
        **gather_env_info(),
        **capture_job_ad_fields(fields_file),
        "dagman_cluster_id": dagman_cluster_id,
    }
    emit_event(
        event_type,
        run_id,
        log_dir=log_dir,
        ndjson_stem=None,
        source="submit_host_autocapture",
        **payload,
    )
    marker.write_text("")
    return payload
