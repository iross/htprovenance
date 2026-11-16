"""Lineage reconstruction and provenance query library + CLI.

Library functions:
    walk_lineage(checkpoint_path)  -- follow parent_hash chain back to epoch 0
    query_run(run_id, log_dir)     -- read a run's events from its NDJSON logs

CLI:
    htprovenance-query lineage <checkpoint>   -- print ancestry chain
    htprovenance-query events <run_id>        -- print all events for a run
    htprovenance-query db build               -- build/refresh the SQLite database
    htprovenance-query files software <file>  -- what software produced a file
    htprovenance-query files downstream <file> -- what was made from a file
    htprovenance-query files chain <a> <b>    -- the jobs leading from a to b

Both `lineage` and `events` accept --json for machine-readable output and
--log-dir to override the default NDJSON location. See `htprovenance-query db
build --help` and htprovenance/db.py for the database schema and
example queries.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
import sqlite3
from contextlib import closing
from typing import Annotated, Optional

import typer

from htprovenance.autocapture import filename_safe_run_id
from htprovenance.db import (
    DEFAULT_CHECKPOINT_DIR,
    DEFAULT_DB_PATH,
    DEFAULT_EVENT_DIR,
    build_database,
)
from htprovenance.events import _DEFAULT_LOG_DIR
from htprovenance.event_log_scan import scan_event_log
from htprovenance.history_enrich import _DEFAULT_BATCH_SIZE
from htprovenance.lineage import (
    chain_between,
    downstream_outputs,
    job_label,
    resolve_hash,
    software_for_output,
)
from htprovenance.packages import diff_package_sets, job_package_set, packages_in_set

app = typer.Typer(no_args_is_help=True)
db_app = typer.Typer(no_args_is_help=True, help="Build/refresh the local SQLite provenance database.")
app.add_typer(db_app, name="db")
files_app = typer.Typer(
    no_args_is_help=True,
    help="File lineage from in-job hashes (htprovenance-hash). FILE arguments are a "
    "SHA-256, a local file, or a file name / URL recorded in the database.",
)
app.add_typer(files_app, name="files")
packages_app = typer.Typer(
    no_args_is_help=True,
    help="Python packages installed in a job's environment. JOB arguments are "
    "<cluster>.<proc> or <schedd>#<cluster>.<proc>, as `files software` prints them.",
)
app.add_typer(packages_app, name="packages")


# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------


def _read_sidecar(path: Path) -> dict:
    """Load and return the parsed sidecar for *path*.

    Raises:
        FileNotFoundError: if the sidecar does not exist.
        ValueError: if the sidecar cannot be parsed.
    """
    sidecar_path = Path(str(path) + ".provenance.json")
    if not sidecar_path.exists():
        raise FileNotFoundError(f"No sidecar found for {path} (expected {sidecar_path})")
    try:
        return json.loads(sidecar_path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"Corrupt sidecar at {sidecar_path}: {exc}") from exc


def _find_checkpoint_by_hash(start_dir: Path, target_hash: str) -> Path | None:
    """Search start_dir recursively for a checkpoint whose sidecar has checkpoint_hash == target_hash."""
    for sidecar_path in start_dir.rglob("*.provenance.json"):
        try:
            data = json.loads(sidecar_path.read_text())
        except (json.JSONDecodeError, OSError):
            continue
        if data.get("checkpoint_hash") == target_hash:
            ckpt_path = Path(str(sidecar_path)[: -len(".provenance.json")])
            if ckpt_path.exists():
                return ckpt_path
    return None


def walk_lineage(checkpoint_path: str | Path) -> list[dict]:
    """Return the ordered sidecar chain from epoch 0 to checkpoint_path.

    Follows parent_hash links backwards, then reverses so index 0 is the
    oldest checkpoint.

    Args:
        checkpoint_path: Path to the checkpoint file (not the sidecar).

    Returns:
        List of sidecar dicts, oldest first.

    Raises:
        FileNotFoundError: if a sidecar in the chain is missing.
        ValueError: if a sidecar is malformed or the chain is cyclic.
    """
    checkpoint_path = Path(checkpoint_path)
    chain: list[dict] = []
    seen_hashes: set[str] = set()
    search_dir = checkpoint_path.parent

    current = checkpoint_path
    while True:
        record = _read_sidecar(current)
        h = record.get("checkpoint_hash", "")
        if h in seen_hashes:
            raise ValueError(f"Cycle detected in lineage chain at hash {h!r}")
        seen_hashes.add(h)
        chain.append(record)

        parent_hash = record.get("parent_hash")
        if parent_hash is None:
            break

        parent_ckpt = _find_checkpoint_by_hash(search_dir, parent_hash)
        if parent_ckpt is None:
            raise FileNotFoundError(
                f"Parent checkpoint with hash {parent_hash!r} not found under {search_dir}"
            )
        current = parent_ckpt

    chain.reverse()
    return chain


def query_run(run_id: str, log_dir: str | Path = _DEFAULT_LOG_DIR) -> list[dict]:
    """Return all provenance events for run_id in chronological order.

    Reads the run's access-point file (<run_id>.ndjson) and every per-job
    file its execute-node jobs wrote (<run_id>.<cluster>.<proc>.ndjson, see
    htprovenance.autocapture.execute_node_ndjson_stem). A per-job file's name
    only approximates its run_id (the run_id is made filename-safe), so only
    its events whose run_id field matches are kept.

    Args:
        run_id: The run identifier (PROVENANCE_RUN_ID, JobBatchName or
            cluster-<ClusterId>).
        log_dir: Directory containing NDJSON event logs.

    Returns:
        List of event dicts sorted by ts field.

    Raises:
        FileNotFoundError: if no NDJSON log for run_id exists in log_dir.
    """
    log_dir = Path(log_dir)
    run_path = log_dir / f"{run_id}.ndjson"
    job_paths = sorted(log_dir.glob(f"{filename_safe_run_id(run_id)}.*.ndjson"))
    if not run_path.exists() and not job_paths:
        raise FileNotFoundError(f"No event log found for run {run_id!r} in {log_dir}")
    events = _read_ndjson(run_path) if run_path.exists() else []
    for job_path in job_paths:
        events.extend(e for e in _read_ndjson(job_path) if e.get("run_id") == run_id)
    return sorted(events, key=lambda e: e.get("ts", ""))


def _read_ndjson(path: Path) -> list[dict]:
    events = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return events


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _format_lineage(chain: list[dict]) -> str:
    lines = []
    for record in chain:
        epoch = record.get("epoch", "?")
        h = record.get("checkpoint_hash", "?")[:16]
        parent = (record.get("parent_hash") or "genesis")[:16]
        site = record.get("produced_at", {}).get("hostname", "unknown")
        lines.append(f"  epoch {epoch:>3}  {h}  ← {parent}  [{site}]")
    return "\n".join(lines)


def _format_scan(records: list[dict]) -> str:
    lines = []
    for r in records:
        # job_id (cluster.proc) is always unique; run_id/job_name is often
        # shared by every proc in a cluster (job-array batches, or a DAGMan
        # run_id resolved at cluster granularity), so it's shown alongside
        # job_id rather than instead of it.
        job_id = f"{r['cluster_id']}.{r['proc_id']}"
        label = r.get("run_id") or r.get("job_name") or "-"
        site = r.get("site") or r.get("resource_name") or "?"
        wall_time = f"{r['wall_time_s']:.0f}s" if "wall_time_s" in r else "?"
        lines.append(
            f"  {job_id:<14} {label:<24} {r['status']:<10} site={site:<30} wall_time={wall_time}"
        )
    return "\n".join(lines)


def _format_events(events: list[dict]) -> str:
    lines = []
    for e in events:
        ts = e.get("ts", "?")[:19]
        etype = e.get("type", "?")
        extras = {k: v for k, v in e.items() if k not in ("schema_version", "type", "run_id", "ts")}
        extra_str = "  " + "  ".join(f"{k}={v}" for k, v in extras.items()) if extras else ""
        lines.append(f"  {ts}  {etype:<20}{extra_str}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------


@app.command()
def lineage(
    checkpoint: Annotated[str, typer.Argument(help="Path to checkpoint file")],
    json_out: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Print the ancestry chain for a checkpoint."""
    try:
        chain = walk_lineage(checkpoint)
    except (FileNotFoundError, ValueError) as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)

    if json_out:
        typer.echo(json.dumps(chain, indent=2))
    else:
        run_id = chain[0].get("run_id", "?") if chain else "?"
        typer.echo(f"Lineage for run {run_id} ({len(chain)} checkpoint(s)):")
        typer.echo(_format_lineage(chain))


@app.command()
def events(
    run_id: Annotated[str, typer.Argument(help="Run ID (PROVENANCE_RUN_ID)")],
    log_dir: Annotated[str, typer.Option(help="NDJSON log directory")] = _DEFAULT_LOG_DIR,
    json_out: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Print all provenance events for a run."""
    try:
        run_events = query_run(run_id, log_dir)
    except FileNotFoundError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1)

    if json_out:
        typer.echo(json.dumps(run_events, indent=2))
    else:
        typer.echo(f"Events for run {run_id} ({len(run_events)} total):")
        typer.echo(_format_events(run_events))


@app.command()
def scan(
    log_file: Annotated[str, typer.Argument(help="HTCondor event log to scan (e.g. metl.log)")],
    log_dir: Annotated[
        str,
        typer.Option(help="Classad/.run_id marker directory to opportunistically resolve run_id from; fine if it doesn't exist"),
    ] = _DEFAULT_LOG_DIR,
    provenance_log_dir: Annotated[
        str,
        typer.Option(help="NDJSON provenance directory to opportunistically resolve run_id from job_name; fine if it doesn't exist"),
    ] = _DEFAULT_LOG_DIR,
    cluster_id: Annotated[
        Optional[list[int]],  # noqa: UP045 -- see db_build's Optional[list[str]] for why
        typer.Option(help="Only include this cluster_id; repeatable to filter to a specific set"),
    ] = None,
    schedd: Annotated[
        Optional[str],  # noqa: UP045
        typer.Option(
            help="Schedd name to record for every job, overriding the one read from the log's "
                 "submit events. Needed for --db when the log doesn't name the schedd"
        ),
    ] = None,
    db: Annotated[
        Optional[str],  # noqa: UP045
        typer.Option(help="Write results into this provenance.db's condor_history table (source='event_log')"),
    ] = None,
    json_out: Annotated[bool, typer.Option("--json", help="Emit JSON")] = False,
) -> None:
    """Summarize every job in a raw HTCondor event log: duration, site, resource usage.

    Works on any event log, including one from a batch of jobs run without the
    DAGMan PRE/POST provenance pipeline -- run_id/job_name enrichment only
    happens when log_dir/provenance_log_dir actually have matching data.
    """
    records = scan_event_log(
        log_file,
        log_dir=log_dir,
        provenance_log_dir=provenance_log_dir,
        cluster_ids=cluster_id,
        schedd=schedd,
    )
    if json_out:
        typer.echo(json.dumps(records, indent=2))
    else:
        typer.echo(f"{len(records)} job(s) in {log_file}:")
        typer.echo(_format_scan(records))

    if db:
        from htprovenance.history_enrich import write_scan_records

        try:
            written = write_scan_records(db, records)
        except ValueError as exc:
            typer.echo(f"Error: {exc}", err=True)
            raise typer.Exit(1) from exc
        typer.echo(f"Wrote {written} row(s) to {db} (condor_history, source=event_log)")


@db_app.command("build")
def db_build(
    db: Annotated[str, typer.Option(help="Path to the SQLite database file")] = DEFAULT_DB_PATH,
    # ruff's UP045 assumes `from __future__ import annotations` makes `X | None`
    # runtime-safe on Python 3.9, but Typer evaluates these annotations at CLI
    # registration time to build the parser, so `list[str] | None` still raises
    # TypeError on 3.9 (types.GenericAlias.__or__ was only added in 3.10).
    checkpoint_dir: Annotated[
        Optional[list[str]],  # noqa: UP045
        typer.Option(help="Checkpoint sidecar directory to scan; repeatable"),
    ] = None,
    event_dir: Annotated[
        Optional[list[str]],  # noqa: UP045
        typer.Option(help="NDJSON event directory to scan; repeatable"),
    ] = None,
    full_rescan: Annotated[
        bool,
        typer.Option("--full-rescan", help="Re-ingest everything, ignoring recorded mtimes/offsets"),
    ] = False,
) -> None:
    """Build or refresh the SQLite database from checkpoint sidecars and NDJSON events."""
    checkpoint_dirs = checkpoint_dir or [DEFAULT_CHECKPOINT_DIR]
    event_dirs = event_dir or [DEFAULT_EVENT_DIR]
    stats = build_database(db, checkpoint_dirs, event_dirs, full_rescan=full_rescan)
    typer.echo(str(stats))


@db_app.command("enrich-history")
def db_enrich_history(
    db: Annotated[str, typer.Option(help="Path to the SQLite database file")] = DEFAULT_DB_PATH,
    schedd: Annotated[
        Optional[str],  # noqa: UP045 -- see db_build's Optional[list[str]] for why
        typer.Option(help="Schedd name to query; defaults to the local schedd"),
    ] = None,
    pool: Annotated[
        Optional[str],  # noqa: UP045
        typer.Option(help="Collector address to resolve --schedd against; defaults to the local pool"),
    ] = None,
    full_rescan: Annotated[
        bool,
        typer.Option("--full-rescan", help="Re-query every cluster_id, ignoring already-enriched ones"),
    ] = False,
    batch_size: Annotated[
        int,
        typer.Option(
            help="cluster_ids per condor_history query. Schedd.history() scans its "
                 "backing store sequentially per call regardless of constraint "
                 "complexity, so fewer/larger batches means less total scanning, "
                 "not more -- this is a safety valve against an unreasonably long "
                 "constraint expression, not a cost-control knob"
        ),
    ] = _DEFAULT_BATCH_SIZE,
) -> None:
    """Backfill the condor_history table from HTCondor job history via the Python bindings.

    Requires htcondor2 (a Linux-only dependency); imported here rather than at
    module load time so the rest of this CLI keeps working without it.
    """
    from htprovenance.history_enrich import enrich_from_condor_history

    try:
        stats = enrich_from_condor_history(
            db,
            schedd_name=schedd,
            pool=pool,
            full_rescan=full_rescan,
            batch_size=batch_size,
            on_progress=typer.echo,
        )
    except (FileNotFoundError, ValueError) as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc
    typer.echo(str(stats))


@db_app.command("enrich-jobad")
def db_enrich_jobad(
    db: Annotated[str, typer.Option(help="Path to the SQLite database file")] = DEFAULT_DB_PATH,
    schedd: Annotated[
        Optional[str],  # noqa: UP045
        typer.Option(
            help="Schedd name for job.assigned events that don't record one (captured before "
                 "schedd was recorded); without it those events are skipped"
        ),
    ] = None,
) -> None:
    """Mirror job.assigned events (jobad.py's in-job $_CONDOR_JOB_AD capture) into condor_history.

    Reads from provenance.db's `events` table (run `db build` first), so no
    HTCondor connection is needed -- it's the same data jobad.py already
    wrote at job start, just also stored where it can be joined against
    condor_history/scan data for the same job.
    """
    from htprovenance.history_enrich import enrich_from_jobad_events

    try:
        written = enrich_from_jobad_events(db, default_schedd=schedd)
    except (FileNotFoundError, ValueError) as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc
    typer.echo(f"Wrote {written} row(s) to {db} (condor_history, source=jobad)")


_DbOption = Annotated[str, typer.Option(help="Path to the SQLite database file")]
_JsonOption = Annotated[bool, typer.Option("--json", help="Emit JSON")]


def _open_lineage_db(db: str) -> sqlite3.Connection:
    """Open db for lineage queries, exiting with an error if it has no file hashes."""
    if not Path(db).is_file():
        typer.echo(f"Error: {db} does not exist; run `htprovenance-query db build` first", err=True)
        raise typer.Exit(1)
    conn = sqlite3.connect(db)
    has_table = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'file_edges'"
    ).fetchone()
    if not has_table:
        conn.close()
        typer.echo(
            f"Error: {db} has no file_edges table; rerun `htprovenance-query db build "
            "--full-rescan` with this version",
            err=True,
        )
        raise typer.Exit(1)
    return conn


def _resolve_or_exit(conn: sqlite3.Connection, file_or_hash: str) -> str:
    try:
        return resolve_hash(conn, file_or_hash)
    except LookupError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc


def _format_file(entry: dict | None) -> str:
    if entry is None:
        return "not recorded"
    sha256 = entry.get("sha256")
    text = f"{entry['path']}  {'sha256:' + sha256 if sha256 else '(' + entry['status'] + ')'}"
    if entry.get("destination_url"):
        text += f"  -> {entry['destination_url']}"
    return text


def _format_environment(environment: dict) -> str:
    """Return one line of key=value pairs, with the package set shortened to 12 characters."""
    fields = dict(environment)
    set_hash = fields.pop("package_set_hash", None)
    count = fields.pop("package_count", None)
    if set_hash:
        fields["packages"] = f"{count} (set {set_hash[:12]})"
    return "  ".join(f"{k}={v}" for k, v in fields.items()) or "not recorded"


@files_app.command("software")
def files_software(
    file: Annotated[str, typer.Argument(help="Output file: hash, local file, or recorded name")],
    db: _DbOption = DEFAULT_DB_PATH,
    json_out: _JsonOption = False,
) -> None:
    """Show the job, executable, container and environment that produced a file."""
    with closing(_open_lineage_db(db)) as conn:
        sha256 = _resolve_or_exit(conn, file)
        records = software_for_output(conn, sha256)
    if json_out:
        typer.echo(json.dumps({"sha256": sha256, "produced_by": records}, indent=2))
        return
    typer.echo(f"sha256:{sha256} was written by {len(records)} job(s)")
    for record in records:
        typer.echo(f"  {record['job']}  run_id={record['run_id']}")
        for output in record["outputs"]:
            typer.echo(f"    output       {_format_file(output)}")
        typer.echo(f"    executable   {_format_file(record['executable'])}")
        typer.echo(f"    container    {_format_file(record['container'])}")
        typer.echo(f"    environment  {_format_environment(record['environment'])}")


@files_app.command("downstream")
def files_downstream(
    file: Annotated[str, typer.Argument(help="Input file: hash, local file, or recorded name")],
    db: _DbOption = DEFAULT_DB_PATH,
    json_out: _JsonOption = False,
) -> None:
    """Show the outputs of every job that read a file."""
    with closing(_open_lineage_db(db)) as conn:
        sha256 = _resolve_or_exit(conn, file)
        records = downstream_outputs(conn, sha256)
    if json_out:
        typer.echo(json.dumps({"sha256": sha256, "read_by": records}, indent=2))
        return
    typer.echo(f"sha256:{sha256} was read by {len(records)} job(s)")
    for record in records:
        roles = ", ".join(f"{read['role']} {read['path']}" for read in record["read_as"])
        typer.echo(f"  {record['job']}  run_id={record['run_id']}  (as {roles})")
        for output in record["outputs"]:
            typer.echo(f"    output  {_format_file(output)}")
        if not record["outputs"]:
            typer.echo("    no outputs recorded")


@files_app.command("chain")
def files_chain(
    start: Annotated[str, typer.Argument(help="Earlier file: hash, local file, or recorded name")],
    end: Annotated[str, typer.Argument(help="Later file: hash, local file, or recorded name")],
    db: _DbOption = DEFAULT_DB_PATH,
    json_out: _JsonOption = False,
) -> None:
    """Show the series of jobs that leads from one file to another."""
    with closing(_open_lineage_db(db)) as conn:
        start_sha256 = _resolve_or_exit(conn, start)
        end_sha256 = _resolve_or_exit(conn, end)
        steps = chain_between(conn, start_sha256, end_sha256)
    if json_out:
        typer.echo(json.dumps({"start": start_sha256, "end": end_sha256, "steps": steps}, indent=2))
    if steps is None:
        typer.echo(f"No recorded jobs connect sha256:{start_sha256} to sha256:{end_sha256}", err=True)
        raise typer.Exit(1)
    if json_out:
        return
    if not steps:
        typer.echo("Both names refer to the same file contents")
        return
    typer.echo(f"{len(steps)} job(s) from sha256:{start_sha256} to sha256:{end_sha256}")
    for number, step in enumerate(steps, start=1):
        typer.echo(f"  {number}. {step['job']}  run_id={step['run_id']}")
        typer.echo(f"       read   {_format_file(step['input'])}")
        typer.echo(f"       wrote  {_format_file(step['output'])}")


def main() -> None:
    app()


_JOB_SPEC = re.compile(r"(?:(?P<schedd>[^#]+)#)?(?P<cluster>\d+)(?:\.(?P<proc>\d+))?")


def _job_or_exit(conn: sqlite3.Connection, spec: str) -> tuple[str, int, int]:
    """Return (schedd, cluster_id, proc_id) for a JOB argument; proc defaults to 0."""
    match = _JOB_SPEC.fullmatch(spec)
    if match is None:
        typer.echo(
            f"Error: {spec!r} is not <cluster>.<proc> or <schedd>#<cluster>.<proc>", err=True
        )
        raise typer.Exit(1)
    cluster_id, proc_id = int(match["cluster"]), int(match["proc"] or 0)
    if match["schedd"]:
        return match["schedd"], cluster_id, proc_id
    schedds = [
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT json_extract(payload_json, '$.schedd') FROM events "
            "WHERE type = 'job.assigned' AND json_extract(payload_json, '$.cluster_id') = ? "
            "AND json_extract(payload_json, '$.proc_id') = ? ORDER BY 1",
            (cluster_id, proc_id),
        )
        if row[0]
    ]
    if len(schedds) != 1:
        problem = (
            f"is recorded on {len(schedds)} schedds ({', '.join(schedds)}); "
            f"name one, e.g. {schedds[0]}#{cluster_id}.{proc_id}"
            if schedds
            else "has no job.assigned event in this database"
        )
        typer.echo(f"Error: job {cluster_id}.{proc_id} {problem}", err=True)
        raise typer.Exit(1)
    return schedds[0], cluster_id, proc_id


def _job_packages_or_exit(conn: sqlite3.Connection, spec: str) -> dict:
    job = _job_or_exit(conn, spec)
    try:
        set_hash, count = job_package_set(conn, job)
        packages = packages_in_set(conn, set_hash, count)
    except LookupError as exc:
        typer.echo(f"Error: {exc}", err=True)
        raise typer.Exit(1) from exc
    return {"job": job_label(job), "package_set_hash": set_hash, "packages": packages}


@packages_app.command("show")
def packages_show(
    job: Annotated[str, typer.Argument(help="Job: <cluster>.<proc> or <schedd>#<cluster>.<proc>")],
    db: _DbOption = DEFAULT_DB_PATH,
    json_out: _JsonOption = False,
) -> None:
    """List the Python packages installed in a job's environment."""
    with closing(_open_lineage_db(db)) as conn:
        record = _job_packages_or_exit(conn, job)
    if json_out:
        typer.echo(json.dumps(record, indent=2))
        return
    typer.echo(
        f"{record['job']}  {len(record['packages'])} package(s)  "
        f"set {record['package_set_hash']}"
    )
    for name, version in record["packages"].items():
        typer.echo(f"  {name}=={version}")


@packages_app.command("diff")
def packages_diff(
    job_a: Annotated[str, typer.Argument(help="First job")],
    job_b: Annotated[str, typer.Argument(help="Second job")],
    db: _DbOption = DEFAULT_DB_PATH,
    json_out: _JsonOption = False,
) -> None:
    """Show how the second job's installed packages differ from the first job's."""
    with closing(_open_lineage_db(db)) as conn:
        first = _job_packages_or_exit(conn, job_a)
        second = _job_packages_or_exit(conn, job_b)
    diff = diff_package_sets(first["packages"], second["packages"])
    identical = first["package_set_hash"] == second["package_set_hash"]
    if json_out:
        jobs = {"first": first["job"], "second": second["job"], "identical": identical}
        typer.echo(json.dumps({**jobs, **diff}, indent=2))
        return
    if identical:
        typer.echo(
            f"{first['job']} and {second['job']} ran with the same "
            f"{len(first['packages'])} package(s)  set {first['package_set_hash']}"
        )
        return
    typer.echo(f"Packages of {second['job']} compared with {first['job']}")
    for name, change in diff["changed"].items():
        typer.echo(f"  changed  {name}  {change['from']} -> {change['to']}")
    for name, version in diff["added"].items():
        typer.echo(f"  added    {name}=={version}")
    for name, version in diff["removed"].items():
        typer.echo(f"  removed  {name}=={version}")


if __name__ == "__main__":
    main()
