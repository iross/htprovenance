"""Backfill provenance.db's condor_history table from three independent sources.

condor_history keeps fields the event-log/NDJSON pipeline (log_monitor.py,
post.py) never sees: final Owner, the exact RemoteHost/LastRemoteHost,
ExitCode, and the originally requested resources alongside what was actually
used. Three entry points write into the same `condor_history` table, tagged
by `source`, merging rather than overwriting when more than one applies to
the same (schedd, cluster_id, proc_id) -- see db.py's condor_history
docstring and _upsert_history_row. Every row needs a schedd, since
ClusterIds are only unique per access point:

  enrich_from_condor_history() -- queries condor_history via the HTCondor
      Python bindings (Schedd.history, not the condor_history CLI) for
      cluster_ids already known to provenance.db that belong to the schedd
      being queried (or have no recorded schedd). The row's schedd comes
      from the ad's GlobalJobId.
      htcondor2 is a Linux-only dependency (see pyproject.toml) -- this
      module is only importable where it's installed, which is why query.py's
      CLI wiring imports it lazily inside the command body rather than at
      module load time. Opportunistic and per-cluster_id: a cluster_id with
      no matching historical record (aged out of the schedd's history
      retention, wrong schedd, or a job that never actually reached
      HTCondor) is counted and skipped, not treated as an error --
      condor_history is a retention-limited cache, not a permanent record.

  write_scan_records() -- writes htprovenance.event_log_scan.scan_event_log()'s
      output (parsed directly from a raw HTCondor event log; no condor_history
      query needed). Every record must carry a schedd.

  enrich_from_jobad_events() -- mirrors job.assigned events (jobad.py's in-job
      $_CONDOR_JOB_AD capture, emitted immediately at job start -- see
      pretrain_local.sh) that are already in provenance.db's `events` table.
      Deliberately excludes wall_time_s/cpu_usage/peak_memory_mb/gpu_usage:
      the job ad is captured before the job has run, so those fields would be
      near-zero placeholders, not real usage. Events recorded before
      job.assigned carried a schedd need one supplied by the caller.

The ClassAd's Environment attribute is never stored, even in the raw
condor_history_json blob: it's where secrets like WANDB_API_KEY live (see
htprovenance.post._SENSITIVE_AD_KEYS). Only run_id is extracted out of it,
via the same regex post.py's run_id_from_classad uses.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from htprovenance.db import _init_schema
from htprovenance.post import (
    _SENSITIVE_AD_KEYS,
    run_id_from_classad,
    schedd_from_global_job_id,
)

logger = logging.getLogger(__name__)

# ClassAd attribute -> condor_history column name. Args (not Arguments),
# RequestGPUs (not RequestGpus), and JOBGLIDEIN_ResourceName/
# MachineAttrGLIDEIN_ResourceName0/MachineAttrMachine0 (not
# GLIDEIN_ResourceName) are HTCondor's actual attribute names for this
# repo's submit descriptions -- confirmed against a live job's
# $_CONDOR_JOB_AD, not stylistic choices. See post.py's
# _DEFAULT_FIELD_MAPPING, which this mirrors (including the
# resource_name/glidein_resource_name split documented there).
_FIELD_MAPPING = {
    "ClusterId": "cluster_id",
    "ProcId": "proc_id",
    "Owner": "owner",
    "Cmd": "cmd",
    "Args": "arguments",
    "JobStatus": "job_status",
    "ExitCode": "exit_code",
    "RemoteHost": "remote_host",
    "LastRemoteHost": "last_remote_host",
    "RequestCpus": "request_cpus",
    "RequestMemory": "request_memory",
    "RequestGPUs": "request_gpus",
    "RemoteWallClockTime": "remote_wall_clock_s",
    "CPUsUsage": "cpus_usage",
    "MemoryUsage": "memory_usage_mb",
    "GPUsUsage": "gpus_usage",
    "JOBGLIDEIN_ResourceName": "resource_name",
    "MachineAttrGLIDEIN_ResourceName0": "glidein_resource_name",
    "MachineAttrMachine0": "machine",
    "JobBatchName": "job_batch_name",
    "HoldReason": "hold_reason",
    "QDate": "qdate",
    "JobStartDate": "job_start_date",
    "CompletionDate": "completion_date",
}

# Environment is projected (to recover run_id) but is never stored -- see
# module docstring -- so it's kept out of _FIELD_MAPPING's column mapping.
_PROJECTION = list(dict.fromkeys(["ClusterId", "Environment", "GlobalJobId", *_FIELD_MAPPING]))

# job_name/site/gpu_ids are event-log-only fields (event_log_scan.py); status
# is populated by both sources but from different vocabularies (see
# _JOB_STATUS_TO_STATUS / event_log_scan._status) -- the `source` column says
# which applies to a given row.
_HISTORY_COLUMNS = [
    "schedd", *_FIELD_MAPPING.values(), "run_id", "job_name", "site", "gpu_ids", "status",
    "source", "condor_history_json", "event_log_json", "jobad_json", "queried_at",
]

# HTCondor's numeric JobStatus ClassAd attribute -> a human-readable status
# string, so condor_history-sourced and event-log-sourced rows can both be
# queried via one `status` column (their vocabularies differ, but overlap
# where it matters, e.g. both spell a held job "held").
_JOB_STATUS_TO_STATUS = {
    1: "idle",
    2: "running",
    3: "removed",
    4: "completed",
    5: "held",
    6: "transferring_output",
    7: "suspended",
}

# event_log_scan.py record field -> condor_history column name.
_SCAN_FIELD_MAPPING = {
    "schedd": "schedd",
    "cluster_id": "cluster_id",
    "proc_id": "proc_id",
    "run_id": "run_id",
    "job_name": "job_name",
    "execute_host": "remote_host",
    "site": "site",
    "resource_name": "resource_name",
    "wall_time_s": "remote_wall_clock_s",
    "cpu_usage": "cpus_usage",
    "peak_memory_mb": "memory_usage_mb",
    "gpu_usage": "gpus_usage",
    "gpu_ids": "gpu_ids",
    "status": "status",
}

# job.assigned event field (jobad.py's capture_job_ad_fields(), see
# pretrain_local.sh) -> condor_history column name. Deliberately excludes
# wall_time_s/cpu_usage/peak_memory_mb/gpu_usage even though
# capture_job_ad_fields() can return them: the job ad is captured at
# submission, before the job has run, so those would be near-zero
# placeholders -- see the condor_history table's docstring in db.py.
_JOBAD_FIELD_MAPPING = {
    "schedd": "schedd",
    "cluster_id": "cluster_id",
    "proc_id": "proc_id",
    "run_id": "run_id",
    "resource_name": "resource_name",
    "glidein_resource_name": "glidein_resource_name",
    "machine": "machine",
    "job_batch_name": "job_batch_name",
    "arguments": "arguments",
    "request_cpus": "request_cpus",
    "request_memory": "request_memory",
    "request_gpus": "request_gpus",
}

# Schedd.history() scans its backing store (history file(s)) sequentially
# per call regardless of constraint complexity -- more, smaller batches
# means more full scans, not less total scanning work. Batch size is a
# safety valve against a constraint expression becoming unreasonably long,
# not a real cost-control knob, so default it high.
_DEFAULT_BATCH_SIZE = 500


@dataclass
class EnrichStats:
    """Outcome counts from an enrich_from_condor_history() run; str()s into a summary line."""

    enriched: int = 0
    not_found: int = 0
    already_enriched: int = 0
    query_errors: int = 0

    def __str__(self) -> str:
        return (
            f"condor_history: {self.enriched} enriched, "
            f"{self.not_found} not found in history, "
            f"{self.already_enriched} already enriched (skipped), "
            f"{self.query_errors} query errors"
        )


def _chunks(seq: list, size: int):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _known_cluster_ids(conn: sqlite3.Connection, schedd_name: str) -> set[int]:
    """Return the cluster_ids known to the db that belong to schedd_name.

    Sources: the `events` table (built by `db build` from NDJSON) and
    condor_history itself (rows already written by scan --db/enrich-jobad
    for jobs that never went through NDJSON at all). Without the
    condor_history half, a provenance.db populated purely via `scan --db`
    would give enrich-history nothing to discover.

    A cluster_id counts as schedd_name's when its recorded schedd matches,
    or when no schedd was recorded: log_monitor's events and job.assigned
    events from before schedd was captured carry none, and they are assumed
    to belong to whichever schedd is being queried, as before.
    """
    from_events = conn.execute(
        "SELECT DISTINCT json_extract(payload_json, '$.schedd'), "
        "CAST(json_extract(payload_json, '$.cluster_id') AS INTEGER) "
        "FROM events WHERE json_extract(payload_json, '$.cluster_id') IS NOT NULL"
    )
    from_condor_history = conn.execute("SELECT DISTINCT schedd, cluster_id FROM condor_history")
    return {
        cluster_id
        for schedd, cluster_id in [*from_events, *from_condor_history]
        if schedd is None or schedd == schedd_name
    }


def _row_from_ad(ad: dict, queried_schedd: str) -> dict:
    """Map a condor_history ClassAd (as a plain dict) to a condor_history table row.

    schedd comes from the ad's GlobalJobId, falling back to the name of the
    schedd that was queried. Never includes the raw Environment attribute --
    see module docstring.
    """
    row = {column: ad[ad_key] for ad_key, column in _FIELD_MAPPING.items() if ad_key in ad}
    row["schedd"] = schedd_from_global_job_id(ad.get("GlobalJobId", "")) or queried_schedd
    run_id = run_id_from_classad(ad)
    row["run_id"] = run_id if run_id != "unknown" else None
    row["status"] = _JOB_STATUS_TO_STATUS.get(row.get("job_status"))
    row["source"] = "condor_history"
    sanitized = {k: v for k, v in ad.items() if k not in _SENSITIVE_AD_KEYS}
    row["condor_history_json"] = json.dumps(sanitized, default=str)
    return row


def _row_from_scan_record(rec: dict, now: str) -> dict:
    """Map an event_log_scan.py record to a condor_history row (source='event_log')."""
    row = {column: rec[key] for key, column in _SCAN_FIELD_MAPPING.items() if key in rec}
    row["source"] = "event_log"
    row["event_log_json"] = json.dumps(rec, default=str)
    row["queried_at"] = now
    return row


def write_scan_records(db_path: str | Path, records: list[dict]) -> int:
    """Upsert event_log_scan.py's scan records into condor_history (source='event_log').

    Args:
        db_path: Path to the provenance SQLite database (see htprovenance.db).
        records: Records as returned by htprovenance.event_log_scan.scan_event_log.

    Returns:
        Number of rows written.

    Raises:
        ValueError: if any record has no schedd. Nothing is written in that
            case: a row keyed without its schedd could never merge with
            enrich-history's rows for the same job.
    """
    missing = sorted(
        f"{rec.get('cluster_id')}.{rec.get('proc_id')}" for rec in records if not rec.get("schedd")
    )
    if missing:
        shown = ", ".join(missing[:5]) + (" ..." if len(missing) > 5 else "")
        raise ValueError(
            f"{len(missing)} scan record(s) have no schedd (jobs {shown}); the event log "
            "didn't name the submitting schedd. Pass the schedd name explicitly "
            "(scan --schedd <name>)."
        )
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        _init_schema(conn)
        for rec in records:
            _upsert_history_row(conn, _row_from_scan_record(rec, now))
        conn.commit()
    finally:
        conn.close()
    return len(records)


def _row_from_jobad_event(event: dict) -> dict:
    """Map a job.assigned event's payload to a condor_history row (source='jobad')."""
    row = {column: event[key] for key, column in _JOBAD_FIELD_MAPPING.items() if key in event}
    row["source"] = "jobad"
    row["jobad_json"] = json.dumps(event, default=str)
    return row


def require_enrich_input(db_path: str | Path, tables: tuple[str, ...]) -> None:
    """Fail fast when db_path has nothing for an enrich command to read.

    sqlite3.connect creates a missing file, so without this check a mistyped
    --db (or one that `db build` never wrote) silently "succeeds" against a
    new, empty database and reports zero rows.

    Args:
        db_path: Path to the provenance SQLite database.
        tables: Tables the command reads its jobs from; at least one must
            have a row.

    Raises:
        FileNotFoundError: if db_path does not exist (it is not created).
        ValueError: if every table in tables is missing or empty.
    """
    path = Path(db_path)
    build_hint = f"run `htprovenance-query db build --db {path}` first"
    if not path.exists():
        raise FileNotFoundError(f"{path} does not exist; {build_hint}")
    conn = sqlite3.connect(path)
    try:
        for table in tables:
            try:
                if conn.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone():
                    return
            except sqlite3.OperationalError:  # table doesn't exist yet
                continue
    finally:
        conn.close()
    raise ValueError(f"{path} has no rows in {' or '.join(tables)}; {build_hint}")


def enrich_from_jobad_events(db_path: str | Path, *, default_schedd: str | None = None) -> int:
    """Mirror job.assigned events (jobad.py's in-job $_CONDOR_JOB_AD capture) into condor_history.

    job.assigned is emitted immediately from within the running job (see
    pretrain_local.sh) and already lands in the `events` table via
    htprovenance.db.build_database -- this reads it from there rather
    than re-scanning NDJSON directly, so it only ever needs provenance.db.

    capture_job_ad_fields() only started including cluster_id/proc_id as of
    v0.1.0rc21 -- a job.assigned event written by an older mldag version has
    neither. For those, cluster_id is backfilled by cross-referencing any
    other event carrying the same run_id (job.executing/job.queued/etc, from
    log_monitor.py, always carry both run_id and cluster_id together); a
    job.assigned with no cluster_id of its own and no cluster_id-bearing
    event sharing its run_id is skipped, not counted. proc_id defaults to 0
    when backfilled this way (matching every other DAGMan-submitted job --
    see event_log_scan.py's job-array handling for the one case where that
    assumption doesn't hold).

    job.assigned events only carry a schedd when they were captured from a
    job ad with a GlobalJobId by a version that records it. For older events,
    default_schedd is used; without one they are skipped and counted in a
    warning, since a condor_history row needs a schedd.

    Args:
        db_path: Path to the provenance SQLite database (see htprovenance.db).
        default_schedd: Schedd name for events that don't record one.

    Returns:
        Number of rows written.

    Raises:
        FileNotFoundError, ValueError: if db_path is missing or has no events
            (see require_enrich_input).
    """
    require_enrich_input(db_path, ("events",))
    now = datetime.now(timezone.utc).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        _init_schema(conn)
        run_id_to_cluster_id = {
            run_id: cluster_id
            for run_id, cluster_id in conn.execute(
                "SELECT run_id, CAST(json_extract(payload_json, '$.cluster_id') AS INTEGER) "
                "FROM events WHERE json_extract(payload_json, '$.cluster_id') IS NOT NULL"
            )
        }
        rows = conn.execute(
            "SELECT run_id, payload_json FROM events WHERE type = 'job.assigned'"
        ).fetchall()
        count = 0
        no_schedd = 0
        for run_id, payload_json in rows:
            event = json.loads(payload_json)
            if "cluster_id" not in event:
                cluster_id = run_id_to_cluster_id.get(run_id)
                if cluster_id is None:
                    continue
                event["cluster_id"] = cluster_id
            event.setdefault("proc_id", 0)
            if not event.get("schedd"):
                if default_schedd is None:
                    no_schedd += 1
                    continue
                event["schedd"] = default_schedd
            row = _row_from_jobad_event(event)
            row["queried_at"] = now
            _upsert_history_row(conn, row)
            count += 1
        conn.commit()
    finally:
        conn.close()
    if no_schedd:
        logger.warning(
            "Skipped %d job.assigned event(s) that record no schedd; rerun with "
            "enrich-jobad --schedd <name> to mirror them.",
            no_schedd,
        )
    return count


_MERGE_COLUMNS = [
    c
    for c in _HISTORY_COLUMNS
    if c not in ("schedd", "cluster_id", "proc_id", "source", "queried_at")
]


def _upsert_history_row(conn: sqlite3.Connection, row: dict) -> None:
    """Insert row, or merge it into an existing (schedd, cluster_id, proc_id) row.

    A merge is column-by-column COALESCE(new, existing): a column this write
    doesn't know about keeps whatever value was already there, rather than
    being blanked to NULL. Without this, writing from condor_history and then
    from an event-log scan (or vice versa) for the same job would silently
    erase whichever columns only the first write populated -- see db.py's
    condor_history docstring.
    """
    row = {**dict.fromkeys(_HISTORY_COLUMNS), **row}
    new_source = row["source"]
    assert isinstance(new_source, str), "_row_from_ad/_row_from_scan_record always set source"
    if not row["schedd"]:
        raise ValueError(
            f"condor_history row for {row['cluster_id']}.{row['proc_id']} "
            f"(source={new_source}) has no schedd"
        )

    existing = conn.execute(
        "SELECT source FROM condor_history WHERE schedd = ? AND cluster_id = ? AND proc_id = ?",
        (row["schedd"], row["cluster_id"], row["proc_id"]),
    ).fetchone()
    if existing and existing[0]:
        row["source"] = ",".join(sorted({*existing[0].split(","), new_source}))

    columns = ", ".join(_HISTORY_COLUMNS)
    placeholders = ", ".join(f":{c}" for c in _HISTORY_COLUMNS)
    merge_clause = ", ".join(
        f"{c} = COALESCE(excluded.{c}, condor_history.{c})" for c in _MERGE_COLUMNS
    )
    conn.execute(
        f"""
        INSERT INTO condor_history ({columns}) VALUES ({placeholders})
        ON CONFLICT (schedd, cluster_id, proc_id) DO UPDATE SET
            {merge_clause},
            source = excluded.source,
            queried_at = excluded.queried_at
        """,
        row,
    )


def _get_schedd(schedd_name: str | None, pool: str | None):
    """Return (htcondor2.Schedd, its name) for schedd_name, or the local schedd if unset.

    The name is needed up front to decide which of the db's cluster_ids belong
    to this schedd, so even the local schedd is located through the collector
    to read its Name.
    """
    import htcondor2 as htcondor

    collector = htcondor.Collector(pool) if pool else htcondor.Collector()
    if schedd_name:
        location_ad = collector.locate(htcondor.DaemonTypes.Schedd, schedd_name)
    else:
        location_ad = collector.locate(htcondor.DaemonTypes.Schedd)
    return htcondor.Schedd(location_ad), location_ad["Name"]


def _noop_progress(_msg: str) -> None:
    pass


def _target_cluster_ids(
    conn: sqlite3.Connection, schedd_name: str, full_rescan: bool, stats: EnrichStats
) -> list[int]:
    """Return schedd_name's cluster_ids to query, recording skips in stats.already_enriched.

    "Already enriched" is scoped to source='condor_history' rows on this
    schedd (not "any row exists"): a cluster_id touched only by scan --db or
    enrich-jobad was never actually condor_history-queried.
    """
    all_ids = _known_cluster_ids(conn, schedd_name)
    if full_rescan:
        return sorted(all_ids)
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT DISTINCT cluster_id FROM condor_history "
            "WHERE schedd = ? AND ',' || source || ',' LIKE '%,condor_history,%'",
            (schedd_name,),
        )
    }
    target_ids = sorted(all_ids - existing)
    stats.already_enriched = len(all_ids) - len(target_ids)
    return target_ids


def enrich_from_condor_history(
    db_path: str | Path,
    *,
    schedd_name: str | None = None,
    pool: str | None = None,
    batch_size: int = _DEFAULT_BATCH_SIZE,
    full_rescan: bool = False,
    on_progress: Callable[[str], None] | None = None,
) -> EnrichStats:
    """Backfill db_path's condor_history table for the queried schedd's cluster_ids.

    cluster_ids come from the events table (NDJSON) and condor_history itself
    (rows already written by scan --db/enrich-jobad), restricted to those
    recorded on this schedd or recorded with no schedd -- see
    _known_cluster_ids.

    Args:
        db_path: Path to the provenance SQLite database (see htprovenance.db).
        schedd_name: Name of the schedd to query, as HTCondor's collector knows
            it. Defaults to the local schedd.
        pool: Collector address to locate the schedd through; defaults to
            the local pool.
        batch_size: How many cluster_ids to fold into one condor_history
            constraint query.
        full_rescan: Re-query every one of this schedd's cluster_ids, including
            ones already in condor_history, ignoring recorded results.
        on_progress: Optional callback invoked with a one-line status message
            before the first query and after each batch completes -- a batch
            of condor_history queries against a large events table can take a
            while, and this function otherwise gives no sign of life until it
            returns. No-op when omitted.

    Returns:
        EnrichStats with counts of what was enriched/not-found/skipped.

    Raises:
        ImportError: if htcondor2 (a Linux-only dependency) is not installed.
        FileNotFoundError, ValueError: if db_path is missing or has neither
            events nor condor_history rows (see require_enrich_input).
    """
    require_enrich_input(db_path, ("events", "condor_history"))
    on_progress = on_progress or _noop_progress

    now = datetime.now(timezone.utc).isoformat()
    stats = EnrichStats()
    conn = sqlite3.connect(db_path)
    try:
        _init_schema(conn)
        schedd, resolved_schedd_name = _get_schedd(schedd_name, pool)
        target_ids = _target_cluster_ids(conn, resolved_schedd_name, full_rescan, stats)

        if not target_ids:
            on_progress("No new cluster_ids to enrich.")
            return stats

        batches = list(_chunks(target_ids, batch_size))
        on_progress(
            f"Querying condor_history on {resolved_schedd_name} for {len(target_ids)} "
            f"cluster_id(s) in {len(batches)} batch(es) of up to {batch_size}..."
        )

        for batch_num, batch in enumerate(batches, start=1):
            constraint = " || ".join(f"ClusterId == {cid}" for cid in batch)
            batch_started = time.monotonic()
            try:
                ads = schedd.history(constraint=constraint, projection=_PROJECTION, match=len(batch))
                ads = list(ads)
            except Exception as exc:
                # Schedd.history() is an RPC to an external daemon -- timeouts,
                # auth failures, and an unreachable schedd all surface here as
                # different exception types; none should abort batches already
                # enriched, so log and move on rather than letting one bad
                # batch fail the whole run.
                logger.warning(
                    "condor_history query failed for %d cluster_id(s): %s", len(batch), exc
                )
                stats.query_errors += len(batch)
                on_progress(f"Batch {batch_num}/{len(batches)}: query failed ({exc})")
                continue
            batch_elapsed = time.monotonic() - batch_started

            found: set[int] = set()
            for ad in ads:
                ad_dict = dict(ad)
                cluster_id = ad_dict.get("ClusterId")
                if cluster_id is None:
                    continue
                cluster_id = int(cluster_id)
                found.add(cluster_id)
                row = _row_from_ad(ad_dict, resolved_schedd_name)
                row["queried_at"] = now
                _upsert_history_row(conn, row)
                stats.enriched += 1
            stats.not_found += len(set(batch) - found)
            on_progress(
                f"Batch {batch_num}/{len(batches)}: {len(found)} enriched, "
                f"{len(batch) - len(found)} not found, "
                f"took {batch_elapsed:.1f}s "
                f"({stats.enriched} enriched so far)"
            )
        conn.commit()
    finally:
        conn.close()
    return stats
