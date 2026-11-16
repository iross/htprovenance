"""NDJSON structured provenance event emitter.

Appends one JSON line per lifecycle event to a per-run NDJSON file on the
shared filesystem.  All writes use O_APPEND mode; on POSIX systems writes
smaller than PIPE_BUF (~4 KB) are atomic, so concurrent epoch writers are
safe without additional locking.

Event log location: <log_dir>/<run_id>.ndjson, or <log_dir>/<ndjson_stem>.ndjson
                    when the caller passes one (events written inside an
                    execute-node job go to one file per cluster.proc; see
                    htprovenance.autocapture.execute_node_ndjson_stem)
Default log_dir:    output/provenance
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = "1.0"

VALID_EVENT_TYPES: frozenset[str] = frozenset({
    "job.submitted",
    "job.queued",
    "job.assigned",
    "job.executing",
    "job.reconnected",
    "job.aborted",
    "transfer.input.started",
    "transfer.input.completed",
    "transfer.output.started",
    "transfer.output.completed",
    "epoch.started",
    "epoch.completed",
    "job.migrated",
    "job.held",
    "job.released",
    "job.failed",
    "job.completed",
    "job.resource_usage",
    "dagman.started",
    "service.started",
})

_DEFAULT_LOG_DIR = "output/provenance"


def event_log_path(stem: str, log_dir: str | Path = _DEFAULT_LOG_DIR) -> Path:
    """Return the path to the NDJSON event log named stem (usually the run_id)."""
    return Path(log_dir) / f"{stem}.ndjson"


def emit_event(
    event_type: str,
    run_id: str,
    *,
    log_dir: str | Path = _DEFAULT_LOG_DIR,
    ndjson_stem: str | None = None,
    **fields,
) -> None:
    """Append one structured event line to an NDJSON log.

    Args:
        event_type: One of VALID_EVENT_TYPES.
        run_id: Stable run identifier, written into the event itself.
        log_dir: Directory for NDJSON files; created if absent.
        ndjson_stem: File name (without .ndjson) to append to. Defaults to
            run_id. db.build_database reads run_id from each event line, not
            from the file name, so the two are free to differ.
        **fields: Event-specific payload fields merged into the envelope.

    Raises:
        ValueError: If event_type is not a recognised event type.
    """
    if event_type not in VALID_EVENT_TYPES:
        raise ValueError(
            f"Unknown event type {event_type!r}. "
            f"Valid types: {sorted(VALID_EVENT_TYPES)}"
        )

    event = {
        "schema_version": SCHEMA_VERSION,
        "type": event_type,
        "run_id": run_id,
        "ts": datetime.now(timezone.utc).isoformat(),
        **fields,
    }

    log_path = event_log_path(ndjson_stem or run_id, log_dir)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Serialise to a single line; compact separators keep it under PIPE_BUF.
    line = json.dumps(event, separators=(",", ":")) + "\n"
    with open(log_path, "a") as f:
        f.write(line)
