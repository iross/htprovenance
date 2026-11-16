"""File lineage queries over provenance.db's file_edges table.

file_edges holds one row per file a job read or wrote, with its SHA-256
(see htprovenance.filehash and db.py). A file is identified by its hash, so
jobs link up wherever one job's output hash is another job's input hash,
whatever the files were called.

    software_for_output(conn, sha256)  -- the jobs that wrote a file, with
                                          their executable, container and
                                          recorded environment
    downstream_outputs(conn, sha256)   -- the jobs that read a file, with
                                          what each of them wrote
    chain_between(conn, start, end)    -- the shortest series of jobs
                                          leading from one file to another

CLI: `htprovenance-query files software|downstream|chain`.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections import deque
from pathlib import Path

from htprovenance.sidecar import sha256_file

_SHA256_HEX = re.compile(r"[0-9a-f]{64}")
_ENVIRONMENT_FIELDS = (
    "python", "cuda", "code_commit", "htprovenance_version", "package_count", "package_set_hash",
)
_EDGE_COLUMNS = (
    "schedd", "cluster_id", "proc_id", "role", "path", "sha256", "size", "status",
    "source", "destination_url", "run_id",
)

Job = tuple[str, int, int]


def job_label(job: Job) -> str:
    """Return "<schedd>#<cluster>.<proc>", the first two parts of a GlobalJobId."""
    schedd, cluster_id, proc_id = job
    return f"{schedd}#{cluster_id}.{proc_id}"


def resolve_hash(conn: sqlite3.Connection, file_or_hash: str) -> str:
    """Return the SHA-256 a hash, a local file, or a recorded file name refers to.

    Tried in that order. A recorded name is matched against the sandbox
    path, the transfer source and the output destination of every file in
    file_edges.

    Raises:
        LookupError: if the name was never recorded, or was recorded with
            more than one content hash (pass the hash you mean instead).
    """
    candidate = file_or_hash.lower().removeprefix("sha256:")
    if _SHA256_HEX.fullmatch(candidate):
        return candidate
    if Path(file_or_hash).is_file():
        return sha256_file(file_or_hash)
    rows = conn.execute(
        "SELECT DISTINCT sha256 FROM file_edges "
        "WHERE sha256 IS NOT NULL AND ? IN (path, source, destination_url) ORDER BY sha256",
        (file_or_hash,),
    ).fetchall()
    if not rows:
        raise LookupError(
            f"{file_or_hash!r} is not a SHA-256, an existing local file, or a file name "
            "recorded in file_edges"
        )
    if len(rows) > 1:
        hashes = ", ".join(row[0] for row in rows)
        raise LookupError(
            f"{file_or_hash!r} was recorded with {len(rows)} different contents; "
            f"pass one of these hashes instead: {hashes}"
        )
    return rows[0][0]


def _edges(conn: sqlite3.Connection, where: str, params: tuple) -> list[dict]:
    # where is always one of this module's own constant clauses, never caller text.
    rows = conn.execute(
        f"SELECT {', '.join(_EDGE_COLUMNS)} FROM file_edges WHERE {where} "  # noqa: S608
        "ORDER BY schedd, cluster_id, proc_id, role, path",
        params,
    ).fetchall()
    return [dict(zip(_EDGE_COLUMNS, row)) for row in rows]


def _job_of(edge: dict) -> Job:
    return edge["schedd"], edge["cluster_id"], edge["proc_id"]


def _job_edges(conn: sqlite3.Connection, job: Job, *, outputs: bool) -> list[dict]:
    comparison = "=" if outputs else "!="
    return _edges(
        conn,
        f"schedd = ? AND cluster_id = ? AND proc_id = ? AND role {comparison} 'output'",
        job,
    )


def _file_summary(edge: dict) -> dict:
    keys = ("path", "sha256", "size", "status", "source", "destination_url")
    return {key: edge[key] for key in keys if edge.get(key) is not None}


def _environment(conn: sqlite3.Connection, job: Job) -> dict:
    """Return the software fields (_ENVIRONMENT_FIELDS) of the job's job.assigned event."""
    row = conn.execute(
        "SELECT payload_json FROM events WHERE type = 'job.assigned' "
        "AND json_extract(payload_json, '$.schedd') = ? "
        "AND json_extract(payload_json, '$.cluster_id') = ? "
        "AND json_extract(payload_json, '$.proc_id') = ? ORDER BY ts DESC LIMIT 1",
        job,
    ).fetchone()
    if row is None:
        return {}
    payload = json.loads(row[0])
    return {key: payload[key] for key in _ENVIRONMENT_FIELDS if key in payload}


def _job_record(job: Job, run_id: str) -> dict:
    schedd, cluster_id, proc_id = job
    return {
        "job": job_label(job),
        "schedd": schedd,
        "cluster_id": cluster_id,
        "proc_id": proc_id,
        "run_id": run_id,
    }


def software_for_output(conn: sqlite3.Connection, sha256: str) -> list[dict]:
    """Return every job that wrote a file with this hash, and the software it ran.

    Each record has the job, the output path(s) with that hash, its
    executable and container file entries (None when not recorded), and the
    environment from its job.assigned event ({} when the job had no
    autocapture).
    """
    produced: dict[Job, list[dict]] = {}
    for edge in _edges(conn, "sha256 = ? AND role = 'output'", (sha256,)):
        produced.setdefault(_job_of(edge), []).append(edge)

    records = []
    for job, edges in produced.items():
        by_role = {e["role"]: e for e in _job_edges(conn, job, outputs=False)}
        records.append({
            **_job_record(job, edges[0]["run_id"]),
            "outputs": [_file_summary(e) for e in edges],
            "executable": _file_summary(by_role["executable"]) if "executable" in by_role else None,
            "container": _file_summary(by_role["container"]) if "container" in by_role else None,
            "environment": _environment(conn, job),
        })
    return records


def downstream_outputs(conn: sqlite3.Connection, sha256: str) -> list[dict]:
    """Return every job that read a file with this hash, and the outputs each wrote.

    "Read" covers any non-output role, so a job that ran the file as its
    executable or container counts too.
    """
    consumed: dict[Job, list[dict]] = {}
    for edge in _edges(conn, "sha256 = ? AND role != 'output'", (sha256,)):
        consumed.setdefault(_job_of(edge), []).append(edge)

    return [
        {
            **_job_record(job, edges[0]["run_id"]),
            "read_as": [_file_summary(e) | {"role": e["role"]} for e in edges],
            "outputs": [_file_summary(e) for e in _job_edges(conn, job, outputs=True)],
        }
        for job, edges in consumed.items()
    ]


def chain_between(conn: sqlite3.Connection, start: str, end: str) -> list[dict] | None:
    """Return the shortest series of jobs turning the file `start` into `end`.

    Each step is a job with the input it read and the output it wrote; one
    step's output hash is the next step's input hash. [] when start and end
    are the same file; None when no recorded jobs connect them.
    """
    if start == end:
        return []
    reached_by: dict[str, dict | None] = {start: None}
    queue = deque([start])
    while queue:
        current = queue.popleft()
        for read in _edges(conn, "sha256 = ? AND role != 'output'", (current,)):
            job = _job_of(read)
            for written in _job_edges(conn, job, outputs=True):
                out_hash = written["sha256"]
                if out_hash is None or out_hash in reached_by:
                    continue
                reached_by[out_hash] = {
                    **_job_record(job, read["run_id"]),
                    "input": _file_summary(read) | {"role": read["role"]},
                    "output": _file_summary(written),
                }
                if out_hash == end:
                    return _walk_back(reached_by, end)
                queue.append(out_hash)
    return None


def _walk_back(reached_by: dict[str, dict | None], end: str) -> list[dict]:
    steps = []
    step = reached_by[end]
    while step is not None:
        steps.append(step)
        step = reached_by[step["input"]["sha256"]]
    return list(reversed(steps))
