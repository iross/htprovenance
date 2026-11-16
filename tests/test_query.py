import hashlib
import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from htprovenance.query import app, query_run, walk_lineage
from htprovenance.sidecar import write_sidecar

runner = CliRunner()

SITE = {"hostname": "gpu04", "slot": "slot1", "gpu_model": "A100", "gpu_count": 1}
ENV = {"python": "3.11", "cuda": "12.2", "code_commit": "abc"}


def _make_ckpt(path: Path, content: bytes = b"weights") -> Path:
    path.write_bytes(content)
    return path


def _build_chain(tmp_path: Path, n: int) -> list[Path]:
    """Create n checkpoints with a valid sidecar chain."""
    ckpts = []
    parent_hash = None
    for i in range(n):
        ckpt = _make_ckpt(tmp_path / f"epoch{i}.ckpt", f"weights{i}".encode())
        parent_hash = write_sidecar(ckpt, "run-test", i, parent_hash, SITE, ENV, {})
        ckpts.append(ckpt)
    return ckpts


def _write_events(log_dir: Path, run_id: str, events: list[dict]) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{run_id}.ndjson"
    with open(log_path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")


# --- walk_lineage ---


def test_walk_lineage_single_epoch(tmp_path):
    ckpts = _build_chain(tmp_path, 1)
    chain = walk_lineage(ckpts[0])
    assert len(chain) == 1
    assert chain[0]["epoch"] == 0
    assert chain[0]["parent_hash"] is None


def test_walk_lineage_three_epochs_oldest_first(tmp_path):
    ckpts = _build_chain(tmp_path, 3)
    chain = walk_lineage(ckpts[2])
    assert len(chain) == 3
    assert chain[0]["epoch"] == 0
    assert chain[1]["epoch"] == 1
    assert chain[2]["epoch"] == 2


def test_walk_lineage_parent_hash_chain_is_intact(tmp_path):
    ckpts = _build_chain(tmp_path, 3)
    chain = walk_lineage(ckpts[2])
    assert chain[0]["parent_hash"] is None
    assert chain[1]["parent_hash"] == chain[0]["checkpoint_hash"]
    assert chain[2]["parent_hash"] == chain[1]["checkpoint_hash"]


def test_walk_lineage_missing_sidecar_raises(tmp_path):
    ckpt = _make_ckpt(tmp_path / "epoch0.ckpt")
    with pytest.raises(FileNotFoundError, match="No sidecar"):
        walk_lineage(ckpt)


def test_walk_lineage_corrupt_sidecar_raises(tmp_path):
    ckpt = _make_ckpt(tmp_path / "epoch0.ckpt")
    (tmp_path / "epoch0.ckpt.provenance.json").write_text("not json")
    with pytest.raises(ValueError, match="Corrupt sidecar"):
        walk_lineage(ckpt)


def test_walk_lineage_missing_parent_raises(tmp_path):
    ckpts = _build_chain(tmp_path, 2)
    # Remove epoch0 sidecar to break the chain
    (tmp_path / "epoch0.ckpt.provenance.json").unlink()
    with pytest.raises(FileNotFoundError):
        walk_lineage(ckpts[1])


# --- query_run ---


def test_query_run_returns_events_sorted_by_ts(tmp_path):
    log_dir = tmp_path / "provenance"
    events = [
        {"schema_version": "1.0", "type": "epoch.completed", "run_id": "r1", "ts": "2026-01-01T01:00:00+00:00"},
        {"schema_version": "1.0", "type": "job.submitted",   "run_id": "r1", "ts": "2026-01-01T00:00:00+00:00"},
        {"schema_version": "1.0", "type": "epoch.started",   "run_id": "r1", "ts": "2026-01-01T00:30:00+00:00"},
    ]
    _write_events(log_dir, "r1", events)
    result = query_run("r1", log_dir)
    assert [e["type"] for e in result] == ["job.submitted", "epoch.started", "epoch.completed"]


def test_query_run_missing_log_raises(tmp_path):
    with pytest.raises(FileNotFoundError, match="No event log"):
        query_run("nonexistent-run", tmp_path)


def test_query_run_skips_malformed_lines(tmp_path):
    log_dir = tmp_path / "provenance"
    log_dir.mkdir()
    (log_dir / "r2.ndjson").write_text(
        '{"schema_version":"1.0","type":"job.submitted","run_id":"r2","ts":"2026-01-01T00:00:00+00:00"}\n'
        "not json\n"
        '{"schema_version":"1.0","type":"job.completed","run_id":"r2","ts":"2026-01-01T01:00:00+00:00"}\n'
    )
    result = query_run("r2", log_dir)
    assert len(result) == 2


def _event(event_type: str, run_id: str, ts: str) -> dict:
    return {"schema_version": "1.0", "type": event_type, "run_id": run_id, "ts": ts}


def test_query_run_merges_run_file_and_per_job_files(tmp_path):
    _write_events(tmp_path, "r1", [_event("job.submitted", "r1", "2026-01-01T00:00:00")])
    _write_events(tmp_path, "r1.555.0", [_event("job.assigned", "r1", "2026-01-01T00:01:00")])
    _write_events(tmp_path, "r1.556.0", [_event("epoch.completed", "r1", "2026-01-01T00:02:00")])

    result = query_run("r1", tmp_path)

    assert [e["type"] for e in result] == ["job.submitted", "job.assigned", "epoch.completed"]


def test_query_run_reads_per_job_files_without_run_file(tmp_path):
    _write_events(tmp_path, "cluster-555.555.0", [_event("job.assigned", "cluster-555", "t")])

    assert [e["type"] for e in query_run("cluster-555", tmp_path)] == ["job.assigned"]


def test_query_run_matches_sanitized_file_names_by_event_run_id(tmp_path):
    """'my sweep' is filed as my_sweep.*; 'my_sweep' and 'my.sweep' runs must not leak in."""
    _write_events(tmp_path, "my_sweep.555.0", [_event("job.assigned", "my sweep", "t1")])
    _write_events(tmp_path, "my_sweep.556.0", [_event("job.assigned", "my_sweep", "t2")])
    _write_events(tmp_path, "my.sweep.557.0", [_event("job.assigned", "my.sweep", "t3")])

    assert [e["ts"] for e in query_run("my sweep", tmp_path)] == ["t1"]
    assert [e["ts"] for e in query_run("my", tmp_path)] == []


# --- CLI: lineage ---


def test_cli_lineage_human_output(tmp_path):
    ckpts = _build_chain(tmp_path, 2)
    result = runner.invoke(app, ["lineage", str(ckpts[1])])
    assert result.exit_code == 0
    assert "epoch" in result.output
    assert "Lineage" in result.output


def test_cli_lineage_json_output(tmp_path):
    ckpts = _build_chain(tmp_path, 2)
    result = runner.invoke(app, ["lineage", str(ckpts[1]), "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert len(data) == 2


def test_cli_lineage_missing_checkpoint_exits_nonzero(tmp_path):
    result = runner.invoke(app, ["lineage", str(tmp_path / "missing.ckpt")])
    assert result.exit_code != 0


# --- CLI: events ---


def test_cli_events_human_output(tmp_path):
    log_dir = tmp_path / "provenance"
    _write_events(log_dir, "run-xyz", [
        {"schema_version": "1.0", "type": "job.submitted", "run_id": "run-xyz",
         "ts": "2026-01-01T00:00:00+00:00"},
    ])
    result = runner.invoke(app, ["events", "run-xyz", "--log-dir", str(log_dir)])
    assert result.exit_code == 0
    assert "job.submitted" in result.output


def test_cli_events_json_output(tmp_path):
    log_dir = tmp_path / "provenance"
    _write_events(log_dir, "run-xyz", [
        {"schema_version": "1.0", "type": "job.submitted", "run_id": "run-xyz",
         "ts": "2026-01-01T00:00:00+00:00"},
    ])
    result = runner.invoke(app, ["events", "run-xyz", "--log-dir", str(log_dir), "--json"])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert isinstance(data, list)
    assert data[0]["type"] == "job.submitted"


def test_cli_events_missing_run_exits_nonzero(tmp_path):
    result = runner.invoke(app, ["events", "no-such-run", "--log-dir", str(tmp_path)])
    assert result.exit_code != 0


# --- scan --db / --schedd ---

_EXECUTING_ONLY_LOG = (
    "001 (322.000.000) 2026-11-04 15:54:00 Job executing on host: <10.0.0.1:1234>\n...\n"
)


def test_cli_scan_db_without_schedd_exits_nonzero_and_writes_nothing(tmp_path):
    log = tmp_path / "batch.log"
    log.write_text(_EXECUTING_ONLY_LOG)
    db_path = tmp_path / "provenance.db"

    result = runner.invoke(app, ["scan", str(log), "--db", str(db_path)])

    assert result.exit_code == 1
    assert "--schedd" in result.output
    assert not db_path.exists() or _condor_history_rows(db_path) == []


def test_cli_scan_db_with_schedd_writes_row(tmp_path):
    log = tmp_path / "batch.log"
    log.write_text(_EXECUTING_ONLY_LOG)
    db_path = tmp_path / "provenance.db"

    result = runner.invoke(
        app, ["scan", str(log), "--db", str(db_path), "--schedd", "ap2002.chtc.wisc.edu"]
    )

    assert result.exit_code == 0, result.output
    assert _condor_history_rows(db_path) == [("ap2002.chtc.wisc.edu", 322, 0)]


def _condor_history_rows(db_path: Path) -> list[tuple]:
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        return conn.execute("SELECT schedd, cluster_id, proc_id FROM condor_history").fetchall()
    finally:
        conn.close()


# --- enrich commands refuse a database with nothing to enrich ---


@pytest.mark.parametrize("command", ["enrich-jobad", "enrich-history"])
def test_cli_enrich_missing_db_exits_nonzero_without_creating_it(tmp_path, command):
    db_path = tmp_path / "prov.db"

    result = runner.invoke(app, ["db", command, "--db", str(db_path)])

    assert result.exit_code == 1
    assert "does not exist" in result.output
    assert "db build" in result.output
    assert not db_path.exists()


@pytest.mark.parametrize("command", ["enrich-jobad", "enrich-history"])
def test_cli_enrich_unbuilt_db_exits_nonzero(tmp_path, command):
    from htprovenance.db import build_database

    db_path = tmp_path / "prov.db"
    build_database(db_path, [], [tmp_path / "no-events-here"])

    result = runner.invoke(app, ["db", command, "--db", str(db_path)])

    assert result.exit_code == 1
    assert "has no rows in events" in result.output


def test_cli_enrich_jobad_ignores_condor_history_rows(tmp_path):
    """enrich-jobad reads only events; a db filled by scan --db alone has nothing for it."""
    db_path = tmp_path / "prov.db"
    log_file = tmp_path / "metl.log"
    log_file.write_text(_EXECUTING_ONLY_LOG)
    scan = ["scan", str(log_file), "--db", str(db_path), "--schedd", "ap2002.chtc.wisc.edu"]
    assert runner.invoke(app, scan).exit_code == 0

    result = runner.invoke(app, ["db", "enrich-jobad", "--db", str(db_path)])

    assert result.exit_code == 1
    assert "has no rows in events;" in result.output


def test_require_enrich_input_accepts_condor_history_rows(tmp_path):
    """enrich-history can start from scan --db rows alone, with no events."""
    from htprovenance.history_enrich import require_enrich_input

    db_path = tmp_path / "prov.db"
    log_file = tmp_path / "metl.log"
    log_file.write_text(_EXECUTING_ONLY_LOG)
    scan = ["scan", str(log_file), "--db", str(db_path), "--schedd", "ap2002.chtc.wisc.edu"]
    assert runner.invoke(app, scan).exit_code == 0

    require_enrich_input(db_path, ("events", "condor_history"))


def test_cli_enrich_jobad_built_db_succeeds(tmp_path):
    from htprovenance.db import build_database

    event_dir = tmp_path / "provenance"
    event_dir.mkdir()
    (event_dir / "r1.ndjson").write_text(
        json.dumps({"type": "job.submitted", "run_id": "r1", "ts": "t"}) + "\n"
    )
    db_path = tmp_path / "prov.db"
    build_database(db_path, [], [event_dir])

    result = runner.invoke(app, ["db", "enrich-jobad", "--db", str(db_path)])

    assert result.exit_code == 0, result.output
    assert "Wrote 0 row(s)" in result.output


def test_cli_enrich_db_without_provenance_tables_exits_nonzero(tmp_path):
    """An existing file that isn't a provenance database (no tables at all)."""
    db_path = tmp_path / "prov.db"
    db_path.touch()

    result = runner.invoke(app, ["db", "enrich-history", "--db", str(db_path)])

    assert result.exit_code == 1
    assert "has no rows in events or condor_history" in result.output
