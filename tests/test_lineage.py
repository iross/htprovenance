import hashlib
import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from typer.testing import CliRunner

from htprovenance.db import build_database
from htprovenance.lineage import (
    chain_between,
    downstream_outputs,
    resolve_hash,
    software_for_output,
)
from htprovenance.query import app

runner = CliRunner()

AP1 = "ap1.example.edu"
AP2 = "ap2.example.edu"


def _h(name: str) -> str:
    """A stable fake content hash for the file contents called name."""
    return hashlib.sha256(name.encode()).hexdigest()


def _file(path: str, content: str | None, role: str, **extra) -> dict:
    if content is None:
        return {"path": path, "role": role, "status": "missing", **extra}
    return {"path": path, "role": role, "sha256": _h(content), "size": 1, "status": "ok", **extra}


def _job_events(job: tuple[str, int, int], inputs: list[dict], outputs: list[dict]) -> list[dict]:
    schedd, cluster_id, proc_id = job
    key = {
        "schema_version": "1.0",
        "run_id": f"cluster-{cluster_id}",
        "schedd": schedd,
        "cluster_id": cluster_id,
        "proc_id": proc_id,
    }
    return [
        {**key, "type": "files.inputs", "ts": "2026-01-01T00:00:00+00:00", "files": inputs},
        {**key, "type": "files.outputs", "ts": "2026-01-01T00:10:00+00:00", "files": outputs},
    ]


def _write_events(event_dir: Path, name: str, events: list[dict]) -> None:
    event_dir.mkdir(parents=True, exist_ok=True)
    with open(event_dir / f"{name}.ndjson", "a") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")


@pytest.fixture
def pipeline(tmp_path):
    """Three chained jobs on AP1: raw -> clean -> model -> report, plus a side branch.

    Job 1 (cluster 1) cleans raw.csv. Job 2 (cluster 2, proc 0 and proc 1,
    a queue-2 submit) each train on clean.csv. Job 3 (cluster 3) turns proc
    0's model into a report. Returns the path of the built database.
    """
    events = tmp_path / "events"
    _write_events(events, "cluster-1.1.0", _job_events(
        (AP1, 1, 0),
        [
            _file("raw.csv", "raw", "input", source="osdf:///staging/raw.csv"),
            _file("clean.py", "clean.py v1", "executable"),
            {"path": "docker://python:3.12", "role": "container", "status": "reference"},
        ],
        [_file("clean.csv", "clean", "output", destination_url="file:///staging/clean.csv")],
    ))
    for proc_id in (0, 1):
        _write_events(events, f"cluster-2.2.{proc_id}", _job_events(
            (AP1, 2, proc_id),
            [
                _file("clean.csv", "clean", "input"),
                _file("train.sh", "train.sh v1", "executable"),
                _file("env.sif", "env.sif v1", "container"),
            ],
            [_file("model.pt", f"model {proc_id}", "output")],
        ))
    _write_events(events, "cluster-3.3.0", _job_events(
        (AP1, 3, 0),
        [_file("model.pt", "model 0", "input"), _file("report.sh", "report.sh v1", "executable")],
        [_file("report.pdf", "report", "output"), _file("debug.log", None, "output")],
    ))
    _write_events(events, "cluster-2.2.0", [{
        "schema_version": "1.0", "type": "job.assigned", "run_id": "cluster-2",
        "ts": "2026-01-01T00:00:01+00:00", "schedd": AP1, "cluster_id": 2, "proc_id": 0,
        "python": "3.12.1", "cuda": "12.2", "code_commit": "abc123",
        "htprovenance_version": "0.1.0", "hostname": "gpu01",
    }])
    db = tmp_path / "provenance.db"
    build_database(db, [tmp_path / "none"], [events])
    return db


def _edge_count(db: Path) -> int:
    with closing(sqlite3.connect(db)) as conn:
        return conn.execute("SELECT COUNT(*) FROM file_edges").fetchone()[0]


# --- db build -> file_edges ---


def test_build_indexes_every_file_entry_under_its_job(pipeline):
    with closing(sqlite3.connect(pipeline)) as conn:
        rows = conn.execute(
            "SELECT role, path, sha256, status, source, destination_url, run_id "
            "FROM file_edges WHERE cluster_id = 1 ORDER BY role, path"
        ).fetchall()

    assert rows == [
        ("container", "docker://python:3.12", None, "reference", None, None, "cluster-1"),
        ("executable", "clean.py", _h("clean.py v1"), "ok", None, None, "cluster-1"),
        ("input", "raw.csv", _h("raw"), "ok", "osdf:///staging/raw.csv", None, "cluster-1"),
        ("output", "clean.csv", _h("clean"), "ok", None, "file:///staging/clean.csv", "cluster-1"),
    ]


def test_rebuilding_does_not_duplicate_file_edges(pipeline, tmp_path):
    before = _edge_count(pipeline)

    build_database(pipeline, [tmp_path / "none"], [tmp_path / "events"])
    build_database(pipeline, [tmp_path / "none"], [tmp_path / "events"], full_rescan=True)

    assert _edge_count(pipeline) == before == 16


def test_full_rescan_refills_an_emptied_file_edges_table(pipeline, tmp_path):
    with closing(sqlite3.connect(pipeline)) as conn:
        conn.execute("DELETE FROM file_edges")
        conn.commit()

    build_database(pipeline, [tmp_path / "none"], [tmp_path / "events"], full_rescan=True)

    assert _edge_count(pipeline) == 16


def test_same_cluster_and_proc_on_two_schedds_are_separate_jobs(tmp_path):
    events = tmp_path / "events"
    for schedd in (AP1, AP2):
        _write_events(events, f"{schedd}.5.0", _job_events(
            (schedd, 5, 0), [_file("in.txt", "shared", "input")],
            [_file("out.txt", f"out from {schedd}", "output")],
        ))
    db = tmp_path / "provenance.db"
    build_database(db, [], [events])

    with closing(sqlite3.connect(db)) as conn:
        readers = downstream_outputs(conn, _h("shared"))

    assert [(r["schedd"], r["outputs"][0]["sha256"]) for r in readers] == [
        (AP1, _h(f"out from {AP1}")),
        (AP2, _h(f"out from {AP2}")),
    ]


def test_file_event_without_a_job_key_is_kept_as_an_event_but_not_indexed(tmp_path, caplog):
    events = tmp_path / "events"
    (event,) = _job_events((AP1, 8, 0), [_file("in.txt", "x", "input")], [])[:1]
    del event["schedd"]
    _write_events(events, "cluster-8.8.0", [event])
    db = tmp_path / "provenance.db"

    stats = build_database(db, [], [events])

    assert stats.events_ingested == 1
    assert _edge_count(db) == 0
    assert "not indexed" in caplog.text


# --- resolve_hash ---


def test_resolve_hash_accepts_a_hash_with_or_without_prefix(pipeline):
    with closing(sqlite3.connect(pipeline)) as conn:
        assert resolve_hash(conn, _h("raw")) == _h("raw")
        assert resolve_hash(conn, f"sha256:{_h('raw').upper()}") == _h("raw")


def test_resolve_hash_hashes_an_existing_local_file(pipeline, tmp_path):
    local = tmp_path / "anything.bin"
    local.write_bytes(b"local bytes")

    with closing(sqlite3.connect(pipeline)) as conn:
        assert resolve_hash(conn, str(local)) == hashlib.sha256(b"local bytes").hexdigest()


def test_resolve_hash_finds_a_recorded_path_source_or_destination(pipeline):
    with closing(sqlite3.connect(pipeline)) as conn:
        assert resolve_hash(conn, "raw.csv") == _h("raw")
        assert resolve_hash(conn, "osdf:///staging/raw.csv") == _h("raw")
        assert resolve_hash(conn, "file:///staging/clean.csv") == _h("clean")


def test_resolve_hash_rejects_a_name_recorded_with_different_contents(pipeline):
    with closing(sqlite3.connect(pipeline)) as conn:
        with pytest.raises(LookupError) as excinfo:
            resolve_hash(conn, "model.pt")

    assert "2 different contents" in str(excinfo.value)
    assert _h("model 0") in str(excinfo.value)
    assert _h("model 1") in str(excinfo.value)


def test_resolve_hash_rejects_an_unknown_name(pipeline):
    with closing(sqlite3.connect(pipeline)) as conn:
        with pytest.raises(LookupError, match="never_seen.txt"):
            resolve_hash(conn, "never_seen.txt")


# --- queries ---


def test_software_for_output_reports_executable_container_and_environment(pipeline):
    with closing(sqlite3.connect(pipeline)) as conn:
        (record,) = software_for_output(conn, _h("model 0"))

    assert record["job"] == f"{AP1}#2.0"
    assert record["run_id"] == "cluster-2"
    assert [o["path"] for o in record["outputs"]] == ["model.pt"]
    assert record["executable"]["sha256"] == _h("train.sh v1")
    assert record["container"]["sha256"] == _h("env.sif v1")
    assert record["environment"] == {
        "python": "3.12.1", "cuda": "12.2", "code_commit": "abc123",
        "htprovenance_version": "0.1.0",
    }


def test_software_for_output_without_autocapture_or_hashed_container(pipeline):
    with closing(sqlite3.connect(pipeline)) as conn:
        (record,) = software_for_output(conn, _h("clean"))

    assert record["container"] == {"path": "docker://python:3.12", "status": "reference"}
    assert record["environment"] == {}


def test_software_for_output_of_a_file_no_job_wrote(pipeline):
    with closing(sqlite3.connect(pipeline)) as conn:
        assert software_for_output(conn, _h("raw")) == []


def test_downstream_outputs_lists_every_reader_and_its_outputs(pipeline):
    with closing(sqlite3.connect(pipeline)) as conn:
        readers = downstream_outputs(conn, _h("clean"))

    assert [(r["job"], [o["sha256"] for o in r["outputs"]]) for r in readers] == [
        (f"{AP1}#2.0", [_h("model 0")]),
        (f"{AP1}#2.1", [_h("model 1")]),
    ]


def test_downstream_outputs_counts_a_job_that_ran_the_file(pipeline):
    with closing(sqlite3.connect(pipeline)) as conn:
        readers = downstream_outputs(conn, _h("train.sh v1"))

    assert [r["job"] for r in readers] == [f"{AP1}#2.0", f"{AP1}#2.1"]
    assert readers[0]["read_as"][0]["role"] == "executable"


def test_downstream_outputs_includes_outputs_that_were_never_written(pipeline):
    with closing(sqlite3.connect(pipeline)) as conn:
        (reader,) = downstream_outputs(conn, _h("model 0"))

    assert {o["path"]: o["status"] for o in reader["outputs"]} == {
        "report.pdf": "ok", "debug.log": "missing",
    }


def test_chain_between_follows_three_jobs(pipeline):
    with closing(sqlite3.connect(pipeline)) as conn:
        steps = chain_between(conn, _h("raw"), _h("report"))

    assert steps is not None
    assert [step["job"] for step in steps] == [f"{AP1}#1.0", f"{AP1}#2.0", f"{AP1}#3.0"]
    assert [step["input"]["path"] for step in steps] == ["raw.csv", "clean.csv", "model.pt"]
    assert [step["output"]["path"] for step in steps] == ["clean.csv", "model.pt", "report.pdf"]


def test_chain_between_unconnected_files_is_none(pipeline):
    with closing(sqlite3.connect(pipeline)) as conn:
        assert chain_between(conn, _h("model 1"), _h("report")) is None
        assert chain_between(conn, _h("report"), _h("raw")) is None


def test_chain_between_a_file_and_itself_is_empty(pipeline):
    with closing(sqlite3.connect(pipeline)) as conn:
        assert chain_between(conn, _h("raw"), _h("raw")) == []


def test_chain_between_terminates_when_jobs_form_a_cycle(tmp_path):
    events = tmp_path / "events"
    _write_events(events, "a", _job_events(
        (AP1, 1, 0), [_file("x", "x", "input")], [_file("y", "y", "output")]))
    _write_events(events, "b", _job_events(
        (AP1, 2, 0), [_file("y", "y", "input")], [_file("x", "x", "output")]))
    db = tmp_path / "provenance.db"
    build_database(db, [], [events])

    with closing(sqlite3.connect(db)) as conn:
        assert chain_between(conn, _h("x"), _h("unrelated")) is None


# --- CLI ---


def test_cli_software_by_recorded_destination(pipeline):
    result = runner.invoke(
        app, ["files", "software", "file:///staging/clean.csv", "--db", str(pipeline)]
    )

    assert result.exit_code == 0
    assert f"{AP1}#1.0" in result.output
    assert f"clean.py  sha256:{_h('clean.py v1')}" in result.output
    assert "docker://python:3.12  (reference)" in result.output
    assert "environment  not recorded" in result.output


def test_cli_downstream_json(pipeline):
    result = runner.invoke(
        app, ["files", "downstream", _h("clean"), "--db", str(pipeline), "--json"]
    )

    assert result.exit_code == 0
    data = json.loads(result.output)
    assert data["sha256"] == _h("clean")
    assert [r["job"] for r in data["read_by"]] == [f"{AP1}#2.0", f"{AP1}#2.1"]


def test_cli_chain_prints_each_step(pipeline):
    result = runner.invoke(
        app, ["files", "chain", "raw.csv", "report.pdf", "--db", str(pipeline)]
    )

    assert result.exit_code == 0
    assert "3 job(s)" in result.output
    assert result.output.index(f"{AP1}#1.0") < result.output.index(f"{AP1}#3.0")


def test_cli_chain_exits_1_when_files_are_not_connected(pipeline):
    result = runner.invoke(
        app, ["files", "chain", "report.pdf", "raw.csv", "--db", str(pipeline)]
    )

    assert result.exit_code == 1
    assert "No recorded jobs connect" in result.output


def test_cli_reports_an_ambiguous_name(pipeline):
    result = runner.invoke(app, ["files", "software", "model.pt", "--db", str(pipeline)])

    assert result.exit_code == 1
    assert "2 different contents" in result.output


def test_cli_reports_a_missing_database(tmp_path):
    result = runner.invoke(
        app, ["files", "software", "x", "--db", str(tmp_path / "absent.db")]
    )

    assert result.exit_code == 1
    assert "does not exist" in result.output
    assert not (tmp_path / "absent.db").exists()


def test_cli_reports_a_database_built_before_file_hashes(tmp_path):
    db = tmp_path / "old.db"
    with closing(sqlite3.connect(db)) as conn:
        conn.execute("CREATE TABLE events (id INTEGER)")

    result = runner.invoke(app, ["files", "software", "x", "--db", str(db)])

    assert result.exit_code == 1
    assert "no file_edges table" in result.output
