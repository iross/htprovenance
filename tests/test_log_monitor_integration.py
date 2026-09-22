"""Integration tests for log_monitor using realistic full HTCondor event log files.

Each test feeds a complete multi-event log (or growing log across polls) through
monitor_once and verifies the full sequence of provenance events emitted.
"""

import json
from pathlib import Path

from htprovenance.log_monitor import monitor_once


# --- helpers ---


def _write_ndjson(prov_dir: Path, run_id: str, job_name: str) -> None:
    prov_dir.mkdir(parents=True, exist_ok=True)
    event = {"type": "job.submitted", "run_id": run_id, "job_name": job_name}
    (prov_dir / f"{run_id}.ndjson").write_text(json.dumps(event) + "\n")


def _read_events(log_dir: Path, run_id: str) -> list[dict]:
    path = log_dir / f"{run_id}.ndjson"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _realistic_log(*blocks: str) -> str:
    """Join event blocks with blank separator lines, as HTCondor writes them."""
    return "\n".join(blocks) + "\n"


# Realistic HTCondor event log blocks for two concurrent jobs submitted via DAGMan.
# job A: cluster 5100001, job name run0-train_epoch0, run_id run-aaa
# job B: cluster 5100002, job name run0-train_epoch1, run_id run-bbb

_SUBMIT_A = (
    "000 (5100001.000.000) 2026-05-01 09:00:00 Job submitted from host: <128.105.68.10:9618>\n"
    '    [ DAGNodeName = "run0-train_epoch0"; JobBatchName = "run0-train_epoch0" ]\n'
    "..."
)
_SUBMIT_B = (
    "000 (5100002.000.000) 2026-05-01 09:00:05 Job submitted from host: <128.105.68.10:9618>\n"
    '    [ DAGNodeName = "run0-train_epoch1"; JobBatchName = "run0-train_epoch1" ]\n'
    "..."
)
_XFER_IN_START_A = "040 (5100001.000.000) 2026-05-01 09:01:00 Started transferring input files"
_XFER_IN_DONE_A  = "040 (5100001.000.000) 2026-05-01 09:02:00 Finished transferring input files"
_EXECUTE_A       = "001 (5100001.000.000) 2026-05-01 09:02:30 Job executing on host: <10.0.1.5:1234>"
_EXECUTE_B       = "001 (5100002.000.000) 2026-05-01 09:02:45 Job executing on host: <10.0.1.6:1234>"
_XFER_OUT_START_A = "040 (5100001.000.000) 2026-05-01 10:30:00 Started transferring output files"
_XFER_OUT_DONE_A  = "040 (5100001.000.000) 2026-05-01 10:31:00 Finished transferring output files"
_COMPLETE_A      = "005 (5100001.000.000) 2026-05-01 10:32:00 Job terminated."
_EVICT_B         = "004 (5100002.000.000) 2026-05-01 10:00:00 Job was evicted."
_HOLD_B          = "012 (5100002.000.000) 2026-05-01 10:05:00 Job was held."
_RELEASE_B       = "013 (5100002.000.000) 2026-05-01 10:10:00 Job was released."


def _build_full_log() -> str:
    lines = [
        _SUBMIT_A,
        _SUBMIT_B,
        _XFER_IN_START_A,
        _XFER_IN_DONE_A,
        _EXECUTE_A,
        _EXECUTE_B,
        _XFER_OUT_START_A,
        _XFER_OUT_DONE_A,
        _COMPLETE_A,
        _EVICT_B,
        _HOLD_B,
        _RELEASE_B,
    ]
    return "\n".join(lines) + "\n"


def _setup_two_jobs(tmp_path: Path):
    ad_dir = tmp_path / "ads"
    ad_dir.mkdir()
    prov_dir = tmp_path / "provenance"
    _write_ndjson(prov_dir, "run-aaa", "run0-train_epoch0")
    _write_ndjson(prov_dir, "run-bbb", "run0-train_epoch1")
    return ad_dir, prov_dir


# --- full lifecycle: single-poll ---


def test_full_lifecycle_job_a_single_poll(tmp_path):
    """All events for job A processed in a single monitor_once call."""
    ad_dir, prov_dir = _setup_two_jobs(tmp_path)
    log = tmp_path / "metl.log"
    log.write_text(_build_full_log())

    monitor_once(log, 0, log_dir=ad_dir, provenance_log_dir=prov_dir)

    events_a = _read_events(prov_dir, "run-aaa")
    # job.submitted was written by _write_ndjson setup; monitor adds the rest
    types_a = [e["type"] for e in events_a]
    assert "job.queued" in types_a
    assert "transfer.input.started" in types_a
    assert "transfer.input.completed" in types_a
    assert "job.executing" in types_a
    assert "transfer.output.started" in types_a
    assert "transfer.output.completed" in types_a
    # 005 (job terminated) has no provenance event in _CODES
    assert "job.terminated" not in types_a


def test_full_lifecycle_job_b_single_poll(tmp_path):
    """All events for job B (submit, execute, evict, hold, release) in one poll."""
    ad_dir, prov_dir = _setup_two_jobs(tmp_path)
    log = tmp_path / "metl.log"
    log.write_text(_build_full_log())

    monitor_once(log, 0, log_dir=ad_dir, provenance_log_dir=prov_dir)

    events_b = _read_events(prov_dir, "run-bbb")
    types_b = [e["type"] for e in events_b]
    assert "job.queued" in types_b
    assert "job.executing" in types_b
    assert "job.migrated" in types_b
    assert "job.held" in types_b
    assert "job.released" in types_b


def test_jobs_do_not_cross_contaminate(tmp_path):
    """Events for job A are not attributed to run-bbb and vice versa."""
    ad_dir, prov_dir = _setup_two_jobs(tmp_path)
    log = tmp_path / "metl.log"
    log.write_text(_build_full_log())

    monitor_once(log, 0, log_dir=ad_dir, provenance_log_dir=prov_dir)

    events_a = _read_events(prov_dir, "run-aaa")
    events_b = _read_events(prov_dir, "run-bbb")

    assert all(e["run_id"] == "run-aaa" for e in events_a
               if e["type"] != "job.submitted")
    assert all(e["run_id"] == "run-bbb" for e in events_b
               if e["type"] != "job.submitted")

    # Transfer events belong only to A
    transfer_types = {"transfer.input.started", "transfer.input.completed",
                      "transfer.output.started", "transfer.output.completed"}
    assert not any(e["type"] in transfer_types for e in events_b)


def test_cluster_ids_correct_in_events(tmp_path):
    """cluster_id field on each emitted event matches the actual HTCondor cluster."""
    ad_dir, prov_dir = _setup_two_jobs(tmp_path)
    log = tmp_path / "metl.log"
    log.write_text(_build_full_log())

    monitor_once(log, 0, log_dir=ad_dir, provenance_log_dir=prov_dir)

    for e in _read_events(prov_dir, "run-aaa"):
        if "cluster_id" in e:
            assert e["cluster_id"] == 5100001, f"wrong cluster_id in {e}"
    for e in _read_events(prov_dir, "run-bbb"):
        if "cluster_id" in e:
            assert e["cluster_id"] == 5100002, f"wrong cluster_id in {e}"


# --- multi-poll: log grows across calls ---


def test_multi_poll_offset_advances(tmp_path):
    """Offset returned by first poll is used to start second poll at the right position."""
    ad_dir, prov_dir = _setup_two_jobs(tmp_path)
    log = tmp_path / "metl.log"

    chunk1 = _SUBMIT_A + "\n" + _EXECUTE_A + "\n"
    log.write_text(chunk1)

    offset = monitor_once(log, 0, log_dir=ad_dir, provenance_log_dir=prov_dir)
    assert offset == len(chunk1.encode())

    chunk2 = _EVICT_B + "\n"
    with open(log, "a") as f:
        f.write(chunk2)

    new_offset = monitor_once(log, offset, log_dir=ad_dir, provenance_log_dir=prov_dir)
    assert new_offset == len((chunk1 + chunk2).encode())


def test_multi_poll_events_emitted_only_once(tmp_path):
    """Processing the same log content twice (offset=0 both times) would double-emit;
    correct usage with advancing offset emits each event exactly once."""
    ad_dir, prov_dir = _setup_two_jobs(tmp_path)
    log = tmp_path / "metl.log"

    submit_block = _SUBMIT_A + "\n"
    execute_block = _EXECUTE_A + "\n"
    log.write_text(submit_block)

    cache: dict = {}
    state: dict = {"cluster_id": None}
    pending: dict = {}

    offset = monitor_once(log, 0, log_dir=ad_dir, provenance_log_dir=prov_dir,
                          run_id_cache=cache, multiline_state=state, pending_lookups=pending)

    with open(log, "a") as f:
        f.write(execute_block)

    monitor_once(log, offset, log_dir=ad_dir, provenance_log_dir=prov_dir,
                 run_id_cache=cache, multiline_state=state, pending_lookups=pending)

    events = _read_events(prov_dir, "run-aaa")
    executing = [e for e in events if e["type"] == "job.executing"]
    assert len(executing) == 1


def test_multi_poll_000_body_split_across_polls(tmp_path):
    """000 header in poll 1, DAGNodeName body line in poll 2 — cache resolves correctly."""
    ad_dir, prov_dir = _setup_two_jobs(tmp_path)
    log = tmp_path / "metl.log"

    header_only = "000 (5100001.000.000) 2026-05-01 09:00:00 Job submitted from host: <128.105.68.10:9618>\n"
    log.write_text(header_only)

    cache: dict = {}
    state: dict = {"cluster_id": None}
    pending: dict = {}

    offset = monitor_once(log, 0, log_dir=ad_dir, provenance_log_dir=prov_dir,
                          run_id_cache=cache, multiline_state=state, pending_lookups=pending)
    assert cache.get(5100001) is None
    assert state["cluster_id"] == 5100001

    body_and_sep = '    [ DAGNodeName = "run0-train_epoch0"; JobBatchName = "run0-train_epoch0" ]\n...\n'
    with open(log, "a") as f:
        f.write(body_and_sep)

    monitor_once(log, offset, log_dir=ad_dir, provenance_log_dir=prov_dir,
                 run_id_cache=cache, multiline_state=state, pending_lookups=pending)

    assert cache.get(5100001) == "run-aaa"
    events = _read_events(prov_dir, "run-aaa")
    assert any(e["type"] == "job.queued" for e in events)


def test_multi_poll_ndjson_written_after_submit(tmp_path):
    """000 seen before job.submitted NDJSON exists → pending; resolved next poll."""
    ad_dir = tmp_path / "ads"
    ad_dir.mkdir()
    prov_dir = tmp_path / "provenance"
    prov_dir.mkdir()
    log = tmp_path / "metl.log"
    log.write_text(_SUBMIT_A + "\n")

    cache: dict = {}
    state: dict = {"cluster_id": None}
    pending: dict = {}

    offset = monitor_once(log, 0, log_dir=ad_dir, provenance_log_dir=prov_dir,
                          run_id_cache=cache, multiline_state=state, pending_lookups=pending)

    assert pending.get(5100001) == "run0-train_epoch0"
    assert cache.get(5100001) is None

    _write_ndjson(prov_dir, "run-aaa", "run0-train_epoch0")
    monitor_once(log, offset, log_dir=ad_dir, provenance_log_dir=prov_dir,
                 run_id_cache=cache, multiline_state=state, pending_lookups=pending)

    assert cache.get(5100001) == "run-aaa"
    assert 5100001 not in pending
    events = _read_events(prov_dir, "run-aaa")
    assert any(e["type"] == "job.queued" for e in events)
