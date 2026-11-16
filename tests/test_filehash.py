import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from htprovenance import filehash

SCHEDD = "ap.example.edu"


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _classad_value(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return f'"{value}"'
    return str(value)


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """A job sandbox: cwd is tmp_path, and write_ad() points $_CONDOR_JOB_AD at a job ad."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("PROVENANCE_RUN_ID", raising=False)
    monkeypatch.delenv("PROVENANCE_LOG_DIR", raising=False)

    def write_ad(cluster_id: int = 100, proc_id: int = 0, **attrs) -> None:
        ad = {
            "ClusterId": cluster_id,
            "ProcId": proc_id,
            "GlobalJobId": f"{SCHEDD}#{cluster_id}.{proc_id}#1700000000",
            **attrs,
        }
        ad_path = tmp_path / ".job.ad"
        ad_path.write_text("".join(f"{k} = {_classad_value(v)}\n" for k, v in ad.items()))
        monkeypatch.setenv("_CONDOR_JOB_AD", str(ad_path))

    return write_ad


def _events(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()]


def _by_path(entries: list[dict]) -> dict[str, dict]:
    return {entry["path"]: entry for entry in entries}


# --- inputs ---


def test_inputs_from_job_ad_hashes_files_urls_and_directories(sandbox, tmp_path):
    sandbox(TransferInput="/home/u/params.txt, osdf:///chtc/staging/u/data.bin?x=1, /home/u/ref")
    (tmp_path / "params.txt").write_bytes(b"alpha")
    (tmp_path / "data.bin").write_bytes(b"staged bytes")
    (tmp_path / "ref").mkdir()
    (tmp_path / "ref" / "genome.fa").write_bytes(b"ACGT")

    entries = _by_path(filehash.record("inputs", []))

    assert entries["params.txt"]["sha256"] == _sha256(b"alpha")
    assert entries["params.txt"]["size"] == 5
    assert entries["params.txt"]["role"] == "input"
    assert entries["data.bin"]["sha256"] == _sha256(b"staged bytes")
    assert entries["data.bin"]["source"] == "osdf:///chtc/staging/u/data.bin?x=1"
    assert entries["ref/genome.fa"]["sha256"] == _sha256(b"ACGT")


def test_inputs_record_the_executable_under_its_sandbox_name(sandbox, tmp_path):
    sandbox(Cmd="/home/u/project/run.sh")
    (tmp_path / "run.sh").write_bytes(b"#!/bin/sh\n")

    entries = filehash.record("inputs", [])

    assert entries == [
        {
            "path": "run.sh",
            "role": "executable",
            "source": "/home/u/project/run.sh",
            "sha256": _sha256(b"#!/bin/sh\n"),
            "size": 10,
            "mtime_ns": (tmp_path / "run.sh").stat().st_mtime_ns,
            "status": "ok",
        }
    ]


def test_inputs_hash_an_untransferred_executable_where_it_is(sandbox, tmp_path):
    tool = tmp_path / "shared" / "tool"
    tool.parent.mkdir()
    tool.write_bytes(b"binary")
    sandbox(Cmd=str(tool), TransferExecutable=False)

    (entry,) = filehash.record("inputs", [])

    assert entry["path"] == str(tool)
    assert entry["sha256"] == _sha256(b"binary")


def test_container_image_is_recorded_by_reference_when_not_a_local_file(sandbox):
    sandbox(ContainerImage="docker://python:3.12-slim")

    (entry,) = filehash.record("inputs", [])

    assert entry == {
        "path": "docker://python:3.12-slim",
        "role": "container",
        "status": "reference",
    }


def test_container_image_file_in_the_sandbox_is_hashed(sandbox, tmp_path):
    sandbox(ContainerImage="/staging/u/env.sif")
    (tmp_path / "env.sif").write_bytes(b"sif image")

    (entry,) = filehash.record("inputs", [])

    assert entry["role"] == "container"
    assert entry["path"] == "env.sif"
    assert entry["sha256"] == _sha256(b"sif image")
    assert entry["source"] == "/staging/u/env.sif"


def test_missing_input_is_recorded_and_reported_on_stderr(sandbox, capsys):
    sandbox(TransferInput="/home/u/gone.txt")

    (entry,) = filehash.record("inputs", [])

    assert entry["status"] == "missing"
    assert "sha256" not in entry
    assert "gone.txt does not exist" in capsys.readouterr().err


@pytest.mark.skipif(os.geteuid() == 0, reason="root can read any file")
def test_unreadable_input_is_recorded_and_reported_on_stderr(sandbox, tmp_path, capsys):
    sandbox(TransferInput="secret.txt")
    secret = tmp_path / "secret.txt"
    secret.write_bytes(b"x")
    secret.chmod(0)

    (entry,) = filehash.record("inputs", [])

    assert entry["status"] == "unreadable"
    assert "sha256" not in entry
    assert "error" in entry
    assert "cannot read input file secret.txt" in capsys.readouterr().err


def test_job_ad_without_transfer_lists_records_no_files(sandbox):
    sandbox()

    assert filehash.record("inputs", []) == []
    assert filehash.record("outputs", []) == []


def test_directory_contents_transfer_is_recorded_as_unresolved(sandbox, capsys):
    sandbox(TransferInput="/home/u/inputs/")

    (entry,) = filehash.record("inputs", [])

    assert entry == {"path": "/home/u/inputs/", "role": "input", "status": "unresolved"}
    assert "pass them as paths" in capsys.readouterr().err


def test_explicit_input_paths_replace_the_job_ad_list_but_keep_the_executable(sandbox, tmp_path):
    sandbox(TransferInput="listed.txt", Cmd="run.sh")
    (tmp_path / "listed.txt").write_bytes(b"listed")
    (tmp_path / "extra.txt").write_bytes(b"extra")
    (tmp_path / "run.sh").write_bytes(b"#!/bin/sh\n")

    entries = _by_path(filehash.record("inputs", ["extra.txt"]))

    assert set(entries) == {"extra.txt", "run.sh"}
    assert entries["extra.txt"]["role"] == "input"


# --- outputs ---


def test_outputs_from_job_ad_include_directory_contents(sandbox, tmp_path):
    sandbox(TransferOutput="result.csv,plots")
    (tmp_path / "result.csv").write_bytes(b"a,b\n")
    (tmp_path / "plots").mkdir()
    (tmp_path / "plots" / "fig.png").write_bytes(b"png")
    (tmp_path / "scratch.tmp").write_bytes(b"not transferred")

    entries = _by_path(filehash.record("outputs", []))

    assert set(entries) == {"result.csv", "plots/fig.png"}
    assert entries["result.csv"]["sha256"] == _sha256(b"a,b\n")
    assert entries["result.csv"]["role"] == "output"


def test_remapped_output_records_its_destination(sandbox, tmp_path):
    sandbox(
        TransferOutput="model.pt,log.txt",
        TransferOutputRemaps="model.pt=file:///staging/u/model-7.pt",
    )
    (tmp_path / "model.pt").write_bytes(b"weights")
    (tmp_path / "log.txt").write_bytes(b"log")

    entries = _by_path(filehash.record("outputs", []))

    assert entries["model.pt"]["destination_url"] == "file:///staging/u/model-7.pt"
    assert entries["model.pt"]["sha256"] == _sha256(b"weights")
    assert "destination_url" not in entries["log.txt"]


def test_output_destination_applies_to_outputs_without_a_remap(sandbox, tmp_path):
    sandbox(TransferOutput="a.txt", OutputDestination="osdf:///chtc/staging/u/run1/")
    (tmp_path / "a.txt").write_bytes(b"a")

    (entry,) = filehash.record("outputs", [])

    assert entry["destination_url"] == "osdf:///chtc/staging/u/run1/a.txt"


def test_default_output_transfer_records_new_and_changed_top_level_files(sandbox, tmp_path):
    sandbox(TransferInput="unchanged.txt,edited.txt", Cmd="run.sh")
    (tmp_path / "unchanged.txt").write_bytes(b"same")
    (tmp_path / "edited.txt").write_bytes(b"before")
    (tmp_path / "run.sh").write_bytes(b"#!/bin/sh\n")
    filehash.record("inputs", [])

    (tmp_path / "edited.txt").write_bytes(b"after, longer")
    (tmp_path / "new.txt").write_bytes(b"new")
    (tmp_path / "_condor_stdout").write_bytes(b"stdout")
    (tmp_path / ".hidden").write_bytes(b"hidden")
    (tmp_path / "subdir").mkdir()
    (tmp_path / "subdir" / "nested.txt").write_bytes(b"not returned by default")

    entries = _by_path(filehash.record("outputs", []))

    assert set(entries) == {"edited.txt", "new.txt"}
    assert entries["edited.txt"]["sha256"] == _sha256(b"after, longer")


def test_missing_output_is_recorded_and_reported_on_stderr(sandbox, capsys):
    sandbox(TransferOutput="never_written.txt")

    (entry,) = filehash.record("outputs", [])

    assert entry["status"] == "missing"
    assert "never_written.txt does not exist" in capsys.readouterr().err


# --- the event ---


def test_events_go_to_the_per_job_file_with_the_job_key(sandbox, tmp_path):
    sandbox(cluster_id=42, proc_id=3, TransferInput="in.txt", TransferOutput="out.txt")
    (tmp_path / "in.txt").write_bytes(b"in")
    (tmp_path / "out.txt").write_bytes(b"out")

    filehash.record("inputs", [])
    filehash.record("outputs", [])

    inputs, outputs = _events(tmp_path / "cluster-42.42.3.ndjson")
    assert inputs["type"] == "files.inputs"
    assert outputs["type"] == "files.outputs"
    for event in (inputs, outputs):
        assert event["run_id"] == "cluster-42"
        assert (event["schedd"], event["cluster_id"], event["proc_id"]) == (SCHEDD, 42, 3)
    assert outputs["files"][0]["sha256"] == _sha256(b"out")


def test_procs_of_one_cluster_write_separate_files(sandbox, tmp_path):
    (tmp_path / "in.txt").write_bytes(b"in")
    for proc_id in (0, 1):
        sandbox(cluster_id=7, proc_id=proc_id, TransferInput="in.txt")
        filehash.record("inputs", [])

    for proc_id in (0, 1):
        (event,) = _events(tmp_path / f"cluster-7.7.{proc_id}.ndjson")
        assert event["proc_id"] == proc_id


def test_provenance_run_id_job_writes_under_output_provenance_and_skips_its_own_log(
    sandbox, tmp_path, monkeypatch
):
    monkeypatch.setenv("PROVENANCE_RUN_ID", "run-abc")
    sandbox(cluster_id=9, TransferOutput="output")
    (tmp_path / "output").mkdir()
    (tmp_path / "output" / "model.pt").write_bytes(b"weights")
    filehash.record("inputs", [])

    entries = filehash.record("outputs", [])

    assert [entry["path"] for entry in entries] == ["output/model.pt"]
    assert len(_events(tmp_path / "output" / "provenance" / "run-abc.9.0.ndjson")) == 2


# --- main() never affects the job ---


def test_main_outside_a_job_records_nothing_and_returns(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("_CONDOR_JOB_AD", raising=False)
    monkeypatch.setattr(sys, "argv", ["htprovenance-hash", "inputs"])

    filehash.main()

    assert "not inside an HTCondor job" in capsys.readouterr().err
    assert list(tmp_path.iterdir()) == []


def test_main_returns_on_a_usage_error(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["htprovenance-hash", "sideways"])

    filehash.main()

    assert "invalid choice" in capsys.readouterr().err


def test_main_returns_when_recording_fails(sandbox, monkeypatch, capsys):
    sandbox()
    monkeypatch.setattr(sys, "argv", ["htprovenance-hash", "inputs"])

    def explode(kind, paths):
        raise RuntimeError("disk on fire")

    monkeypatch.setattr(filehash, "record", explode)

    filehash.main()

    assert "nothing recorded" in capsys.readouterr().err


def test_main_reports_how_many_files_were_hashed(sandbox, tmp_path, monkeypatch, capsys):
    sandbox(TransferInput="here.txt,gone.txt")
    (tmp_path / "here.txt").write_bytes(b"here")
    monkeypatch.setattr(sys, "argv", ["htprovenance-hash", "inputs"])

    filehash.main()

    assert "recorded 1 of 2 inputs file(s)" in capsys.readouterr().out


def test_runs_without_third_party_packages(sandbox, tmp_path):
    """A job without htprovenance installed runs the module from the wheel: stdlib only."""
    sandbox(TransferInput="in.txt")
    (tmp_path / "in.txt").write_bytes(b"in")
    block_third_party = (
        "import sys\n"
        "class Block:\n"
        "    def find_spec(self, name, path=None, target=None):\n"
        "        if name.split('.')[0] in ('yaml', 'typer', 'htcondor', 'htcondor2'):\n"
        "            raise ImportError(name + ' is not available in this job')\n"
        "sys.meta_path.insert(0, Block())\n"
        "sys.argv = ['htprovenance-hash', 'inputs']\n"
        "from htprovenance.filehash import main\n"
        "main()\n"
    )
    env = {
        **os.environ,
        "HTPROVENANCE_NO_AUTORUN": "1",
        "PYTHONPATH": str(Path(filehash.__file__).parent.parent),
    }

    result = subprocess.run(
        [sys.executable, "-c", block_third_party],
        cwd=tmp_path, env=env, capture_output=True, text=True, check=True,
    )

    assert "recorded 1 of 1 inputs file(s)" in result.stdout
    assert (tmp_path / "cluster-100.100.0.ndjson").exists()
