import json
import logging
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest
from typer.testing import CliRunner

from htprovenance import autocapture, filehash, packages
from htprovenance.db import build_database
from htprovenance.query import app

runner = CliRunner()

AP1 = "ap1.example.edu"
AP2 = "ap2.example.edu"
BASE = [("numpy", "2.1.0"), ("pandas", "2.2.3"), ("pyyaml", "6.0.2")]
UPGRADED = [("numpy", "2.2.0"), ("pyyaml", "6.0.2"), ("scipy", "1.14.1")]


class _Dist:
    def __init__(self, name, version):
        self.metadata = {"Name": name}
        self.version = version


@pytest.fixture
def run_job(tmp_path, monkeypatch):
    """Run autocapture as one job would; its provenance lands in tmp_path/returned."""
    returned = tmp_path / "returned"
    monkeypatch.delenv("PROVENANCE_RUN_ID", raising=False)
    monkeypatch.setenv("PROVENANCE_LOG_DIR", str(returned))
    monkeypatch.setattr(autocapture, "_run", lambda cmd: "node01")
    monkeypatch.setattr(
        autocapture,
        "default_gpu_info",
        lambda: ({"gpu_count": 0, "gpu_model": "none", "gpu_id": "none"}, "none"),
    )

    def run(cluster_id: int, installed: list | None, *, schedd: str = AP1, proc_id: int = 0):
        sandbox = tmp_path / f"sandbox-{schedd}-{cluster_id}-{proc_id}"
        sandbox.mkdir()
        ad = sandbox / ".job.ad"
        ad.write_text(
            f"ClusterId = {cluster_id}\nProcId = {proc_id}\nJobUniverse = 5\n"
            f'GlobalJobId = "{schedd}#{cluster_id}.{proc_id}#1700000000"\n'
        )
        monkeypatch.chdir(sandbox)
        monkeypatch.setenv("_CONDOR_JOB_AD", str(ad))
        if installed is None:

            def broken():
                raise OSError("site-packages is unreadable")

            monkeypatch.setattr(packages, "installed_packages", broken)
        else:
            monkeypatch.setattr(packages, "installed_packages", lambda: installed)
        return autocapture.capture_and_emit()

    run.returned = returned
    return run


def _list_files(directory: Path) -> list[Path]:
    return sorted(directory.glob("packages.*.json"))


def _build(run_job, tmp_path) -> Path:
    db = tmp_path / "provenance.db"
    build_database(db, [], [run_job.returned])
    return db


def _package_rows(db: Path) -> list[tuple]:
    with closing(sqlite3.connect(db)) as conn:
        return conn.execute(
            "SELECT set_hash, name, version FROM package_sets ORDER BY set_hash, name"
        ).fetchall()


# --- listing ---


def test_installed_packages_normalizes_names_and_keeps_the_first_copy(monkeypatch):
    dists = [
        _Dist("PyYAML", "6.0.2"),
        _Dist("typing_extensions", "4.12.2"),
        _Dist("pyyaml", "5.1"),  # shadowed copy later on sys.path
        _Dist(None, "1.0"),  # broken metadata
        _Dist("zope.interface", None),
    ]
    monkeypatch.setattr(packages.metadata, "distributions", lambda: iter(dists))

    assert packages.installed_packages() == [
        ("pyyaml", "6.0.2"),
        ("typing-extensions", "4.12.2"),
        ("zope-interface", "unknown"),
    ]


def test_installed_packages_lists_this_environment():
    names = [name for name, _ in packages.installed_packages()]

    assert "pytest" in names
    assert names == sorted(names)


def test_set_hash_ignores_order_and_changes_with_a_version():
    assert packages.package_set_hash(BASE) == packages.package_set_hash(BASE[::-1])
    assert packages.package_set_hash(BASE) != packages.package_set_hash(UPGRADED)
    assert packages.package_set_hash([]) != packages.package_set_hash(BASE)


# --- capture ---


def test_job_assigned_carries_the_set_hash_and_the_list_is_a_separate_file(run_job):
    payload = run_job(100, BASE)

    assert payload["package_count"] == 3
    assert payload["package_set_hash"] == packages.package_set_hash(BASE)
    (list_file,) = _list_files(run_job.returned)
    assert list_file.name == f"packages.{payload['package_set_hash']}.json"
    assert packages.read_package_list(list_file) == (payload["package_set_hash"], BASE)
    (event,) = [
        json.loads(line)
        for line in (run_job.returned / "cluster-100.100.0.ndjson").read_text().splitlines()
    ]
    assert event["package_set_hash"] == payload["package_set_hash"]
    assert "packages" not in event


def test_jobs_sharing_an_environment_return_one_identical_list_file(run_job):
    run_job(100, BASE)
    (list_file,) = _list_files(run_job.returned)
    first_content = list_file.read_bytes()

    run_job(101, BASE[::-1])

    assert _list_files(run_job.returned) == [list_file]
    assert list_file.read_bytes() == first_content


def test_a_different_environment_gets_its_own_list_file(run_job):
    first = run_job(100, BASE)
    second = run_job(101, UPGRADED)

    assert first["package_set_hash"] != second["package_set_hash"]
    assert len(_list_files(run_job.returned)) == 2


def test_failing_to_list_packages_leaves_job_assigned_without_package_fields(run_job, capsys):
    payload = run_job(100, None)

    assert payload["hostname"] == "node01"
    assert "package_set_hash" not in payload
    assert "package_count" not in payload
    assert _list_files(run_job.returned) == []
    assert (run_job.returned / "cluster-100.100.0.ndjson").exists()
    captured = capsys.readouterr()
    assert (captured.out, captured.err) == ("", "")


def test_file_hashing_does_not_report_the_list_file_as_an_output(run_job, monkeypatch):
    monkeypatch.delenv("PROVENANCE_LOG_DIR")  # provenance at the top of the sandbox
    run_job(100, BASE)
    Path("result.csv").write_text("result")
    assert len(_list_files(Path())) == 1

    entries = filehash.record("outputs", [])

    assert [entry["path"] for entry in entries] == ["result.csv"]


# --- db build ---


def test_build_stores_each_set_once_however_many_jobs_share_it(run_job, tmp_path):
    for cluster_id in (100, 101, 102):
        run_job(cluster_id, BASE)
    run_job(103, UPGRADED)

    rows = _package_rows(_build(run_job, tmp_path))

    assert len(rows) == len(BASE) + len(UPGRADED)
    assert {row[0] for row in rows} == {
        packages.package_set_hash(BASE),
        packages.package_set_hash(UPGRADED),
    }


def test_rebuilding_does_not_duplicate_or_recount_package_sets(run_job, tmp_path):
    run_job(100, BASE)
    db = tmp_path / "provenance.db"

    first = build_database(db, [], [run_job.returned])
    again = build_database(db, [], [run_job.returned], full_rescan=True)

    assert first.package_sets_ingested == 1
    assert again.package_sets_ingested == 0
    assert len(_package_rows(db)) == len(BASE)


@pytest.mark.parametrize(
    "damage",
    [
        lambda text: text.replace("2.1.0", "9.9.9"),  # edited after the fact
        lambda text: text[: len(text) // 2],  # truncated in transfer
        lambda text: "[]",  # not a package list
    ],
)
def test_list_file_not_matching_its_name_is_skipped_with_a_warning(
    run_job, tmp_path, caplog, damage
):
    run_job(100, BASE)
    (list_file,) = _list_files(run_job.returned)
    list_file.write_text(damage(list_file.read_text()))

    with caplog.at_level(logging.WARNING):
        stats = build_database(tmp_path / "provenance.db", [], [run_job.returned])

    assert stats.package_sets_skipped_malformed == 1
    assert stats.package_sets_ingested == 0
    assert _package_rows(tmp_path / "provenance.db") == []
    assert list_file.name in caplog.text


# --- queries ---


def test_packages_show_lists_a_jobs_packages(run_job, tmp_path):
    run_job(100, BASE)
    db = _build(run_job, tmp_path)

    result = runner.invoke(app, ["packages", "show", "100.0", "--db", str(db)])

    assert result.exit_code == 0, result.output
    assert f"{AP1}#100.0  3 package(s)" in result.output
    assert "  numpy==2.1.0\n" in result.output
    assert "  pyyaml==6.0.2\n" in result.output


def test_packages_show_defaults_to_proc_zero_and_emits_json(run_job, tmp_path):
    run_job(100, BASE)
    db = _build(run_job, tmp_path)

    result = runner.invoke(app, ["packages", "show", "100", "--db", str(db), "--json"])

    assert result.exit_code == 0, result.output
    record = json.loads(result.output)
    assert record["job"] == f"{AP1}#100.0"
    assert record["package_set_hash"] == packages.package_set_hash(BASE)
    assert record["packages"] == dict(BASE)


def test_packages_diff_reports_changed_added_and_removed(run_job, tmp_path):
    run_job(100, BASE)
    run_job(101, UPGRADED)
    db = _build(run_job, tmp_path)

    result = runner.invoke(app, ["packages", "diff", "100.0", "101.0", "--db", str(db)])

    assert result.exit_code == 0, result.output
    assert "changed  numpy  2.1.0 -> 2.2.0" in result.output
    assert "added    scipy==1.14.1" in result.output
    assert "removed  pandas==2.2.3" in result.output
    assert "pyyaml" not in result.output


def test_packages_diff_json(run_job, tmp_path):
    run_job(100, BASE)
    run_job(101, UPGRADED)
    db = _build(run_job, tmp_path)

    result = runner.invoke(app, ["packages", "diff", "100.0", "101.0", "--db", str(db), "--json"])

    assert json.loads(result.output) == {
        "first": f"{AP1}#100.0",
        "second": f"{AP1}#101.0",
        "identical": False,
        "added": {"scipy": "1.14.1"},
        "removed": {"pandas": "2.2.3"},
        "changed": {"numpy": {"from": "2.1.0", "to": "2.2.0"}},
    }


def test_packages_diff_says_when_two_jobs_ran_the_same_set(run_job, tmp_path):
    run_job(100, BASE)
    run_job(101, BASE)
    db = _build(run_job, tmp_path)

    result = runner.invoke(app, ["packages", "diff", "100.0", "101.0", "--db", str(db)])

    assert result.exit_code == 0, result.output
    assert "ran with the same 3 package(s)" in result.output


def test_same_cluster_on_two_schedds_needs_the_schedd_named(run_job, tmp_path):
    run_job(100, BASE, schedd=AP1)
    run_job(100, UPGRADED, schedd=AP2)
    db = _build(run_job, tmp_path)

    ambiguous = runner.invoke(app, ["packages", "show", "100.0", "--db", str(db)])
    named = runner.invoke(app, ["packages", "show", f"{AP2}#100.0", "--db", str(db)])

    assert ambiguous.exit_code == 1
    assert "recorded on 2 schedds" in ambiguous.output
    assert f"{AP1}#100.0" in ambiguous.output
    assert named.exit_code == 0, named.output
    assert "scipy==1.14.1" in named.output


def test_job_whose_list_file_was_not_returned_is_a_clear_error(run_job, tmp_path):
    run_job(100, BASE)
    (list_file,) = _list_files(run_job.returned)
    list_file.unlink()
    db = _build(run_job, tmp_path)

    result = runner.invoke(app, ["packages", "show", "100.0", "--db", str(db)])

    assert result.exit_code == 1
    assert list_file.name in result.output
    assert "was not in the event directories" in result.output


def test_job_that_recorded_no_packages_is_a_clear_error(run_job, tmp_path):
    run_job(100, None)
    db = _build(run_job, tmp_path)

    result = runner.invoke(app, ["packages", "show", "100.0", "--db", str(db)])

    assert result.exit_code == 1
    assert "did not record its packages" in result.output


def test_job_with_an_empty_environment_lists_no_packages(run_job, tmp_path):
    run_job(100, [])
    db = _build(run_job, tmp_path)

    result = runner.invoke(app, ["packages", "show", "100.0", "--db", str(db)])

    assert result.exit_code == 0, result.output
    assert "0 package(s)" in result.output


@pytest.mark.parametrize(
    ("spec", "message"),
    [("999.0", "has no job.assigned event"), ("not-a-job", "is not <cluster>.<proc>")],
)
def test_unknown_or_malformed_job_is_a_clear_error(run_job, tmp_path, spec, message):
    run_job(100, BASE)
    db = _build(run_job, tmp_path)

    result = runner.invoke(app, ["packages", "show", spec, "--db", str(db)])

    assert result.exit_code == 1
    assert message in result.output


def test_files_software_shows_the_package_count_and_short_set_hash(
    run_job, tmp_path, monkeypatch
):
    monkeypatch.delenv("PROVENANCE_LOG_DIR")
    payload = run_job(100, BASE)
    Path("result.csv").write_text("result")
    filehash.record("outputs", [])
    db = tmp_path / "provenance.db"
    build_database(db, [], [Path.cwd()])

    result = runner.invoke(app, ["files", "software", "result.csv", "--db", str(db)])

    assert result.exit_code == 0, result.output
    assert f"packages=3 (set {payload['package_set_hash'][:12]})" in result.output
    assert payload["package_set_hash"] not in result.output.split("environment")[1]
