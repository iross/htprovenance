"""Installed Python packages of a job's environment, stored once per distinct set.

Many jobs share one environment, so a job records only a hash of its
package set (in its job.assigned event, see htprovenance.autocapture) and
writes the full list to a separate file named by that hash:

    packages.<set_hash>.json

The file's content depends only on the set, so jobs with the same
environment return identical files that replace each other, and `db build`
stores each set once in the package_sets table.

The set is the environment of the Python interpreter that wrote
job.assigned, i.e. the first one the job started. Capture uses only the
standard library.

CLI: `htprovenance-query packages show|diff`.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from importlib import metadata
from pathlib import Path

LIST_FILE_GLOB = "packages.*.json"
_LIST_FILE_NAME = re.compile(r"packages\.([0-9a-f]{64})\.json")
_LIST_SCHEMA_VERSION = 1
_NAME_SEPARATORS = re.compile(r"[-_.]+")

Packages = list[tuple[str, str]]


def installed_packages() -> Packages:
    """Return (name, version) of every distribution installed for this interpreter.

    Names are normalized as package indexes do (lowercase, runs of "-", "_"
    and "." become "-"), and sorted. When a name is installed in more than
    one place the first on sys.path is kept: that is the one imported.
    """
    found: dict[str, str] = {}
    for dist in metadata.distributions():
        name = dist.metadata["Name"]
        if not name:
            continue
        found.setdefault(_NAME_SEPARATORS.sub("-", name).lower(), dist.version or "unknown")
    return sorted(found.items())


def package_set_hash(packages: Packages) -> str:
    """Return the SHA-256 identifying a package set: its sorted name==version lines."""
    lines = "".join(f"{name}=={version}\n" for name, version in sorted(packages))
    return hashlib.sha256(lines.encode()).hexdigest()


def is_list_file(path: Path) -> bool:
    """Return True if path is named like a package list file."""
    return _LIST_FILE_NAME.fullmatch(path.name) is not None


def write_package_list(log_dir: str | Path) -> tuple[str, int]:
    """Write this interpreter's package list into log_dir.

    Returns (set_hash, package_count).
    """
    packages = sorted(installed_packages())
    set_hash = package_set_hash(packages)
    document = {
        "schema_version": _LIST_SCHEMA_VERSION,
        "set_hash": set_hash,
        "packages": [{"name": name, "version": version} for name, version in packages],
    }
    directory = Path(log_dir)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"packages.{set_hash}.json").write_text(json.dumps(document, indent=1) + "\n")
    return set_hash, len(packages)


def read_package_list(path: Path) -> tuple[str, Packages]:
    """Return (set_hash, packages) from a package list file.

    Raises:
        ValueError: if the file is not a package list, or its packages do
            not hash to the set_hash in its name (truncated or edited).
    """
    match = _LIST_FILE_NAME.fullmatch(path.name)
    if match is None:
        raise ValueError(f"{path.name} is not named packages.<sha256>.json")
    try:
        document = json.loads(path.read_text())
        packages = [(str(p["name"]), str(p["version"])) for p in document["packages"]]
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise ValueError(f"cannot read a package list from it: {exc!r}") from exc
    if package_set_hash(packages) != match.group(1):
        raise ValueError("its packages do not match the hash in its name")
    return match.group(1), packages


def job_package_set(conn: sqlite3.Connection, job: tuple[str, int, int]) -> tuple[str, int]:
    """Return (set_hash, package_count) from the job's latest job.assigned event.

    Raises:
        LookupError: if the job has no job.assigned event, or the event
            predates package recording.
    """
    row = conn.execute(
        "SELECT json_extract(payload_json, '$.package_set_hash'), "
        "json_extract(payload_json, '$.package_count') FROM events "
        "WHERE type = 'job.assigned' AND json_extract(payload_json, '$.schedd') = ? "
        "AND json_extract(payload_json, '$.cluster_id') = ? "
        "AND json_extract(payload_json, '$.proc_id') = ? ORDER BY ts DESC LIMIT 1",
        job,
    ).fetchone()
    label = f"{job[0]}#{job[1]}.{job[2]}"
    if row is None:
        raise LookupError(f"no job.assigned event is recorded for {label}")
    if row[0] is None:
        raise LookupError(f"{label} did not record its packages")
    return row[0], row[1] or 0


def packages_in_set(conn: sqlite3.Connection, set_hash: str, package_count: int) -> dict[str, str]:
    """Return {name: version} for a package set.

    Raises:
        LookupError: if the set's list file was never ingested.
    """
    rows = conn.execute(
        "SELECT name, version FROM package_sets WHERE set_hash = ? ORDER BY name", (set_hash,)
    ).fetchall()
    if not rows and package_count:
        raise LookupError(
            f"package set {set_hash} is not in the database: packages.{set_hash}.json "
            "was not in the event directories given to `db build`"
        )
    return dict(rows)


def diff_package_sets(first: dict[str, str], second: dict[str, str]) -> dict:
    """Return what changed from first to second: added, removed, changed versions."""
    shared = first.keys() & second.keys()
    return {
        "added": {name: second[name] for name in sorted(second.keys() - first.keys())},
        "removed": {name: first[name] for name in sorted(first.keys() - second.keys())},
        "changed": {
            name: {"from": first[name], "to": second[name]}
            for name in sorted(shared)
            if first[name] != second[name]
        },
    }
