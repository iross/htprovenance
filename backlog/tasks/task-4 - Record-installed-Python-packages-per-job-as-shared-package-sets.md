---
id: TASK-4
title: Record installed Python packages per job as shared package sets
status: Done
assignee: []
created_date: '2026-11-16 19:28'
updated_date: '2026-11-16 20:37'
labels: []
dependencies:
  - TASK-2
ordinal: 4000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Goal: say which Python package versions a job ran with, and what differs between two jobs' environments, without storing the same list once per job.

Background (2026-11-16 discussion): a git commit is not a reliable software identifier, because images built with COPY from a working directory or with a build image carry no git metadata. The container image digest will come from the job ad (an HTCondor developer is adding it; not in scope here). Hashing loaded code files was considered and dropped as not useful to a user.

## Capture

In the startup hook, the job's first Python process lists the installed distributions (name, version) of the environment it runs in.

- The job's job.assigned event records only the set hash (SHA-256 over the sorted name==version lines) and the package count.
- The full list goes to a separate file, packages.<set_hash>.json, next to the job's NDJSON. Its content depends only on the set, so jobs sharing an environment return identical files that overwrite each other.
- Best-effort like the rest of autocapture; the list file is never reported as a job output by file hashing.

## Storage and queries

- db build reads packages.*.json from the event dirs into package_sets (set_hash, name, version), one copy per distinct set.
- files software shows the package count and set hash.
- New commands: list a job's packages; show the differences between two jobs' package sets.

Limit: records the environment of the first Python interpreter the job starts.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 job.assigned for an execute-node job carries package_set_hash and package_count; the full list is written once per set as packages.<set_hash>.json beside the job's NDJSON
- [x] #2 Two jobs with the same installed packages produce byte-identical list files and the same set hash; a changed version changes the hash
- [x] #3 Listing packages never raises into the job and prints nothing; a failure leaves job.assigned intact without the package fields
- [x] #4 File hashing does not report the package list file as a job output
- [x] #5 db build ingests packages.*.json into package_sets once per set; a file whose content does not match its name's hash is skipped with a warning; re-ingest is idempotent
- [x] #6 files software shows package count and set hash; a command lists a job's packages; a command shows added, removed and changed packages between two jobs, and says so when the sets are identical
- [x] #7 Queries give a clear error when a job has no recorded package set or its list file was not returned
- [x] #8 README documents what is recorded, the list file, and the two commands
- [x] #9 Verified in a real HTCondor job
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Implemented 2026-11-16 on branch feat/package-sets.

- htprovenance/packages.py (stdlib only): installed_packages() (names normalized, first copy on sys.path kept), package_set_hash(), write_package_list() / read_package_list(), and the db-side lookups job_package_set(), packages_in_set(), diff_package_sets().
- autocapture: execute-node job.assigned gains package_set_hash and package_count; the list is written as packages.<set_hash>.json in the same directory as the job's NDJSON. No new event type. DAGMan/SERVICE captures on the AP are unchanged.
- filehash: output hashing skips files named packages.<sha256>.json.
- db: package_sets(set_hash, name, version) with PK (set_hash, name); db build reads packages.*.json from the event dirs, verifies the content against the hash in the file name, and skips sets already stored. BuildStats and its summary line gained package-set counts.
- query: `htprovenance-query packages show JOB` and `packages diff JOB_A JOB_B` (--db, --json); JOB is <cluster>[.<proc>] or <schedd>#<cluster>.<proc>. `files software` shows packages=<count> (set <first 12 chars>).

Choices made: the per-job value lives in the job.assigned payload (read with json_extract), not a new column or table. The watcher passes the two new fields through into checkpoint sidecars' environment block, since it copies every non-site key of job.assigned. A job with explicit transfer_output_files must return the list file itself, as with its NDJSON.

Measured locally: 13 packages = 835 bytes, about 64 bytes per package.

Tests: tests/test_packages.py (25). Existing tests that listed every file in the provenance directory now list *.ndjson. Full suite 445 passed, 1 skipped. ap-check kit: auto node checks added (package set recorded, list file returned and ingested, not listed as an output); dry-run locally. AC #9 open until the kit runs on an AP.

Real-job verification (2026-11-16, ap2002, user-run ap-check kit): all 22 checks passed. The `auto` job (8763579.0 on e4046, `docker://python:3.12-slim`, wheel pip-installed into a venv) recorded a 16-package set in job.assigned; `packages.cdc3d719af54….json` came back by default output transfer, was ingested by `db build`, and was not listed as a job output. Packages recorded: htprovenance 0.1.0rc2 and its dependencies (htcondor 26.1.0, pydantic 2.14.3, typer 0.29.1, rich 15.0.0, pyyaml 6.0.3, …) plus pip 25.0.1. Third timing: 2 GB hashed in 5.1 s (405 MB/s) on e4007.
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Jobs record the installed Python packages of their first Python interpreter. `job.assigned` carries `package_set_hash` and `package_count`; the list is written once per distinct set to `packages.<set_hash>.json` beside the job's NDJSON. `db build` stores each set once in `package_sets` and skips list files whose content does not match the hash in their name. `htprovenance-query packages show|diff` lists and compares jobs' sets, and `files software` prints the count and short set hash.

Tests: `tests/test_packages.py` (25 tests); full suite 445 passed, 1 skipped. Verified in a real container job on ap2002 (8763579.0).

Limits: a job with explicit `transfer_output_files` must return the list file itself; only the first Python environment a job starts is recorded.
<!-- SECTION:FINAL_SUMMARY:END -->
