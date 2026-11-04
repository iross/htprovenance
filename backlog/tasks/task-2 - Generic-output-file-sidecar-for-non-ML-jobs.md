---
id: TASK-2
title: In-job file hashing and lineage queries
status: To Do
assignee: []
created_date: '2026-11-04 20:40'
updated_date: '2026-11-04 22:06'
labels: []
dependencies:
  - TASK-1
ordinal: 2000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Goal: answer, from provenance.db, for regular (non-DAG) jobs:
- What software (executable, container, code commit) produced this output file?
- What outputs came from all jobs that used this input?
- What chain of jobs connects input A to output C?

HTCondor history/adstash record filenames (TransferInput/TransferOutput, Cmd, ContainerImage) but not file contents, so reused or overwritten names can't anchor lineage. The job's sandbox is the only place that sees the actual bytes when they are used, including staged inputs (already downloaded) and outputs about to be remapped to /staging.

## Capture

A command the job script calls itself, once after inputs arrive and once before it exits:

  htprovenance-hash inputs [PATH...]
  htprovenance-hash outputs [PATH...]

- With no paths, use the job ad's TransferInput / TransferOutput (resolved to sandbox basenames; osdf:// and file:// entries map to the downloaded file). Explicit paths add to or replace that list.
- The executable (Cmd) is hashed as role=executable on the inputs call. ContainerImage / SingularityImage from the job ad is recorded as role=container (hashed if the .sif is in the sandbox, otherwise the image reference as given; tags are mutable, note this).
- For outputs with a transfer_output_remaps entry, record the destination URL alongside the hash.
- Emits files.inputs / files.outputs events (sha256, path, size, role, destination URL if any) into the job's per-proc NDJSON from TASK-1, with the same run_id, cluster_id, proc_id and schedd.
- Best-effort like autocapture: never changes the job's exit code. Unreadable or missing files are recorded as such and reported on stderr.

Open decision: jobs without htprovenance installed (non-Python jobs, or Python envs without the package). Either require it, or ship a standalone stdlib-only script the job transfers in and runs with python3. Avoid a second implementation in shell.

HTCondor +PreCmd/+PostCmd would make this zero-touch, but they were silently ignored on ap2002 (2026-11-04 test); pending an answer from the HTCondor developers. Not in scope here.

## Storage and queries

db build ingests files.* events into a file_edges table: (schedd, cluster_id, proc_id, role, path) -> sha256, size, destination_url, run_id. Re-ingesting is idempotent.

Query commands, by file path or hash:
- software for an output: producing job, its executable/container edges, and its job.assigned environment (code_commit, python, cuda) when present
- outputs downstream of an input: jobs that read the hash, then their outputs
- chain between two files: walk output-hash -> consuming job -> its outputs
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 htprovenance-hash inputs/outputs emits files.* events with sha256, size, role and path into the job's per-proc NDJSON
- [ ] #2 With no paths given, file lists come from the job ad's TransferInput/TransferOutput; executable and container edges are recorded
- [ ] #3 Remapped outputs record their destination URL alongside the hash
- [ ] #4 The command never changes the job's exit code; missing/unreadable files are recorded and reported on stderr
- [ ] #5 Decision on jobs without htprovenance installed is made and recorded in this task
- [ ] #6 db build ingests files.* events into file_edges keyed by (schedd, cluster_id, proc_id, role, path); re-ingest is idempotent
- [ ] #7 Query commands answer: software for an output, outputs downstream of an input, job chain between two files
- [ ] #8 Tests cover: missing and unreadable files, empty transfer lists, queue N procs, remapped outputs, same cluster_id on two schedds, chains of 3+ jobs
- [ ] #9 Hashing time measured on ap2001/ap2002 for a multi-GB staged input and documented in the README
- [ ] #10 README documents the two lines a job script adds and the three queries
<!-- AC:END -->
