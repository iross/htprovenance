---
id: TASK-2
title: In-job file hashing and lineage queries
status: In Progress
assignee: []
created_date: '2026-11-04 20:40'
updated_date: '2026-11-16 18:38'
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
- [x] #1 htprovenance-hash inputs/outputs emits files.* events with sha256, size, role and path into the job's per-proc NDJSON
- [x] #2 With no paths given, file lists come from the job ad's TransferInput/TransferOutput; executable and container edges are recorded
- [x] #3 Remapped outputs record their destination URL alongside the hash
- [x] #4 The command never changes the job's exit code; missing/unreadable files are recorded and reported on stderr
- [x] #5 Decision on jobs without htprovenance installed is made and recorded in this task
- [x] #6 db build ingests files.* events into file_edges keyed by (schedd, cluster_id, proc_id, role, path); re-ingest is idempotent
- [x] #7 Query commands answer: software for an output, outputs downstream of an input, job chain between two files
- [x] #8 Tests cover: missing and unreadable files, empty transfer lists, queue N procs, remapped outputs, same cluster_id on two schedds, chains of 3+ jobs
- [ ] #9 Hashing time measured on ap2001/ap2002 for a multi-GB staged input and documented in the README
- [x] #10 README documents the two lines a job script adds and the three queries
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Implemented 2026-11-16 on branch feat/file-hash-lineage.

- htprovenance/filehash.py: `htprovenance-hash inputs|outputs [PATH...]` emits files.inputs / files.outputs into the job's per-job NDJSON with schedd, cluster_id, proc_id. Entries carry path, role, sha256, size, mtime_ns, status (ok / missing / unreadable / reference / unresolved), source (the TransferInput entry or Cmd) and destination_url.
- htprovenance/db.py: file_edges table keyed by (schedd, cluster_id, proc_id, role, path); db build upserts it from every files.* event it reads, including on --full-rescan.
- htprovenance/lineage.py + `htprovenance-query files software|downstream|chain`.

Decision for AC #5 (jobs without htprovenance installed): no separate script. filehash and what it imports use only the standard library (post.py now imports yaml inside load_classad_field_mapping), so a job transfers the wheel and runs `PYTHONPATH=<wheel> python3 -m htprovenance.filehash inputs`. Needs Python 3.9+ in the job; jobs with no Python are not covered. Checked locally with /usr/bin/python3 3.9.6 (no yaml/typer) against a built wheel: three chained jobs, one input renamed between jobs, `files chain` returned all three steps.

Choices made where the task left room:
- PATH arguments replace the job ad's TransferInput / TransferOutput list (they do not add to it); executable and container are still recorded on inputs.
- With transfer_output_files unset, outputs are the top-level files that are new or whose size/mtime differ from what the inputs call recorded, minus dotfiles and _condor_stdout/_condor_stderr.
- output_destination is recorded as destination_url for outputs without a remap.
- A TransferInput entry ending in '/' (directory contents) is recorded as status 'unresolved' with a stderr note.
- A container is hashed only when the image is a readable file (sandbox basename or the path as given); otherwise status 'reference' with no hash.
- `files downstream` is one hop (readers and their outputs); `files chain` is the multi-hop search. A name recorded with more than one hash is an error listing the hashes.
- A job that runs again replaces its earlier file_edges rows.

Open: AC #9 (hashing time for a multi-GB staged input on ap2001/ap2002) not measured, and nothing here has run inside a real HTCondor job yet: sandbox names for the executable and URL inputs, and TransferOutputRemaps / OutputDestination formats, are from the job-ad attribute definitions and unit tests only.

Real-job test 2026-11-16 on ap2002 (DAG workflow.dag+8756504, jobs 8756532.0 clean, 8756942.0 train, 8757281.0 report in docker://python:3.12-slim, plus a timing job), run by the user from the untracked ap-check/ kit with htprovenance loaded from the transferred wheel. All 10 checks passed: 4 jobs returned hashes with nothing missing/unreadable/unresolved; inputs recorded under sandbox names; remapped output recorded destination_url cleaned_v1.csv; own NDJSON not listed as output; default output transfer recorded only model.bin; job.sh hashed as executable with the same hash in every job; container recorded by reference; `files chain raw.csv report.txt` returned 3 jobs across the rename. This closes the earlier 'nothing has run inside a real job' gap for local-file inputs, the executable and TransferOutputRemaps. Still not exercised in a real job: URL (osdf://, file://) inputs, OutputDestination, a .sif container file, jobs that also ran autocapture (so `files software` showed 'environment not recorded').

AC #9: timing job hashed a 2 GB file in 5.1 s (~400 MB/s) on e4046; documented in README. The file was generated in the sandbox with dd, not transferred from /staging, and was likely in page cache, so this is not literally the 'staged input' measurement the AC names. Left unchecked pending the user's call.
<!-- SECTION:NOTES:END -->
