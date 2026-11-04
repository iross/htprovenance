---
id: TASK-3
title: 'Selectable transport for in-job capture: NDJSON file and/or Chirp'
status: To Do
assignee: []
created_date: '2026-11-04 22:33'
labels: []
dependencies:
  - TASK-1
ordinal: 3000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
In-job capture (autocapture's job.assigned) currently only writes NDJSON inside the sandbox. For a plain condor_submit job that file only comes back if the job lists output in transfer_output_files (verified on ap2001, 2026-11-04). HTCondor Chirp lets the job send data to the access point instead.

## What Chirp does at CHTC (ap2001 test, jobs 12419038/12419039, 2026-11-04)

| | default (no WantIOProxy) | +WantIOProxy = true |
|---|---|---|
| condor_chirp on node | /usr/libexec/condor/condor_chirp | same |
| set_job_attr (immediate) | fails, Error 22 | works, any name; logged as 033 events |
| set_job_attr_delayed ChirpProvDelayed | works; attribute is in the job ad | works |
| ulog <text> | works; 008 event in the user's event log | works |

So with no submit-file changes, a job can write ChirpProv* attributes into its own ad (which then reach condor_history and adstash) and lines into its event log.

## Design

- Toggle: PROVENANCE_TRANSPORT = file | chirp | both. Default when unset: both.
- file: today's NDJSON behavior from TASK-1.
- chirp: job.assigned fields sent as delayed ChirpProv* attributes (e.g. ChirpProvRunId, ChirpProvHostname, ChirpProvGpuModel, ChirpProvCodeCommit, ChirpProvHtprovenanceVersion). Exact attribute set and naming to be decided after the probe (one attribute per field vs. one JSON attribute depends on size limits).
- If chirp is selected and fails (binary missing, nonzero exit), fall back to writing the file and record that in the event's transport field. Capture never fails the job.
- Consumers: enrich-history projects ChirpProv* attributes into condor_history (and/or the events table, as job.assigned with source=chirp). scan is unchanged unless ulog is used.
- Unknown values for PROVENANCE_TRANSPORT: warn on stderr and use the default.

## Open questions (probe job before implementing)

1. Containers: is condor_chirp reachable inside an Apptainer image? If not, can a pure-Python client (htchirp) do delayed updates without the I/O proxy?
2. Do delayed updates require the Chirp prefix (CHIRP_DELAYED_UPDATE_PREFIX)?
3. Limits on number of delayed attributes and value length; ulog message length.
4. How condor_chirp reaches the starter without WantIOProxy (env vars / files in the sandbox), so a Python client can do the same.

Out of scope: TASK-2's file hashes. Choose their transport once this exists (ulog is a candidate for per-file events).
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Probe job answers open questions 1-4; results recorded in this task
- [ ] #2 PROVENANCE_TRANSPORT selects file, chirp or both; unset means both; unknown values warn and use the default
- [ ] #3 chirp mode sends job.assigned fields as delayed ChirpProv* attributes visible in the job ad and condor_history
- [ ] #4 If chirp fails, the event is written to the NDJSON file instead and capture never affects the job's exit code
- [ ] #5 enrich-history maps ChirpProv* attributes into provenance.db
- [ ] #6 Tests cover each mode, unknown value, chirp binary missing, chirp nonzero exit (chirp invocation mocked)
- [ ] #7 Verified on ap2001 with a real job in each mode, including one in a container
- [ ] #8 README documents the toggle, the default, and what each transport needs
<!-- AC:END -->
