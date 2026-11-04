---
id: TASK-1
title: Default run_id for non-DAG jobs and record the schedd
status: In Progress
assignee: []
created_date: '2026-11-04 20:40'
updated_date: '2026-11-04 22:25'
labels: []
dependencies: []
ordinal: 1000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Outside a generated DAG, PROVENANCE_RUN_ID is unset, so autocapture emits job.assigned under run_id 'unknown' to output/provenance/unknown.ndjson (htprovenance/autocapture.py _capture_execute_node). Every non-DAG job shares that run_id, and there is no way to group a batch.

## run_id resolution

Applies to execute-node capture only (autocapture._capture_execute_node), the one place a non-DAG job's run_id is produced. The access-point branch in autocapture (dagman-<cluster> for DAGMan, local-<cluster> for other local-universe jobs) stays as it is.

From $_CONDOR_JOB_AD:
1. PROVENANCE_RUN_ID if set (DAG path unchanged)
2. JobBatchName if set. Reusing a batch name across several submits intentionally groups them under one run_id. A hand-written DAG gets DAGMan's default batch name (<dagfile>+<dagman cluster>), so its nodes group per DAG run.
3. cluster-<ClusterId>

post.py and log_monitor are not changed. They only run inside DAGs, and their 'unknown' / unknown:<cluster_id> run_ids mark a DAG job whose run_id hasn't been resolved yet: log_monitor retries those and repair.py rewrites unknown*.ndjson files after the fact. Replacing them with cluster-<id> would make unresolved DAG jobs look like standalone clusters.

## NDJSON filenames

Procs write NDJSON in their own sandboxes and overwrite each other when transferred back into a shared Iwd. Name files <run_id>.<cluster>.<proc>.ndjson. The cluster is needed because a JobBatchName run_id can span several clusters. JobBatchName is free text, so sanitize it for use in a filename (the event's run_id field keeps the original). The schedd is not part of the filename. db.build_database reads run_id from each event line, not the filename; verify.

## Schedd

ClusterIds are unique only per schedd, so a database holding jobs from more than one access point currently merges unrelated jobs into one condor_history row. Record the schedd and make it part of the key:

- job.assigned gets a schedd field, taken from GlobalJobId ('schedd#cluster.proc#qdate').
- condor_history's primary key becomes (schedd, cluster_id, proc_id). schedd is NOT NULL: SQLite treats NULLs in a primary key as distinct, which would break the merge-on-write behavior.
- enrich-jobad fills schedd from the job.assigned payload; enrich-history from GlobalJobId in the history ad. enrich-history's discovery (_all_known_cluster_ids) works on (schedd, cluster_id) pairs and only queries the schedd it is pointed at for that schedd's clusters.
- scan reads a raw event log, which has no GlobalJobId. Derive the schedd from the alias= in the submit event's sinful string when present, and add a --schedd option to set or override it. If neither yields a schedd, scan --db refuses to write rather than inserting rows that can never merge with enrich-history's rows.
- _drop_stale_condor_history detects the old primary key (not just missing columns) so older databases get the table recreated.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 Without PROVENANCE_RUN_ID, an execute-node job's run_id is JobBatchName when set, else cluster-<ClusterId>
- [x] #2 PROVENANCE_RUN_ID takes precedence when set (DAG path unchanged)
- [x] #3 Access-point captures still use dagman-<cluster> and local-<cluster>
- [x] #4 post.py and log_monitor keep their unresolved-DAG-run sentinels that repair.py depends on
- [x] #5 NDJSON files are named <run_id>.<cluster>.<proc>.ndjson with a filename-safe run_id and no schedd; two procs never share a file
- [x] #6 db build ingests the new files with the original (unsanitized) run_id
- [x] #7 job.assigned includes schedd from GlobalJobId
- [x] #8 condor_history is keyed by (schedd, cluster_id, proc_id) with schedd NOT NULL; same cluster/proc from two schedds gives two rows
- [x] #9 enrich-jobad and enrich-history fill schedd; enrich-history only queries the given schedd for its own clusters
- [x] #10 scan derives schedd from the submit event alias or --schedd; scan --db errors clearly when it has neither
- [x] #11 An existing provenance.db with the old condor_history key is detected and the table recreated
- [x] #12 Tests cover: env var set, JobBatchName only, neither, JobBatchName with unsafe characters, unreadable job ad, multiple procs, two schedds with the same cluster_id, scan with and without a derivable schedd
- [x] #13 README documents run_id resolution order and the NDJSON naming scheme
- [x] #14 README's statements that jobs are keyed by (cluster_id, proc_id) are updated to include schedd, and scan's --schedd option is documented
- [x] #15 Docstrings that describe the old behavior are updated: autocapture module and capture_and_emit, events.event_log_path, db.py's condor_history schema comment, event_log_scan module
- [x] #16 Verified on ap2001 whether output/provenance/ comes back without explicit transfer_output_files; README documents the answer
<!-- AC:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Implemented in 4243cf9 (run_id fallback, per-proc NDJSON, schedd in job.assigned) and ad8a484 (condor_history keyed by schedd).

Scope change vs. original plan: post.py and log_monitor keep their 'unknown' / unknown:<cluster_id> run_ids. They only run inside DAGs, and those values mark unresolved DAG runs that log_monitor retries and repair.py rewrites.

Added beyond the ACs: enrich-jobad --schedd, so job.assigned events captured before schedd was recorded can still be mirrored (otherwise the existing cluster_id backfill for old mldag data would silently stop working).

Unverified on a real access point (tests use fakes):
- _get_schedd now locates the local schedd via Collector.locate(DaemonTypes.Schedd) with no name, to read its Name. Confirm on ap2001/ap2002 that this returns the local schedd.
- scan's schedd comes from alias= in the 000 event's 'Job submitted from host: <...>' address. Confirm against a real log (e.g. the ap2002 precmd test.log) that alias is present and equals the schedd Name in GlobalJobId.
- AC #16 (does output/provenance/ come back without transfer_output_files) is still open.

Verified 2026-11-04:
- ap2002: Collector().locate(DaemonTypes.Schedd)['Name'] returns ap2002.chtc.wisc.edu; the submit event's alias=ap2002.chtc.wisc.edu matches GlobalJobId's schedd part (with an IPv6 addrs= entry in the address, which the regex handles).
- ap2001 jobs 12419027 / 12419028: with no transfer_output_files, only top-level top.txt came back (4 bytes sent); output/provenance/test.ndjson did not. With transfer_output_files = output, top.txt, both came back (22 bytes). Documented in README.
Open question raised with the user: non-DAG jobs therefore don't return their NDJSON by default; consider writing to the sandbox top level when not in a DAG. Related: site_info.json and the .htprovenance_autocapture_done marker are top-level files, so they already come back by default and site_info.json overwrites across procs in a shared Iwd.
<!-- SECTION:NOTES:END -->
