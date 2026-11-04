# htprovenance

Provenance capture and querying for HTCondor jobs and the artifacts they produce.

Split out of [iross/MLDAG](https://github.com/iross/MLDAG), which now depends on
this package for the provenance half of its job instrumentation while keeping
DAG generation, monitoring and reporting on its own side.

## Installing

```bash
uv add htprovenance

# Or from a pinned git tag
uv add "htprovenance @ git+https://github.com/iross/htprovenance.git@v0.1.0rc1"
```

Dependencies are deliberately minimal — `pyyaml`, `typer`, and `htcondor` (Linux
only) — so this installs cleanly inside a job sandbox without pulling in the
plotting/dataframe stack that DAG generation and reporting need.

## What it does

Provenance is captured from three independent places and merged into one SQLite
database keyed by `(cluster_id, proc_id)`:

- **Submit side.** `htprovenance-pre` / `htprovenance-post` run as DAGMan
  `SCRIPT PRE`/`POST` and emit `job.submitted` / `job.completed` / `job.failed`
  NDJSON events. `htprovenance-log-monitor` runs as a DAGMan `SERVICE`, tailing
  the HTCondor event log for lifecycle transitions the PRE/POST pair can't see
  (`job.executing`, transfer start/stop, evictions, resource usage).
- **Execute side, zero config.** Once this package is installed in the job's
  Python environment, a `.pth` hook fires at interpreter startup, detects
  `$_CONDOR_JOB_AD`, and records site/hardware/software context plus the job's
  submit-time ClassAd attributes — no code required in the training script. See
  `htprovenance/autocapture.py`. On the access point, DAGMan and its `SERVICE`
  nodes are recorded as `dagman.started` / `service.started` in
  `dagman-<DAGMan cluster>.ndjson` instead.
- **After the fact.** `htprovenance-query db enrich-history` backfills from
  `condor_history`; `htprovenance-query db enrich-jobad` mirrors the in-job
  capture into the same table; a raw event log can be scanned directly for jobs
  that were never instrumented at all.

## Run IDs for jobs outside a generated DAG

The execute-side capture files its `job.assigned` event under a `run_id`,
resolved from the job's environment and ClassAd:

1. `PROVENANCE_RUN_ID`, if set. mldag's generated DAGs set it; those events go
   to `<run_id>.ndjson` as before.
2. `JobBatchName`, if set. Submits that share a batch name share a `run_id`.
   A hand-written DAG gets DAGMan's default batch name
   (`<dagfile>+<DAGMan cluster>`), so its nodes group per DAG run.
3. `cluster-<ClusterId>` otherwise: one `condor_submit` is one run.

For cases 2 and 3 each job writes its own `<run_id>.<cluster>.<proc>.ndjson`,
so the procs of a `queue N` never overwrite each other's file when their
outputs come back to the same directory. The file name has unsafe characters
in the batch name replaced with `_` and is capped in length; the `run_id`
inside each event keeps the original text, and `db build` reads it from there.

The event also records `schedd` (taken from `GlobalJobId`), since a
`ClusterId` is only unique per access point.

## CLI

| Command | Purpose |
|---|---|
| `htprovenance-pre` | DAGMan `SCRIPT PRE` hook — emits `job.submitted` |
| `htprovenance-post` | DAGMan `SCRIPT POST` hook — emits `job.completed`/`job.failed` |
| `htprovenance-log-monitor` | DAGMan `SERVICE` — tails the HTCondor event log |
| `htprovenance-query` | Build/query the provenance database; lineage lookups |
| `htprovenance-repair` | Repair/backfill existing provenance data |

## Configuring which ClassAd fields land in provenance

`htprovenance-post` captures a curated subset of each job's HTCondor ClassAd (from
`job_ad_file`) into the `job.completed`/`job.failed` provenance events. The
default mapping (used when no file is configured, or the configured file
doesn't exist) is:

| ClassAd attribute | Provenance field |
|---|---|
| `RemoteWallClockTime` | `wall_time_s` |
| `CPUsUsage` | `cpu_usage` |
| `MemoryUsage` | `peak_memory_mb` |
| `GPUsUsage` | `gpu_usage` |
| `GLIDEIN_ResourceName` | `resource_name` |
| `Arguments` | `arguments` |

To capture different or additional fields, add a `classad_fields_file` entry
to `Experiment.yaml` pointing at a YAML file (conventionally
`provenance_fields.yaml`) listing the attributes to extract:

```yaml
fields:
  RequestCpus: num_cpus_requested   # explicit rename
  Cmd                                # bare entry -> auto snake_case ("cmd")
```

The path is baked into the generated DAG at DAG-generation time (`mldag-gen`) (like
`--log-dir`), so it can't drift between runs — but you can edit the file's
*contents* at any time without regenerating the DAG. Some ClassAd attributes
(currently just `Environment`, which can carry secrets like a W&B API key)
are blocked outright; requesting one raises an error when the file is loaded.

## Inspecting jobs without the full provenance pipeline

Two commands work on jobs that were never instrumented with the PRE/POST
provenance scripts (a one-off batch, a hand-submitted `.dag`) — no run_id or
NDJSON events required. Both enrich opportunistically when related data is
available and silently fall back to bare cluster_id/no-op when it isn't:

```bash
# Duration, execute host/site, and resource usage straight from an event log
htprovenance-query scan metl.log

# Filter to specific clusters (repeatable; every proc of a matched cluster is kept)
htprovenance-query scan metl.log --cluster-id 12345 --cluster-id 12399

# Dump results into provenance.db's condor_history table (source='event_log')
# instead of/alongside printing them
htprovenance-query scan metl.log --db provenance.db

# Backfill provenance.db's condor_history table from HTCondor job history
# (final host, exit code, hold reasons, requested vs. used resources)
htprovenance-query db enrich-history --schedd <name>

# Mirror job.assigned events (jobad.py's in-job $_CONDOR_JOB_AD capture,
# emitted immediately at job start) into condor_history too
htprovenance-query db enrich-jobad
```

`scan` parses any HTCondor event log directly; if `--log-dir`/
`--provenance-log-dir` happen to point at a DAGMan provenance run's classad or
NDJSON directories, matching jobs are enriched with `run_id`/`job_name` too.
Jobs are keyed by `(cluster_id, proc_id)`, not `cluster_id` alone, since a
`queue N` job array puts many procs under one cluster.

`condor_history` holds rows from any of three sources — `db enrich-history`
(queried from HTCondor), `scan --db` (parsed from a raw event log), or
`db enrich-jobad` (mirrored from `job.assigned` events already in the `events`
table) — distinguished by its `source` column, since they can disagree and
none is definitively more current than the others. Writing from more than one
for the same `(cluster_id, proc_id)` never produces duplicate rows or a
clobber: a write merges column-by-column into any existing row (a column the
new write doesn't know about keeps its previous value), and `source` becomes
e.g. `condor_history,jobad` once both have contributed. `enrich-jobad`
deliberately never writes `remote_wall_clock_s`/`cpus_usage`/`memory_usage_mb`/
`gpus_usage`: the job ad is captured at submission, before the job has run,
so those fields would be near-zero placeholders rather than real usage — use
`enrich-history` or `scan` for those.

`db enrich-history` queries `condor_history` via the HTCondor Python bindings
(not the CLI) for every cluster_id already in `provenance.db`'s `events`
table, and is safe to re-run — already-enriched cluster_ids are skipped
unless `--full-rescan` is passed. Requires `htcondor2` (Linux only).

## Relationship to mldag

`mldag` depends on `htprovenance`, not the other way around. It bakes
`python -m htprovenance.pre/post/log_monitor` into the DAGs it generates, and
passes the installed `htprovenance` version into each job's environment as
`HTPROVENANCE_VERSION` so the job can install a matching copy of this package.

Because those `SCRIPT PRE/POST/SERVICE` lines are plain strings in a generated
`.dag` file rather than checked imports, a DAG generated against one module
layout keeps referring to it until the DAG is regenerated. Drain in-flight DAGs
before upgrading the submit-side install across a rename, and regenerate `.dag`
files rather than hand-editing them.

## Development

```bash
uv sync
uv run pytest tests/ -q
```
