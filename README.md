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
  `htprovenance/autocapture.py`.
- **After the fact.** `htprovenance-query db enrich-history` backfills from
  `condor_history`; `htprovenance-query db enrich-jobad` mirrors the in-job
  capture into the same table; a raw event log can be scanned directly for jobs
  that were never instrumented at all.

## CLI

| Command | Purpose |
|---|---|
| `htprovenance-pre` | DAGMan `SCRIPT PRE` hook — emits `job.submitted` |
| `htprovenance-post` | DAGMan `SCRIPT POST` hook — emits `job.completed`/`job.failed` |
| `htprovenance-log-monitor` | DAGMan `SERVICE` — tails the HTCondor event log |
| `htprovenance-query` | Build/query the provenance database; lineage lookups |
| `htprovenance-repair` | Repair/backfill existing provenance data |

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
