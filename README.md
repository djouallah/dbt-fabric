# dbt-fabric — the same business logic, on any dbt adapter

One dbt project that builds the **same AEMO gold layer** on five adapters:

![Bronze, silver, gold - and the engine is a variable. Bronze is files, not tables: download_aemo.py, plain python, pulls the nemweb zips and lands plain CSV plus an archive-log parquet, once, the same bytes for every engine. Silver is <engine>_landing, in dbt SQL: stg_csv_archive_log is a view on that log saying which files are new; fct_price and fct_scada type and dedupe the daily archive keeping all 130 and all 53 columns; fct_price_today and fct_scada_today do the same for the intraday feed, still in AEMO's own shape. Gold is <engine>_mart, in dbt SQL: fct_summary, one row per (date, time, DUID) joining generation to the price of its region and merged key by key, beside the dimensions it is read through, dim_duid and dim_calendar - Direct Lake reads all three in place, no copy. Underneath silver and gold sits the only thing that changes, dbt build --target <engine>: five model trees of the same SQL (at least in spirit) where the flag enables one, running on duckrun (DuckDB to Delta via delta-rs), iceberg (dbt OSS 2 to an Iceberg REST catalog), ducklake (DuckDB plus a SQL catalog, parquet with Delta metadata exported over it), dwh (Fabric Warehouse, Delta) or spark (Fabric Spark, Delta in a Lakehouse).](docs/how-it-works-dark.svg)

| target | adapter | engine | shape | writes |
|---|---|---|---|---|
| `dwh` | `dbt-fabric` | Fabric Warehouse | distributed | Delta tables in the Warehouse |
| `spark` | `dbt-fabricspark` | Fabric Spark | distributed | Delta in a Fabric Lakehouse |
| `duckrun` | `duckrun` | DuckDB | single node | Delta Lake on OneLake, via delta-rs |
| `iceberg` | `dbt-oss` 2 | DuckDB | single node | Iceberg, through the OneLake Iceberg REST catalog |
| `ducklake` | `dbt-duckdb` | DuckDB | single node | DuckLake parquet + a Delta export, catalog in a Fabric SQL DB |

## The thesis, and what the repo actually found

**In theory the engine is abstract.** dbt's promise is that a model is a `SELECT` and the
adapter does the rest: change `--target`, get the same table somewhere else. For the
*business logic* that holds. The eight models compute the same numbers on all five engines,
and `.github/scripts/parity.py` fingerprints the gold table on each and compares them, so
that is a check rather than a claim.

**In practice the engine is not abstract at all — but the axis people expect is the wrong
one.** Whether an engine is distributed or single-node barely shows up. Read the five
`fct_summary.sql` files side by side and nothing in them cares that Spark has executors and
DuckDB has one process. What does show up, on every single model, is:

1. **The SQL dialect.** Bracket quoting, `DATEPART` against `strftime`, no `GREATEST` in
   T-SQL, `to_timestamp` with an explicit format because Spark's `CAST` turns a slash date into
   `NULL` rather than an error, `DOUBLE PRECISION` that Spark rejects, string comparison that
   pads in T-SQL and does not elsewhere, three different ways to round a `DOUBLE` into a
   `DECIMAL`.
2. **What each engine and adapter actually implements.** Which `MERGE` shapes the catalog
   accepts, whether the adapter's temporary relation is a temp view or a persistent one, how
   a CSV can be read at all, how many file paths one statement may name, whether
   `--full-refresh` is safe, whether compaction is built in, what a hook may call.

[`docs/engine-nuances.md`](docs/engine-nuances.md) is what it took to get five engines to the
same table. Almost every row there is one of those two kinds. The compute shape appears
in exactly one place — the DuckDB legs need a machine with memory, so in CI they run
*inside* Fabric too — and it changed no model.

This replaces four repos that were the same project four times over —
`dbt_fabric_python_iceberg`, `_dwh`, `_ducklake` and `_delta` — which had drifted far enough
that three correctness fixes each existed in exactly one of them.

## Layout

```
dbt1/                                      dbt-core 1.x: duckrun, ducklake, dwh, spark
dbt2/                                      dbt OSS 2: iceberg (+ its catalogs.yml)
<project>/models/aemo/<engine>/<layer>/<model>.sql
                                           the same 8 model names in all five trees
<project>/models/aemo/_staging.yml _dimensions.yml _marts.yml
                                           ONE patch file per layer; the two projects' copies
                                           are pinned identical by tests_py
macros/aemo_columns.sql                    the AEMO CSV layout — single source of truth,
                                           SHARED: both projects read ../macros
<project>/tests/aemo/<engine>/             the same 12 assertions, per dialect
download_aemo.py                           one downloader, one landing zone, plain CSV
.github/scripts/check_gating.py            proves the gating, offline
.github/scripts/parity.py                  proves the engines agree
.github/scripts/check_catalog_stats.py     proves the published page is not empty
.github/scripts/remote_dbt.py              runs a DuckDB leg inside Fabric (8 vCores)
.github/scripts/record.py                  the run record: items by GUID, each leg's window
.github/scripts/layout.py                  what each engine wrote: files, row groups, encodings, order
.github/scripts/measure_cu.py              what each engine cost: capacity units per run and engine
history/                                   parity/ fingerprints, runs/ records, cu.json ledger
docs/run.md                                run it, locally or on Fabric
docs/engine-nuances.md                     what differs between the five, and why
docs/candidate-engines.md                  Sail and Polars: what stopped a sixth leg
deploy.py                                  the in-Fabric demo: repo copy, notebook + pipeline, semantic model
fabric_items/                              the scheduled notebook, its variable library, the pipeline
semantic_model/                            one Direct Lake model, deployed once per engine
```

Five copies of each model is the design, not an accident. They are gated so exactly one is
live, and the duplication is what lets each engine say what its adapter forces in plain SQL,
without a thicket of `{% if target.type %}` conditionals. The shared *data* — the AEMO
column layout — lives once, in `macros/aemo_columns.sql`, which both projects read.

**Why two projects.** `catalogs.yml` is how dbt 2 declares an Iceberg REST catalog, and dbt
reads it from the directory holding `dbt_project.yml`. Put one next to a dbt 1.x project and
every engine in it dies — `Adapter 'duckdb' does not support catalogs.yml v2 yet` with
`use_catalogs_v2` set, or a v1-loader validation error without it. So the split is by dbt
major version and nothing else: same models, same patch files, same shared macros, same
`iceberg_landing` / `iceberg_mart` schemas the dbt-duckdb leg wrote before it.

### How one run selects one engine

`dbt build --target dwh` in `dbt1/` sets `target.name == 'dwh'`, so only
`models/aemo/dwh/**` is `+enabled` and its three sibling trees parse into
`manifest['disabled']`. The model file names are *identical* across all five trees — legal
only because exactly one tree is enabled per run. There is no `--select` anywhere.

Gating is on **`target.name`, not `target.type`**. The four `dbt1` targets happen to have
four distinct types today, but that is an accident of the engine list — `target.name` is the
folder name, which is what selection actually means. `dbt2` holds one tree and keeps the same
gate anyway: without it a run under the wrong target name would build into the wrong schema
rather than building nothing.

**The default failure mode of this design is a green run that built nothing.** A target name
that matches no folder disables everything, and `dbt build` then reports "Nothing to do" and
exits 0. That is what `check_gating.py` is for, and why CI runs it before anything spends.

## The gold layer

Eight models, identical on all five engines:

`stg_csv_archive_log` · `dim_calendar` · `dim_duid` · `fct_price` · `fct_price_today` ·
`fct_scada` · `fct_scada_today` · `fct_summary`

`fct_summary` is the Power BI-facing table and the one parity compares: one row per
`(date, time, DUID)` joining generation to the matching regional price. `fct_price` is
AEMO's DREGION record (all 130 columns) and `fct_scada` the DUNIT record (all 53); the
summary exposes five of those columns, the wide facts are the analytical surface.

Models that existed on only one engine are deliberately **not** carried over — that is
exactly what this repo exists to stop. They remain in the original repos' history.

## Landing

One `download_aemo.py`, one landing zone, **plain CSV for every engine**. The DuckDB repos
used to gzip into `csv/` while dwh landed plain into `csv_raw/`; if the engines read
different bytes then comparing their output means nothing.

It is a script, not a dbt python model, because the Fabric Warehouse adapter's python
models are PySpark-via-Livy only and the spark leg has no usable python-model runtime
either — which is why the dwh repo already kept a fifth copy of it outside `model-paths`.

## What it creates in Fabric

Four items, all inside one workspace folder (`FOLDER`, default `dbt`):

```
dbt/
├── dbt_landing        Lakehouse  csv_raw/ + the archive log. Written ONCE, read by all five.
├── dbt                Lakehouse  duckrun_* iceberg_* ducklake_* spark_* schemas
├── dbt_dwh            Warehouse  dwh_landing, dwh_mart
└── dbt_ducklake_meta  SQL DB     DuckLake catalog metadata only, no data
```

The engines used to get a data item each, which is up to fourteen items at the root of a
workspace once you count the SQLEndpoint shadow item every lakehouse drags along. **They
share one lakehouse and are kept apart by schema instead.** The Warehouse and the SQL DB
cannot join them — a Warehouse cannot hold Delta tables another engine wrote, and DuckLake's
catalog must be a SQL database.

Two landing variables, and the difference matters:

| var | meaning |
|---|---|
| `LANDING_PATH` | where `download_aemo.py` writes. **Identical on all five legs.** |
| `FILES_PATH` | how *this engine's* dbt reads that zone — the same path, except dwh, which reads through a shortcut because a Warehouse has no `Files` section |

Locally you only need `FILES_PATH`; the downloader falls back to it.

## Isolating a run

Schemas are always `<engine>_<layer>`, and `DBT_SCHEMA` prefixes on top of that:

| `DBT_SCHEMA` | staging | marts |
|---|---|---|
| unset / `mart` | `iceberg_landing` | `iceberg_mart` |
| `test` | `test_iceberg_landing` | `test_iceberg_mart` |

Every engine goes through `generate_schema_name()`, so one env var moves a whole run out of
the way. The engine half is not cosmetic — five engines share one lakehouse, so two of them
resolving to the same schema would overwrite each other's gold layer with every test still
green. `check_gating.py` asserts the prefix offline.

## CI

- `ci.yml` — free and credential-less: pytest, plus `check_gating.py` as a five-way matrix
  (one environment per engine). Runs on every push. The matrix cannot be collapsed into one
  job: `dbt-fabric` and `dbt-fabricspark` shadow each other under `dbt.adapters`, and dbt OSS
  2 and `duckrun` both want to own the `dbt` console script.
- `pipeline.yml` — manual only, and the whole pipeline in one file: it lands ONCE in a shared
  `land` job, then runs all five engines in parallel as a matrix of `build` legs, then the
  **layout** job reads every engine's tables back and the **record** job compares the
  fingerprints and commits one run record to `history/runs/`. Each leg does `dbt build`
  (models and tests) → fingerprint. **duckrun, ducklake and iceberg run dbt on Fabric
  compute** — a throwaway Python notebook of 8 vCores through duckrun's `run_python`
  (`.github/scripts/remote_dbt.py`); tokens are minted inside Fabric by `notebookutils` and
  never travel. dwh and spark run dbt on the runner, where it is only a client of the
  Warehouse / Livy. A `compact` job folds the Iceberg catalog's small files afterwards, before
  `layout` measures them. `process_limit` is a dispatch input: files each fact model folds per
  run, oldest first, on every engine. `deploy` (`none` / `no_model` / `full`) deploys the
  in-Fabric demo after the build (next section).
- `capacity.yml` — fires after every pipeline run and once a day: reads capacity units per
  run and engine from the Fabric Capacity Metrics model and commits `history/cu.json`.
- `docs.yml` — fires after every pipeline run: `dbt docs generate --static` and deploys the
  one self-contained page to GitHub Pages — **[the DAG and the catalog](https://djouallah.github.io/dbt-fabric/)**.
  It builds nothing and spends no Fabric compute. **duckrun of the five**, because the DAG is
  the same on all of them and the *catalog* is not: duckrun reports `num_rows`, `bytes` and
  `last_modified` out of the Delta log, where dbt-fabric's catalog query gives an approximate
  row count and nothing else. It is also the only engine whose models guard their parse-time
  `run_query` on `flags.WHICH`, so generating its docs fires no query; on dwh the same command
  would run real `OPENROWSET` queries against the Warehouse.

Manual only because deploying Fabric items and spending capacity is a deliberate act, and
the record job commits to `history/`, so a push trigger would make the commit start the
next run.

## What it cost, and what it wrote

Ported from [`direct-lake-parquet-layout`](https://github.com/djouallah/direct-lake-parquet-layout)
and adapted to shared, persistent items. Every pipeline run leaves one record in
`history/runs/` — the Fabric item GUIDs it touched, each leg's compute window, the parquet
layout of every engine's tables (files, row groups, `fct_summary`'s per-column encodings and
physical row order) and the parity fingerprints — and the `Capacity units` workflow keeps
`history/cu.json`: **compute** capacity units per run and engine, read from the Capacity Metrics
model for each leg's items inside its own hours. Storage transactions are deliberately not
attributed: the lakehouse is shared, so its OneLake operations in any window belong to
everybody. [`history/README.md`](history/README.md) has the schemas and the caveats.

Note: cancelling a GitHub job does **not** stop Fabric — the notebook or Livy session keeps
running, and billing.

## Scheduling it inside Fabric

```bash
gh workflow run pipeline.yml -f engines=iceberg -f deploy=full   # or Actions → pipeline → Run workflow
```

`deploy` is a dispatch input of `pipeline.yml` (`none` by default; `no_model` skips the
semantic models and their reframe, the slow part). The `deploy` job runs `deploy.py` after
the build legs, with the same OIDC identity — duckrun mints the storage, Fabric and Power BI
tokens from the GitHub assertion, so there is no login step. It copies the git-tracked repo
into the `dbt` lakehouse's `Files/dbt`, deploys `fabric_items/` (a notebook, its
`deploy_config` variable library, a pipeline), one semantic model per engine built, and
schedules the pipeline every 12 hours.

The notebook is the scheduled form of one CI leg: it runs the same `provision.py` →
`download_aemo.py` → `run_in_fabric.py` from that copy, on the engine `dbt_target` in
`fabric_items/deploy_config.VariableLibrary/variables.json` names (edit the file to change
it). The pipeline runs it at 2 vCores and again at 8 if that fails (`pipelinecore`). Do not
run it alongside `pipeline.yml`; both land into `dbt_landing`.

The semantic model deploys as `aemo_<engine>`, bound to `<engine>_mart`. Direct Lake has no
schema parameter — a partition's schema is a literal in the model — so `deploy.py` writes it
in per engine, the way duckrun writes the OneLake GUIDs. A Direct Lake model reframes on
deploy, which is why it deploys after the build. dwh's model reads the Warehouse item through
Direct Lake on OneLake.

## License

MIT
