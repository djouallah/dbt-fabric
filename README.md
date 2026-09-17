# dbt-fabric — the same business logic, on any dbt adapter

One dbt project that builds the **same AEMO gold layer** on five adapters:

| target | adapter | engine | shape | writes |
|---|---|---|---|---|
| `duckrun` | `duckrun` | DuckDB | single node | Delta Lake on OneLake, via delta-rs |
| `iceberg` | `dbt-duckdb` | DuckDB | single node | Iceberg, through the OneLake Iceberg REST catalog |
| `ducklake` | `dbt-duckdb` | DuckDB | single node | DuckLake parquet + a Delta export, catalog in a Fabric SQL DB |
| `dwh` | `dbt-fabric` | Fabric Warehouse | distributed | Delta tables in the Warehouse, written with T-SQL |
| `spark` | `dbt-fabricspark` | Fabric Spark | distributed | Delta in a Fabric Lakehouse |

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

The table below is what it took to get five engines to the same table. Almost every row is
one of those two kinds. The compute shape appears in exactly one place — the DuckDB legs need
a machine with memory, so in CI they run *inside* Fabric too — and it changed no model.

This replaces four repos that were the same project four times over —
`dbt_fabric_python_iceberg`, `_dwh`, `_ducklake` and `_delta` — which had drifted far enough
that three correctness fixes each existed in exactly one of them.

## Run it

```bash
pip install -r requirements/duckrun.txt
export FILES_PATH=./landing ONELAKE_TABLES_PATH=./warehouse
python download_aemo.py
dbt build --target duckrun --profiles-dir .
```

That works on a laptop with no Fabric account: `duckrun` writes Delta to a local directory.
Swap `--target` for any of the other four once the Fabric env vars are set.

Check the wiring without credentials, and **before spending any capacity**:

```bash
pip install -r requirements/dev.txt
python -m pytest tests_py/ -q                    # column spec, adapter overrides, SQL dialects

pip install -r requirements/duckrun.txt
python .github/scripts/check_gating.py duckrun   # one engine, in that engine's own env
```

Gating runs **one engine per environment**, because the adapters cannot share one:
`dbt-fabric` and `dbt-fabricspark` shadow each other under the `dbt.adapters` namespace.
CI runs it as a five-way matrix; with no argument the script does every engine whose adapter
it can import.

## Layout

```
models/aemo/<engine>/<layer>/<model>.sql   the same 8 model names in all five trees
models/aemo/_staging.yml _dimensions.yml _marts.yml
                                           ONE patch file per layer, shared by all engines
macros/aemo_columns.sql                    the AEMO CSV layout — single source of truth
tests/aemo/<engine>/                       the same 12 assertions, per dialect
download_aemo.py                           one downloader, one landing zone, plain CSV
.github/scripts/check_gating.py            proves the gating, offline
.github/scripts/parity.py                  proves the engines agree
.github/scripts/remote_dbt.py              runs a DuckDB leg inside Fabric (8 vCores)
deploy.py                                  the in-Fabric demo: repo copy, notebook + pipeline, semantic model
fabric_items/                              the scheduled notebook, its variable library, the pipeline
semantic_model/                            one Direct Lake model, deployed once per engine
```

Five copies of each model is the design, not an accident. They are gated so exactly one is
live, and the duplication is what lets each engine say what its adapter forces in plain SQL,
without a thicket of `{% if target.type %}` conditionals. The shared *data* — the AEMO
column layout — lives once, in `macros/aemo_columns.sql`.

### How one run selects one engine

`dbt build --target dwh` sets `target.name == 'dwh'`, so only `models/aemo/dwh/**` is
`+enabled` and the other four trees parse into `manifest['disabled']`. The model file names
are *identical* across the five trees — legal only because exactly one tree is enabled per
run. There is no `--select` anywhere.

Gating is on **`target.name`, not `target.type`**, because `iceberg` and `ducklake` are both
`type: duckdb`. Same reason `macros/iceberg_adapter_overrides.sql` guards each `duckdb__`
override on `target.name`: a `duckdb__` macro dispatches on adapter *type* and so reaches
both targets.

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

## What differs between engines, and why

Every row is forced by the engine, and each is documented at its site in the code. Grouped
by *kind*, because the kind is the finding.

### Dialect — the same sentence, five grammars

| engine | difference |
|---|---|
| dwh | T-SQL: `[date]`/`[time]` bracket quoting, `DATEPART(HOUR) * 100 + DATEPART(MINUTE)` for the HHMM `time` column, no `GREATEST` (a `UNION ALL` instead), `CAST(... AS VARCHAR(256))` because the Warehouse cannot store the `NVARCHAR` its string functions return |
| dwh | T-SQL **pads strings on comparison** (`'ERB01' = 'ERB01 '` is TRUE); DuckDB and Spark do not. One trailing space in a join key split the engines for over a year with every test green |
| spark | `to_timestamp(..., 'yyyy/MM/dd HH:mm:ss')`: Spark's `CAST(string AS TIMESTAMP)` returns `NULL` for slash dates instead of erroring, which silently nulled the whole column. DuckDB and T-SQL both parse slashes, so only this leg was affected |
| spark | no `DOUBLE PRECISION`, `date_format` for `strftime`, `TIMESTAMP` for `TIMESTAMPTZ` — the fingerprint macro carries a dialect branch for exactly this |
| all | `DOUBLE → DECIMAL` tie-breaking is HALF_UP on Spark, HALF_EVEN on DuckDB and a third thing in T-SQL, which is why `parity.py` gives the money columns a relative tolerance and exact-matches everything else |

### Feature implementation — what the engine, adapter or catalog will actually do

| engine | difference |
|---|---|
| iceberg | insert-only merge on every model — the OneLake Iceberg catalog rejects a matched-UPDATE (data files + delete files in one commit) with `BadRequest 400`, "Only one instance of each update type is allowed per request" |
| iceberg | `duckdb__` overrides for the hidden `__` column and for `DROP` without `CASCADE`; no snapshot expiry, so `compact_iceberg.py` is a real job |
| duckrun | `merge` with *do nothing on match* is a DuckDB anti-join plus a plain append — no file rewritten; compaction and vacuum are built into the adapter, so there is no maintenance job |
| ducklake | community `mssql_ducklake` extension (which *replaces* stock `ducklake`), `threads: 1` for a single-writer catalog, compaction hooks, and `delta_export()` — no arguments, it writes each table's `_delta_log` in place, which is why its `data_path` is the lakehouse `Tables/` section |
| ducklake | the catalog DB needs `COLLATE Latin1_General_100_BIN2_UTF8`, which cannot be changed after creation |
| dwh | `OPENROWSET(... PARSER_VERSION='2.0')` over **plain** CSV: Fabric cannot read gzip CSV at all, and PARSER 1.0 cannot parse the ragged AEMO rows; at most 1024 explicit BULK paths *per statement*, hence `process_limit` |
| dwh | `append`, not merge — Fabric has no compare-and-swap, and the file list already excludes what is loaded; never `--full-refresh`, which DROPs and recreates and deadlocks Fabric's background maintenance, loses grants and rebinds Direct Lake |
| dwh | `fct_summary` merges on the full `[date],[time],[DUID]` key like spark. Its full-history lever is `REBUILD_SUMMARY=1` or `--vars 'rebuild_summary: true'` (emit every date, same write path), never `--full-refresh` |
| dwh | the adapter wraps merge models in `MERGE ... USING (<sql>)`, so a top-level `WITH` is illegal, and wraps views in `EXEC('create view ... as <sql>')`, where a leading comment swallows the `SELECT` |
| spark | dbt-fabricspark's `__dbt_tmp` is a **persistent** view on a schema-enabled lakehouse, so a model body can read neither a `TEMPORARY VIEW` nor a path datasource whose name Fabric's base32hex decoder rejects (`text.\`path\`` dies on the `x`; `parquet.\`path\`` only works because every letter of `parquet` is inside `0-9A-V`). The CSV read therefore lands in a `<model>__stage` Delta table from two pre_hooks, and the body reads that |
| spark | `threads: 4` is a hard cap — one Spark REPL per thread, five REPLs per Livy session; the adapter runs `OPTIMIZE` after every build unless told not to; V-Order is a workspace *resource profile*, not a session conf |
| all | `fct_summary` is ONE design on all five, taken from [`direct-lake-parquet-layout`](https://github.com/djouallah/direct-lake-parquet-layout): every run recomputes exactly the dates that could still be stale (never seen, the last 6 days, in the intraday feed) and merges key by key, with the intraday tail gated to units the daily archive can reproduce. Insert-only on the DuckDB family because the OneLake Iceberg catalog rejects a matched UPDATE; update+insert on spark and dwh. To reset one engine's summary, drop the table — the next run recomputes it (`--full-refresh` fails on iceberg) |

### Compute shape — the axis that turned out not to matter

| engine | difference |
|---|---|
| duckrun, iceberg, ducklake | single-node DuckDB folds the archive in memory, so in CI these legs run **inside Fabric** on an 8-vCore Python notebook rather than on the 7 GB hosted runner (which was shut down mid-`fct_scada`). The runner provisions, launches and collects the log. **No model changed.** |
| dwh, spark | dbt runs on the runner as a client; the fold happens in the Warehouse / the Livy session. A first build over the whole archive costs cluster time, which is what `process_limit` bounds |

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
  (one environment per engine). Runs on every push.
- `build.yml` — the reusable per-engine leg: `dbt build` (models and tests) → fingerprint (it lands
  only when the caller passes `land: true`). **duckrun, ducklake and iceberg run dbt on
  Fabric compute** — a throwaway Python notebook of 8 vCores through duckrun's `run_python`
  (`.github/scripts/remote_dbt.py`); tokens are minted inside Fabric by `notebookutils` and
  never travel. dwh and spark run dbt on the runner, where it is only a client of the
  Warehouse / Livy.
- `pipeline.yml` — manual only; lands ONCE in a shared `land` job, then runs all five engines
  in parallel, then the **parity** job compares their fingerprints. `process_limit` is a
  dispatch input: files each fact model folds per run, oldest first, on every engine.
  `deploy` (`none` / `no_model` / `full`) deploys the in-Fabric demo after the build (next
  section).

Manual only because deploying Fabric items and spending capacity is a deliberate act, and
the parity job commits to `history/parity/`, so a push trigger would make the commit start
the next run.

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
