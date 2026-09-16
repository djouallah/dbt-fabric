# dbt-fabric — the same business logic, on any dbt adapter

One dbt project that builds the **same AEMO gold layer** on five adapters:

| target | adapter | compute | writes |
|---|---|---|---|
| `duckrun` | `dbt-duckrun` | DuckDB | Delta Lake on OneLake, via delta-rs |
| `iceberg` | `dbt-duckdb` | DuckDB | Iceberg, through the OneLake Iceberg REST catalog |
| `ducklake` | `dbt-duckdb` | DuckDB | DuckLake parquet + a Delta export, catalog in a Fabric SQL DB |
| `dwh` | `dbt-fabric` | Fabric Warehouse | Warehouse tables (T-SQL) |
| `spark` | `dbt-fabricspark` | Fabric Spark | Delta in a Fabric Lakehouse |

**The thesis: the engine does not matter, the output does.** Every engine builds the same
eight models to the same schema and the same numbers. The only permitted difference between
them is *operational* — SQL dialect, adapter capability, incremental strategy, maintenance.
No engine gets extra models, extra columns, or different semantics, and
`.github/scripts/parity.py` compares the gold table across engines so that is a check rather
than a claim.

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
```

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
`(date, time, DUID)` joining generation to the matching regional price.

Models that existed on only one engine are deliberately **not** carried over — that is
exactly what this repo exists to stop. They remain in the original repos' history.

## What differs between engines, and why

Every one of these is forced by the engine, and each is documented at its site:

| engine | forced difference |
|---|---|
| iceberg | insert-only merge on every model — the OneLake Iceberg catalog rejects a matched-UPDATE (data files + delete files in one commit) with `BadRequest 400`, "Only one instance of each update type is allowed per request" |
| iceberg | `duckdb__` overrides for the hidden `__` column and for `DROP` without `CASCADE` |
| ducklake | community `mssql_ducklake` extension (which *replaces* stock `ducklake`), compaction and `delta_export()` hooks, `threads: 1` for a single-writer catalog |
| ducklake | the catalog DB needs `COLLATE Latin1_General_100_BIN2_UTF8`, which cannot be changed after creation |
| dwh | `OPENROWSET(... PARSER_VERSION='2.0')` over **plain** CSV: Fabric cannot read gzip CSV at all, and PARSER 1.0 cannot parse the ragged AEMO rows |
| dwh | `append`, not merge — Fabric has no compare-and-swap, and the file list already excludes what is loaded |
| dwh | never `--full-refresh`: it DROPs and recreates, which deadlocks Fabric background maintenance, loses grants and rebinds Direct Lake |
| dwh | ≤1024 explicit BULK paths *per statement*, hence `process_limit` |
| spark | `from_csv` over `text.\`path\`` — `__dbt_tmp` is a persistent view and cannot reference a TEMPORARY VIEW |
| spark | `to_timestamp(..., 'yyyy/MM/dd HH:mm:ss')`: Spark's `CAST` returns NULL for slash dates instead of erroring, which silently nulled the column. DuckDB and T-SQL both parse them, so only this leg was affected |
| spark | `threads: 4` is a hard cap — one Spark REPL per thread, five REPLs per Livy session |

## Landing

One `download_aemo.py`, one landing zone, **plain CSV for every engine**. The DuckDB repos
used to gzip into `csv/` while dwh landed plain into `csv_raw/`; if the engines read
different bytes then comparing their output means nothing.

It is a script, not a dbt python model, because the Fabric Warehouse adapter's python
models are PySpark-via-Livy only and the spark leg has no usable python-model runtime
either — which is why the dwh repo already kept a fifth copy of it outside `model-paths`.

## Isolating a run

`DBT_SCHEMA` is the redirect lever. Unset or `mart` gives the production schemas
(`landing`, `mart`); anything else prefixes them (`DBT_SCHEMA=dbt` → `dbt_landing`,
`dbt_mart`). Every engine goes through `generate_schema_name()`, so one env var moves a whole
run out of the way.

## CI

- `ci.yml` — free and credential-less: pytest, plus `check_gating.py` as a five-way matrix
  (one environment per engine). Runs on every push.
- `build.yml` — the reusable per-engine leg: land → `dbt build` → test → fingerprint.
- `pipeline.yml` — manual only; runs the engines one at a time, then the **parity** job
  compares their fingerprints.

Manual only because deploying Fabric items and spending capacity is a deliberate act, and
the parity job commits to `history/parity/`, so a push trigger would make the commit start
the next run.

Note: cancelling a GitHub job does **not** stop Fabric — the notebook or Livy session keeps
running, and billing.

## License

MIT
