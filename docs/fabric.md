# On Fabric — what it lands and creates

The Fabric side: where the raw AEMO data lands, the items a run creates in one workspace
folder, and how to point a whole run at throwaway schemas.

## Landing

One `ingest` notebook, one landing zone, **plain CSV for both engines**, plus the archive
log `csv_raw_archive_log.parquet` that both engines' `stg_csv_archive_log` reads. If the
engines read different bytes, comparing their output means nothing.

Landing is a prerequisite, not a modelling step, so it is a plain Python notebook rather
than a dbt python model (dbt-fabric's python models are PySpark-via-Livy only). DuckDB is
used there as a library — listings, the archive log, normalising the DUID CSVs — and it
ships with the notebook. The files are written with obstore straight to OneLake, to
`abfss://…/dbt_landing/Files`, not through the lakehouse mount, so the same notebook lands
them from Fabric or from a laptop; obstore is pip-installed if the session lacks it. It is
idempotent: the archive
log is the watermark, so a re-run fetches only what it has not already landed.

## The three notebooks

`run_pipeline` orders them: `ingest`, then `run` once per engine, then `parity`.

| notebook | parameters | what it does |
|---|---|---|
| `ingest` | none | lands the AEMO files in `dbt_landing` |
| `run` | `engine` | copies the dbt project, installs the adapter, connects, `dbt build` |
| `parity` | none | reads both engines' gold tables from OneLake with DuckDB, and fails the run if they differ |

Each holds its own code and calls no script; open one to see what the step does. All three
have `dbt_landing` as their default lakehouse. `run` reads the dbt project from `dbt_landing/Files/project/`. A deploy
from CI uploads it there; after a Jumpstart install, `ingest` downloads it from GitHub once,
at `repo_ref`, and again only if `repo_ref` changes or the folder is deleted.

## What it creates in Fabric

Three items hold the data, all inside one workspace folder, `fabric-medallion-dbt`, created
by the install:

```
fabric-medallion-dbt/
├── dbt_landing   Lakehouse  csv_raw/ + the archive log. Written ONCE, read by both engines.
├── dbt           Lakehouse  spark_landing, spark_mart; and Files/landing, a shortcut to
│                            dbt_landing that the Warehouse reads through
└── dbt_dwh       Warehouse  dwh_landing, dwh_mart
```

The Warehouse cannot share the lakehouse: a Warehouse cannot hold Delta tables another engine
wrote. That is an engine-forced floor, not a layout choice. The `run` notebook looks the
items up by name and hands the dbt profile what it reads: the warehouse's server, the
lakehouse's id, the path of the landed files.

Two landing variables, and the difference matters:

| var | meaning |
|---|---|
| `LANDING_PATH` | where the `ingest` notebook writes. **Identical on both legs**: `dbt_landing/Files` |
| `FILES_PATH` | how *this engine's* dbt reads that zone — the same path for spark; dwh reads through the `Files/landing` shortcut, because a Warehouse has no `Files` section |

They were one variable once, and re-pointing it for dwh made that leg download its own
private copy of the CSVs — parity was then grading the engines on different inputs. Never
re-emit `FILES_PATH` to move an engine's data; give it its own key.

## Isolating a run

Schemas are always `<engine>_<layer>`, and `DBT_SCHEMA` prefixes on top of that:

| `DBT_SCHEMA` | staging | marts |
|---|---|---|
| unset / `mart` | `dwh_landing` | `dwh_mart` |
| `test` | `test_dwh_landing` | `test_dwh_mart` |

Both engines go through `generate_schema_name()`, so one env var moves a whole run out of the
way. The engine half is not cosmetic — spark writes into the shared `dbt` lakehouse, and two
targets resolving to the same schema would overwrite each other's gold layer with every test
still green. `check_gating.py` asserts the prefix offline.

## Serving

`fabric_items/` holds one Direct Lake semantic model per engine, `aemo_dwh` and
`aemo_spark`, over `<engine>_mart`. They are the same `model.bim`;
`fabric_items/parameter.yml` binds each to its engine's item and schema when they are
installed, by Microsoft Fabric Jumpstart or by the `deploy` workflow (see the
[README](../README.md)).
