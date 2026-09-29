# On Fabric — what it lands and creates

The Fabric side: where the raw AEMO data lands, the items a run creates in one workspace
folder, and how to point a whole run at throwaway schemas.

## Landing

One `ingest/download_aemo.py`, one landing zone, **plain CSV for both engines**, plus the archive
log `csv_raw_archive_log.parquet` that both engines' `stg_csv_archive_log` reads. If the
engines read different bytes, comparing their output means nothing.

Landing is a prerequisite, not a modelling step, so it is a plain script rather than a dbt
python model (dbt-fabric's python models are PySpark-via-Livy only). DuckDB is used there as
a library — listings, the archive log, normalising the DUID CSVs, all on local temp files —
and the bytes move to OneLake through `ingest/onelake.py` (azure-identity +
azure-storage-file-datalake). It is idempotent: the archive log is the watermark, so a re-run
fetches only what it has not already landed.

## What it creates in Fabric

Three items, all inside one workspace folder (`FOLDER`, default `dbt`), created if missing
and kept if present by `.github/scripts/provision.py`:

```
dbt/
├── dbt_landing   Lakehouse  csv_raw/ + the archive log. Written ONCE, read by both engines.
├── dbt           Lakehouse  spark_landing, spark_mart; and Files/landing, a shortcut to
│                            dbt_landing that the Warehouse reads through
└── dbt_dwh       Warehouse  dwh_landing, dwh_mart
```

The Warehouse cannot share the lakehouse: a Warehouse cannot hold Delta tables another engine
wrote. That is an engine-forced floor, not a layout choice. `provision.py landing` creates
the shared items only (folder, `dbt_landing`, `dbt`); `provision.py dwh` / `spark` adds that
engine's and prints the env vars its profile reads.

Two landing variables, and the difference matters:

| var | meaning |
|---|---|
| `LANDING_PATH` | where `download_aemo.py` writes. **Identical on both legs**: `dbt_landing/Files` |
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

`semantic_model/` holds one Direct Lake semantic model over `<engine>_mart`. Deploying it —
and scheduling the pipeline from inside Fabric — is coming next, through Microsoft Fabric
Jumpstart (fabric-jumpstart / fabric-cicd).
