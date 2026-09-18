# On Fabric — what it lands, creates and schedules

The Fabric side: where the raw AEMO data lands, the four items a run creates in one
workspace folder, how to point a whole run at throwaway schemas, and how to run the pipeline
from inside Fabric rather than from GitHub.

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
