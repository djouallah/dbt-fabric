# `history/` — what each run agreed on, touched, wrote and cost

Three documents, all written by CI and committed back to this repo. Ported from
[`direct-lake-parquet-layout`](https://github.com/djouallah/direct-lake-parquet-layout) and
adapted to one thing that repo does not have: **persistent, shared Fabric items**.

| file | written by | what |
|---|---|---|
| `parity/<engine>.json` | `pipeline.yml` → `record` job, when the comparison passes | the latest fingerprint per engine ([parity/README.md](parity/README.md)) |
| `runs/<UTC ts>-<run id>.json` | `pipeline.yml` → `record` job, every run | every Fabric item GUID the run touched, each leg's compute window, the layout read back, this run's fingerprints |
| `cu.json` | `capacity.yml` (`Capacity units`), after every run and daily | **compute** capacity units and seconds per run and engine, cumulative |

`runs/` and `cu.json` are joined on the run id: a run's legs name their compute items and the
hours they ran, and the ledger holds what those items cost in those hours.

## The run record

```json
{"schema": 1,
 "run":    {"id": "35182005083", "sha": "...", "started": "...Z", "finished": "...Z", "url": "..."},
 "inputs": {"engines": "all", "dbt_schema": "mart", "process_limit": "1000", "...": "..."},
 "items":  {"<GUID>": {"role": "data", "kind": "Lakehouse", "name": "dbt"},
            "<GUID>": {"role": "warehouse", "kind": "Warehouse", "name": "dbt_dwh", "engine": "dwh"},
            "<GUID>": {"role": "compute", "kind": "Notebook", "name": "dbt-duckrun-35182005083", "engine": "duckrun"}},
 "legs":   {"duckrun": {"started": "...Z", "finished": "...Z", "outcome": "success", "compute": ["<notebook GUID>"]},
            "dwh":     {"started": "...Z", "finished": "...Z", "outcome": "success", "compute": ["<warehouse GUID>"]},
            "spark":   {"started": "...Z", "finished": "...Z", "outcome": "success", "compute": ["<dbt lakehouse GUID>"]}},
 "layout": {"stats":     {"<engine>": {"<table>": {"total_rows": 1, "num_files": 1, "num_row_groups": 1, "avg_row_group": 1, "size_mb": 1, "vorder": false, "compression": "SNAPPY"}}},
            "encodings": {"<engine>": {"<column>": {"encodings": ["PLAIN", "RLE_DICTIONARY"], "type": "INT32", "dict_pages": 1, "chunks": 1, "mb": 1}}},
            "ordering":  {"<engine>": {"columns": {"<column>": {"rg_overlap_pct": 0, "rgs": 1, "runs": 1}}, "sample": {}, "vorder_files": {}}}},
 "parity": {"<engine>": {"rows_total": 1, "mw_sum": 1, "...": "..."}}}
```

- `role` is a closed vocabulary: `landing` · `data` (the shared `dbt` lakehouse) · `warehouse`
  · `catalog` (ducklake's SQL DB) · `folder` · `compute` (a throwaway notebook).
- **`legs.<engine>.compute` are the items whose compute operations belong to that leg**, and
  `started` / `finished` bracket build + tests + fingerprint. Measured against the live model
  on 2026-09-17 (workspace `sqlengines`, capacity `CAT_Premium_Europe`):

  | engine | `compute` | operation, as the metrics app names it |
  |---|---|---|
  | duckrun, iceberg | the throwaway notebook `dbt-<engine>-<run id>` | `Jupyter Notebook Scheduled Run` |
  | ducklake | that notebook **and** `dbt_ducklake_meta` (the catalog SQL DB — `Sql Usage`, 19.5k CU in three days, nothing else queries it) | `Jupyter Notebook Scheduled Run`, `Sql Usage` |
  | dwh | `dbt_dwh` (Warehouse) | `Warehouse Query` |
  | spark | `dbt` (the shared Lakehouse — a Livy session bills against the lakehouse it was opened on, and only the spark leg opens one there) | `High Concurrency Session Livy Run` |

  Deliberately **not** attributed to any leg: the demo notebook `run` and its pipeline
  (`Jupyter Notebook Pipeline Run`, `ActivityRun`), the deployed semantic models (`Query`,
  refreshes), and every `OneLake …` storage operation.
- Every job writes a *fragment* (`record.py`, `RUN_RECORD` env) and the `record` job merges
  them by basename order. `items` and `legs` are dicts because the merge unions dicts and
  replaces lists.
- `layout` is best-effort per engine: an engine that could not be read is absent, never `{}`.
  Whether `iceberg` appears tells you whether OneLake virtualised the REST catalog's tables as
  Delta for that run.
- `parity` holds the fingerprints downloaded from *this* run's legs — never `history/parity/`,
  which after the first commit also holds the previous run's.

## The ledger — `cu.json`

```json
{"schema": 1, "updated": "...Z",
 "reads": [{"at": "...Z", "since": "<model clock>", "runs": 3, "changed": 12, "timed": 15, "pending": 0}],
 "runs": {"35182005083": {"started": "...Z", "engines": {
     "duckrun": {"cu": {"Notebook run": 4120.5}, "seconds": {"Notebook run": 1830.2},
                 "window": ["2026-09-17T03:20:00Z", "2026-09-17T03:51:00Z"]},
     "dwh":     {"cu": {"Warehouse Query": 8016.0}, "seconds": {"Warehouse Query": 1204.5}, "window": ["...Z", "...Z"]},
     "spark":   {"cu": {"High Concurrency Session Livy Run": 24865.3}, "seconds": {"High Concurrency Session Livy Run": 2100.0},
                 "window": ["...Z", "...Z"], "partial": true}}}}}
```

**Compute only, on purpose.** Every `OneLake …` operation is a storage transaction, and on a
lakehouse shared by five engines the storage in any window is everybody's — there is nothing
to attribute. The operation grain is kept so the compute is readable by name; storage never
enters the ledger.

**How a leg's number is made.** `measure_cu.py` asks the Capacity Metrics model (table
`Metrics By Item Operation And Hour`, DAX over `executeQueries`) for CU and duration per
(item, operation, hour) for the items the run records name, then sums each leg's items over
the model-clock hours its window touches. That is the change from the source repo, where every
item was deleted at the end of its run and a GUID's cumulative CU *was* the run's.

**Three rules, none of which needs any state** (unchanged from the source):

- only runs the read returned are touched — one past the app's ~14-day retention keeps its value;
- `max(old, new)` per (run, engine, operation), never a blind overwrite and never `+`: a sum over
  a fixed window only ever grows as ingestion catches up, so re-reading is idempotent and an
  undercounted first read self-corrects;
- the floor is the earliest recorded leg start, clamped to retention.

**A run measured minutes after it finished is a LOWER BOUND** — ~6 min ingestion lag, then
5–64 min of smoothing — and the step summary says `may still rise` for ~70 minutes. The
`workflow_run` read populates the row immediately; the daily 13:17 UTC read settles it. A run
you want settled sooner: dispatch `Capacity units` by hand (`gh workflow run "Capacity units"`).

**What the window cannot do, recorded rather than fixed:**

- two runs less than an hour apart share an hour on dwh and spark, and both get that hour's
  whole CU (the notebook legs are per-run items and immune);
- a Livy session idling past leg-end bills into the next hour and is missed (spark is a lower
  bound);
- a leg that died before `leg-end` (cancelled job, dead runner) has no `finished`; the ledger
  assumes `CU_FALLBACK_HOURS` (2) and marks the entry `partial`. Cancelling a GitHub job never
  stops Fabric.

## Env and secrets

| var | | |
|---|---|---|
| `CU_METRICS_WORKSPACE_ID` / `CU_METRICS_MODEL_ID` | secret | the Capacity Metrics app's workspace and semantic model |
| `CU_CAPACITY_ID` | secret | pin it; unpinned costs an extra query plus a full read per capacity |
| `CU_WORKSPACE_FILTER` | = `FABRIC_WORKSPACE_ID` | the only row filter, a column of the fact table itself |
| `PBI_TOKEN` | optional | by-hand escape hatch; CI lets duckrun mint it from the OIDC login |
| `CU_SINCE` | optional | override the floor, **in the model's clock** |
| `CU_MODEL_OFFSET_HOURS` | `10` | the app's own UTC offset; a wrong value reads as "no activity", not as an error |
| `CU_RETENTION_DAYS` / `CU_FALLBACK_HOURS` | `14` / `2` | |
| `CU_RUNS_DIR` / `CU_LEDGER` | `history/runs` / `history/cu.json` | point a by-hand read at a copy |

Every real GUID is a secret: no tracked file holds one and `measure_cu.py` keeps no fallback.

## Tests

`python -m pytest tests_py/ -q` — offline, no token, seconds. `test_record.py` pins the merge
(dicts not lists, basename order, no-op without `RUN_RECORD`, stale fingerprints never folded);
`test_measure_cu.py` pins the ledger rules and the attribution (a leg gets only its items, only
its hours, never storage); `test_layout.py` pins the parquet-metadata aggregations.
