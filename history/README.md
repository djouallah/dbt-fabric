# `history/` — what each run agreed on and wrote

Two documents, both written by CI and committed back to this repo. Ported from
[`direct-lake-parquet-layout`](https://github.com/djouallah/direct-lake-parquet-layout).

| file | written by | what |
|---|---|---|
| `parity/<engine>.json` | `pipeline.yml` → `record` job, when the comparison passes | the latest fingerprint per engine ([parity/README.md](parity/README.md)) |
| `runs/<UTC ts>-<run id>.json` | `pipeline.yml` → `record` job, every run | the run stamp and inputs, the parquet layout read back, this run's fingerprints |

## The run record

```json
{"schema": 1,
 "run":    {"id": "35182005083", "sha": "...", "started": "...Z", "finished": "...Z", "url": "..."},
 "inputs": {"engines": "all", "dbt_schema": "mart", "process_limit": "1000", "...": "..."},
 "layout": {"stats":     {"<engine>": {"<table>": {"total_rows": 1, "num_files": 1, "num_row_groups": 1, "avg_row_group": 1, "size_mb": 1, "vorder": false, "compression": "SNAPPY"}}},
            "encodings": {"<engine>": {"<column>": {"encodings": ["PLAIN", "RLE_DICTIONARY"], "type": "INT32", "dict_pages": 1, "chunks": 1, "mb": 1}}},
            "ordering":  {"<engine>": {"columns": {"<column>": {"rg_overlap_pct": 0, "rgs": 1, "runs": 1}}, "sample": {}, "vorder_files": {}}}},
 "parity": {"<engine>": {"rows_total": 1, "mw_sum": 1, "...": "..."}}}
```

- `layout` is what each engine WROTE, read back by `.github/scripts/layout.py` from the tables'
  Delta logs and parquet footers: rows, files, row groups, size and compression per table, and
  for `fct_summary` — the table Power BI reads through Direct Lake — the per-column encodings
  (dictionary or PLAIN) and the physical row order (row-group overlap, run lengths, Spark's
  per-file V-Order tag). It is best-effort per engine: an engine that could not be read is
  absent, never `{}`.
- `parity` holds the fingerprints downloaded from *this* run's legs — never `history/parity/`,
  which after the first commit also holds the previous run's.
- Every job writes a *fragment* (`record.py`, `RUN_RECORD` env) and the `record` job merges
  them by basename order. Everything is keyed by name because the merge unions dicts and
  replaces lists.

## Tests

`python -m pytest tests_py/ -q` — offline, no token, seconds. `test_record.py` pins the merge
(basename order, no-op without `RUN_RECORD`, stale fingerprints never folded);
`test_layout.py` pins the parquet-metadata aggregations.
