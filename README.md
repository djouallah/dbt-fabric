# dbt-fabric — the same business logic, on any dbt adapter

One dbt project that builds the **same AEMO gold layer** on five adapters:

![Bronze, silver, gold - and the engine is a variable. Bronze is files, not tables: download_aemo.py, plain python, pulls the nemweb zips and lands plain CSV plus an archive-log parquet, once, the same bytes for every engine. Silver is <engine>_landing, in dbt SQL: stg_csv_archive_log is a view on that log saying which files are new; fct_price and fct_scada type and dedupe the daily archive keeping all 130 and all 53 columns; fct_price_today and fct_scada_today do the same for the intraday feed, still in AEMO's own shape. Gold is <engine>_mart, in dbt SQL: fct_summary, one row per (date, time, DUID) joining generation to the price of its region and merged key by key, beside the dimensions it is read through, dim_duid and dim_calendar - Direct Lake reads all three in place, no copy. Underneath silver and gold sits the only thing that changes, dbt build --target <engine>: five model trees of the same SQL (at least in spirit) where the flag enables one, running on duckrun (DuckDB to Delta via delta-rs), iceberg (DuckDB plus the iceberg extension, to an Iceberg REST catalog), ducklake (DuckDB plus a SQL catalog, parquet with Delta metadata exported over it), dwh (Fabric Warehouse, Delta) or spark (Fabric Spark, Delta in a Lakehouse).](docs/how-it-works-dark.svg)

| target | adapter | engine | shape | writes |
|---|---|---|---|---|
| `dwh` | `dbt-fabric` | Fabric Warehouse | distributed | Delta tables in the Warehouse |
| `spark` | `dbt-fabricspark` | Fabric Spark | distributed | Delta in a Fabric Lakehouse |
| `duckrun` | `duckrun` | DuckDB | single node | Delta Lake on OneLake, via delta-rs |
| `iceberg` | `dbt-duckdb` | DuckDB | single node | Iceberg, through the OneLake Iceberg REST catalog |
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

## Docs

| | |
|---|---|
| [Run it](docs/run.md) | the local loop, and the offline checks worth running first |
| [Layout and gating](docs/layout.md) | the trees, the eight models, how one target selects one engine |
| [Engine nuances](docs/engine-nuances.md) | everything that is *not* the same on the five, and why |
| [On Fabric](docs/fabric.md) | landing, the items a run creates, isolating a run, scheduling |
| [CI and cost](docs/ci.md) | the workflows, the run record, capacity units |
| [Candidate engines](docs/candidate-engines.md) | Sail and Polars: what stopped a sixth leg |

## License

MIT
