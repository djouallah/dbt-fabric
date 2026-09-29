# fabric-medallion-dbt — the same business logic, on any dbt adapter

One dbt project that builds the **same AEMO gold layer** on five adapters:

![Bronze, silver, gold - and the engine is a variable. Bronze is files, not tables: download_aemo.py, plain python, pulls the nemweb zips and lands plain CSV plus an archive-log parquet, once, the same bytes for every engine. Silver is <engine>_landing, in dbt SQL: stg_csv_archive_log is a view on that log saying which files are new; fct_price and fct_scada type and dedupe the daily archive keeping all 130 and all 53 columns; fct_price_today and fct_scada_today do the same for the intraday feed, still in AEMO's own shape. Gold is <engine>_mart, in dbt SQL: fct_summary, one row per (date, time, DUID) joining generation to the price of its region and merged key by key, beside the dimensions it is read through, dim_duid and dim_calendar - Direct Lake reads all three in place, no copy. Underneath silver and gold sits the only thing that changes, dbt build --target <engine>: five model trees of the same SQL (at least in spirit) where the flag enables one, running on duckrun (DuckDB to Delta via delta-rs), iceberg (DuckDB plus the iceberg extension, to an Iceberg REST catalog), ducklake (DuckDB plus a SQL catalog, parquet with Delta metadata exported over it), dwh (Fabric Warehouse, Delta) or spark (Fabric Spark, Delta in a Lakehouse).](docs/how-it-works-dark.svg)

| target | adapter | engine | shape | writes | officially supported |
|---|---|---|---|---|---|
| `dwh` | [`dbt-fabric`](https://github.com/microsoft/dbt-fabric) | Fabric Warehouse | distributed | Delta tables in the Warehouse | **yes** |
| `spark` | [`dbt-fabricspark`](https://github.com/microsoft/dbt-fabricspark) | Fabric Spark | distributed | Delta in a Fabric Lakehouse | **yes** |
| `duckrun` | [`duckrun`](https://github.com/djouallah/duckrun) | DuckDB | single node | Delta Lake on OneLake, via delta-rs | no (community) |
| `iceberg` | [`dbt-duckdb`](https://github.com/duckdb/dbt-duckdb) | DuckDB | single node | Iceberg, through the OneLake Iceberg REST catalog | no (community) |
| `ducklake` | [`dbt-duckdb`](https://github.com/duckdb/dbt-duckdb) | DuckDB | single node | DuckLake parquet + a Delta export, catalog in a Fabric SQL DB | no (community) |

## Docs

[The thesis](docs/thesis.md), how to [run it](docs/run.md), and the rest in [docs/](docs/README.md).

## License

MIT
