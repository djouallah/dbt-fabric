# fabric-medallion-dbt — the same business logic, on any dbt adapter

One dbt project that builds the **same AEMO gold layer** on five adapters:

![Medallion architecture on Microsoft Fabric. Ingest with any tool (PySpark notebook, Dataflow Gen2, Python notebook) into Bronze: raw files and Delta tables in OneLake, as ingested. Transform in SQL with dbt into Silver (typed, deduplicated, conformed) and Gold (facts and dimensions, business-ready), on either Fabric Spark or Fabric Warehouse, the same SQL models on either engine. Serve Power BI semantic models with Direct Lake (reads Delta in OneLake, no data copy) or DirectQuery (live queries on the SQL endpoint). The product: well-built Delta tables that Direct Lake and DirectQuery read fast.](docs/medallion-fabric-dbt.svg)

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
