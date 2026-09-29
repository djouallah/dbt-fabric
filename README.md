# fabric-medallion-dbt

A medallion architecture on Microsoft Fabric: one dbt project that builds the **same AEMO gold layer** on Fabric Warehouse or Fabric Spark.

![Medallion architecture on Microsoft Fabric. Ingest with any tool (PySpark notebook, Dataflow Gen2, Python notebook) into Bronze: raw files and Delta tables in OneLake, as ingested. Transform in SQL with dbt into Silver (typed, deduplicated, conformed) and Gold (facts and dimensions, business-ready), on either Fabric Spark or Fabric Warehouse, the same SQL models on either engine. Serve Power BI semantic models with Direct Lake (reads Delta in OneLake, no data copy) or DirectQuery (live queries on the SQL endpoint). The product: well-built Delta tables that Direct Lake and DirectQuery read fast.](docs/medallion-fabric-dbt.svg)

## Supported targets

| target | adapter | engine | writes |
|---|---|---|---|
| `dwh` | [`dbt-fabric`](https://github.com/microsoft/dbt-fabric) | Fabric Warehouse | Delta tables in the Warehouse |
| `spark` | [`dbt-fabricspark`](https://github.com/microsoft/dbt-fabricspark) | Fabric Spark | Delta in a Fabric Lakehouse |

Both adapters are maintained by Microsoft.

## Other adapters (community)

The same models also run on three community DuckDB adapters. They are not supported and are
included to show that the business logic is portable.

| target | adapter | writes |
|---|---|---|
| `duckrun` | [`duckrun`](https://github.com/djouallah/duckrun) | Delta Lake on OneLake, via delta-rs |
| `iceberg` | [`dbt-duckdb`](https://github.com/duckdb/dbt-duckdb) | Iceberg, through the OneLake Iceberg REST catalog |
| `ducklake` | [`dbt-duckdb`](https://github.com/duckdb/dbt-duckdb) | DuckLake parquet + a Delta export, catalog in a Fabric SQL DB |

## Docs

[The thesis](docs/thesis.md), how to [run it](docs/run.md), and the rest in [docs/](docs/README.md).

## License

MIT
