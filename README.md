# fabric-medallion-dbt

A medallion architecture on Microsoft Fabric: one dbt project that builds the **same AEMO gold layer** on Fabric Warehouse or Fabric Spark.

![Medallion architecture on Microsoft Fabric. Ingest with any tool (PySpark notebook, Dataflow Gen2, Python notebook) into Bronze: raw files and Delta tables in OneLake, as ingested. Transform in SQL with dbt into Silver (typed, deduplicated, conformed) and Gold (facts and dimensions, business-ready), on either Fabric Spark or Fabric Warehouse, the same SQL models on either engine. Serve Power BI semantic models with Direct Lake (reads Delta in OneLake, no data copy) or DirectQuery (live queries on the SQL endpoint). The product: well-built Delta tables that Direct Lake and DirectQuery read fast.](docs/medallion-fabric-dbt.svg)

## Supported targets

| target | adapter | engine | writes |
|---|---|---|---|
| `dwh` | [`dbt-fabric`](https://github.com/microsoft/dbt-fabric) | Fabric Warehouse | Delta tables in the Warehouse |
| `spark` | [`dbt-fabricspark`](https://github.com/microsoft/dbt-fabricspark) | Fabric Spark (Livy) | Delta tables in a Fabric Lakehouse |

Both adapters are maintained by Microsoft. The eight models compute the same numbers on
both, and every pipeline run fingerprints the gold table on each engine and compares them.

A version of this project that also runs on community adapters, with a comparison across
more engines, is at
[fabric-medallion-dbt-community](https://github.com/djouallah/fabric-medallion-dbt-community).

## Install it in Fabric

In a Fabric notebook, in the workspace you want it in:

```python
!pip install -q fabric-jumpstart
import fabric_jumpstart as jumpstart
jumpstart._install_from_github(
    logical_id="fabric-medallion-dbt",
    repo_url="https://github.com/djouallah/fabric-medallion-dbt",
    repo_ref="main",
    entry_point="run_pipeline.DataPipeline",
    items_in_scope=["VariableLibrary", "Lakehouse", "Warehouse", "Notebook", "SemanticModel",
                    "DataPipeline"],
)
```

That creates the two lakehouses, the warehouse, the `deploy_config` variable library, the
`run` notebook, the `run_pipeline` pipeline and a Direct Lake semantic model per engine
(`aemo_dwh`, `aemo_spark`). Open `run_pipeline` and click **Run**, or give
it a schedule: it lands the AEMO files once, builds both engines in parallel, and fails if
their gold tables disagree. `deploy_config` holds the settings (`engines`: `all`, `dwh` or
`spark`; the download and process limits).

## Docs

[Overview](docs/overview.md), how to [run it](docs/run.md), and the rest in
[docs/](docs/README.md). The published dbt docs (lineage, models, columns, tests) are at
<https://djouallah.github.io/fabric-medallion-dbt/>.

## License

MIT
