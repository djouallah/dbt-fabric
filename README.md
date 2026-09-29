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
    workspace_path="fabric_items/",
    entry_point="run_pipeline.DataPipeline",
    items_in_scope=["VariableLibrary", "Lakehouse", "Warehouse", "Notebook", "SemanticModel",
                    "DataPipeline"],
)
```

That creates the two lakehouses, the warehouse, the `deploy_config` variable library, the
`run_pipeline` pipeline with its three notebooks and a Direct Lake semantic model per engine
(`aemo_dwh`, `aemo_spark`). Open `run_pipeline` and click **Run**, or give it a schedule:

| step | notebook | what it does |
|---|---|---|
| 1 | `ingest` | lands the AEMO files, once |
| 2 | `run`, once per engine, in parallel | installs dbt and the adapter, `dbt build` |
| 3 | `parity` | fails the run if the two gold tables disagree |

Each notebook holds its own code, so opening one shows what the step does. `deploy_config` holds the settings (`engines`: `all`, `dwh` or
`spark`; the download and process limits).

This is the demo install, and it needs a public repo: Jumpstart clones it from GitHub, and
the `run` notebook downloads the dbt project from it on every run.

## Production: deploy from CI

For your own copy of the repo, private or not. The `deploy` workflow publishes the same items
from its checkout with [`fabric-cicd`](https://microsoft.github.io/fabric-cicd) and uploads
the dbt project to the `dbt_landing` lakehouse, as a folder you can open and read. Nothing
is fetched from GitHub, and there is no secret to store or rotate: the login is OpenID
Connect.

1. Copy this repo into your own.
2. Create a service principal (an app registration) with a federated credential for your
   repo and the branch you deploy from. Make it a Contributor on the workspace, and have a
   Fabric admin allow "Service principals can use Fabric APIs".
3. Add three repository secrets: `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `FABRIC_WORKSPACE_ID`.
   None of them is a password.
4. Run the `deploy` workflow.
5. In the workspace, run `run_pipeline` or give it a schedule.

What differs from the demo install:

| | demo (Jumpstart) | production (`deploy`) |
|---|---|---|
| the repo | public | public or private |
| items installed by | a notebook cell, from a GitHub clone | CI, from its checkout |
| `project_source` | `github` | `onelake` |
| `run` gets the dbt project from | GitHub, at `repo_ref` | `dbt_landing/Files/project/` |
| a change reaches the workspace | at the next run | at the next deploy |

## Docs

[Overview](docs/overview.md), how to [run it](docs/run.md), and the rest in
[docs/](docs/README.md). The published dbt docs (lineage, models, columns, tests) are at
<https://djouallah.github.io/fabric-medallion-dbt/>.

## License

MIT
