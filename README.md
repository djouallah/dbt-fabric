# fabric-medallion-dbt

A medallion architecture on Microsoft Fabric: one dbt project that builds the **same AEMO gold layer** on Fabric Warehouse or Fabric Spark.

![Medallion architecture on Microsoft Fabric. Ingest with any tool (PySpark notebook, Dataflow Gen2, Python notebook) into Bronze: raw files and Delta tables in OneLake, as ingested. Transform in SQL with dbt into Silver (typed, deduplicated, conformed) and Gold (facts and dimensions, business-ready), on either Fabric Spark or Fabric Warehouse, the same SQL models on either engine. Serve Power BI semantic models with Direct Lake (reads Delta in OneLake, no data copy) or DirectQuery (live queries on the SQL endpoint). The product: well-built Delta tables that Direct Lake and DirectQuery read fast.](docs/medallion-fabric-dbt.svg)

## Supported targets

| target | adapter | engine | writes |
|---|---|---|---|
| `dwh` | [`dbt-fabric`](https://github.com/microsoft/dbt-fabric) | Fabric Warehouse | Delta tables in the Warehouse |
| `spark` | [`dbt-fabricspark`](https://github.com/microsoft/dbt-fabricspark) | Fabric Spark (Livy) | Delta tables in a Fabric Lakehouse |

Both adapters are maintained by Microsoft. The eight models compute the same numbers on
both, and every pipeline run reads both engines' gold tables and compares them.

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
on the first run `ingest` downloads the dbt project from it, once, into
`dbt_landing/Files/project/`. Delete that folder to download it again. The entry for the
Jumpstart catalog, which would shorten this to `jumpstart.install("fabric-medallion-dbt")`,
is prepared in [jumpstart/](jumpstart/README.md).

## Production: develop in VS Code, deploy from CI, run in Fabric

For your own copy of the repo, private or not. GitHub is the only source of truth, and
[`fabric-cicd`](https://microsoft.github.io/fabric-cicd) publishes from it into two
workspaces, **DEV** and **PROD**, holding the same items. Nobody edits a workspace by hand,
and nothing is fetched from GitHub at run time.

| stage | where | what runs |
|---|---|---|
| develop | VS Code, against DEV | `dbt build` for each engine, as you (`az login`) |
| check | GitHub, on every push | `ci`: the offline tests and the gating check, both engines |
| deploy to DEV | `deploy` workflow, `dev` | publishes the items and the dbt project to DEV; run `run_pipeline` there to test a notebook or pipeline change end to end, parity included |
| release | `deploy` workflow, `production` | the same, into PROD, after a reviewer approves |
| run | PROD | `run_pipeline`, on a schedule |

### Set it up once

1. Copy this repo into your own. Create the DEV and PROD workspaces.
2. Create a service principal (an app registration). Make it a Contributor on both
   workspaces, and have a Fabric admin allow "Service principals can use Fabric APIs".
3. In the repo's settings, create two **Environments**, `dev` and `production`. Give each the
   secret `FABRIC_WORKSPACE_ID` (its workspace) and add `AZURE_CLIENT_ID` and
   `AZURE_TENANT_ID` as repository secrets. On `production`, turn on **Required reviewers**.
4. Give the service principal one federated credential per environment, entity type
   *Environment*, `dev` and `production`. There is no password to store or rotate: the login
   is OpenID Connect.
5. Run the `deploy` workflow with `dev`, then run `run_pipeline` once in DEV: its `ingest`
   lands DEV's own copy of the AEMO files.

### Develop in VS Code

You need Python 3.12 and the [Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli).
`pip` is the whole install: `dbt-fabric` talks to the Warehouse with Microsoft's
`mssql-python`, so there is no ODBC driver to install. The two adapters cannot share an
environment, so each engine gets its own venv:

```bash
python -m venv .venv-dwh   && .venv-dwh/Scripts/pip install -r requirements/dwh.txt -r requirements/dev.txt
python -m venv .venv-spark && .venv-spark/Scripts/pip install -r requirements/spark.txt -r requirements/dev.txt
cp .env.example .env       # set FABRIC_WORKSPACE_ID to the DEV workspace
az login
```

(`bin/` instead of `Scripts/` on Linux and macOS.) Then build either engine against DEV,
with any dbt arguments:

```bash
.venv-dwh/Scripts/python   .github/scripts/dev.py dwh   build
.venv-spark/Scripts/python .github/scripts/dev.py spark build --select fct_summary
```

`dev.py` looks up DEV's items, sets what the `run` notebook sets in Fabric, with the
settings in `deploy_config`, and runs dbt as you. VS Code's **Run Task** has the two builds,
the tests and the gating check. A change to a model is a change to both engines' copies
(`models/aemo/dwh/`, `models/aemo/spark/`): build both before you push.

The notebooks run only in Fabric. Opened in VS Code on a local kernel, they stop at
`import notebookutils`, which exists only in the Fabric runtime. To edit one in VS Code, use
the Fabric Data Engineering extension and its Fabric runtime kernel, or change it in the
repo and deploy to DEV.

### Why not Fabric's dbt job

Fabric's own dbt job item is still in preview, and `fabric-cicd` cannot deploy it. It also
runs adapter versions of its own choosing, which are older than what this project pins. And
it cannot run the two engines and the parity check as one run.

### What differs from the demo install

| | demo (Jumpstart) | production (`deploy`) |
|---|---|---|
| the repo | public | public or private |
| items installed by | a notebook cell, from a GitHub clone | CI, from its checkout |
| `dbt_landing/Files/project/` is filled by | `ingest`, once, from GitHub at `repo_ref` | the deploy |
| a change reaches the workspace | at the next install of a newer tag | at the next deploy |

## Docs

[Overview](docs/overview.md), how to [run it](docs/run.md), and the rest in
[docs/](docs/README.md). The published dbt docs (lineage, models, columns, tests) are at
<https://djouallah.github.io/fabric-medallion-dbt/>.

## License

MIT
