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
| develop | VS Code, against DEV | the `run` notebook for each engine, then `parity`, on a local kernel, as you (`az login`) |
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

All three notebooks run on your laptop as well as in Fabric: open them in VS Code on a
local Python kernel. In Fabric they are faster, because they run next to OneLake. Outside
Fabric there is no `notebookutils`, so each one works this way instead:

- **Settings:** read from `deploy_config`'s file in this repo.
- **dbt project:** this repo as it is, uncommitted edits included.
- **Tokens:** your Azure CLI login, for the Fabric API, the Warehouse, Livy and OneLake.
- **Landing:** `ingest` writes the files straight to OneLake with
  [obstore](https://developmentseed.org/obstore/), the same code as in Fabric. It installs
  obstore if the kernel does not have it.

You need Python 3.12 with `duckdb`, which `parity` uses, and the
[Azure CLI](https://learn.microsoft.com/cli/azure/install-azure-cli).

```bash
cp .env.example .env       # FABRIC_WORKSPACE_ID = the DEV workspace; VS Code passes it to the kernel
az login
```

1. Open `fabric_items/ingest.Notebook/notebook-content.ipynb` and run it. It lands the AEMO
   files in the workspace's `dbt_landing`.
2. Open `run`, set `engine = "dwh"` and run all cells. Then set `engine = "spark"` and run
   them again.
3. Open `parity` and run it.

On its first run for an engine, `run` creates a venv for it in the repo, `.venv-dwh` or
`.venv-spark` (the two adapters cannot share one), and pip-installs that engine's
`requirements/`. That is the whole install: `dbt-fabric` reaches the Warehouse through
Microsoft's `mssql-python`, with no ODBC driver. A change to a model is a change to both
engines' copies (`models/aemo/dwh/`, `models/aemo/spark/`), so build both before you push.

On a laptop, `ingest` leaves `dbt_landing/Files/project/` alone. That folder is what
Fabric's `run` builds, and on the laptop `run` builds your working tree instead.

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
