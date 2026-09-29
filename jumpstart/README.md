# The Fabric Jumpstart catalog entry

This folder holds what the catalog needs to list this repo, so that
`jumpstart.install("fabric-medallion-dbt")` works. It is ready to submit, and nothing here
has been submitted yet. Nothing in this folder is installed into a workspace.

| here | goes to, in a fork of `microsoft/fabric-jumpstart` |
|---|---|
| `fabric-medallion-dbt.yml` | `src/fabric_jumpstart/fabric_jumpstart/jumpstarts/community/` |
| `fabric-medallion-dbt_light.svg`, `_dark.svg` (TODO) | `assets/images/diagrams/` |
| `content/fabric-medallion-dbt/` | `src/fabric_jumpstart_web/content/scenarios/` |

`tests_py/test_jumpstart_entry.py` pins the entry to `install_jumpstart.INSTALL`, and to the
rules of the catalog's own schema.

## Before submitting

- **The diagram SVGs.** Paste the entry's `mermaid_diagram` into
  <https://jumpstart.fabric.microsoft.com/tools/diagram-generator>. Save the light and dark
  renders here as `fabric-medallion-dbt_light.svg` and `fabric-medallion-dbt_dark.svg`.
- **`id`**: the next free one in `jumpstarts/` at the time. 30 was free on 2026-09-29.
- **`date_added` and `last_updated`**: the day of the PR.
- **`minutes_to_deploy` and `minutes_to_complete_jumpstart`**: these are estimates. Take them from a real install and a first run.
- **The PR** is titled `feat: add fabric-medallion-dbt jumpstart`. It closes the issue below. Then run
  the install snippet the bot comments and reply that it worked.

## A release is a tag, and the tag must point at itself

The catalog installs at `repo_ref`, and it must be a tag. The `run` notebook downloads the
dbt project from GitHub at `deploy_config`'s `repo_ref`, which is `main` on `main`. Tag `main`
as it is, and an install of `v1.0.0` would build whatever is on `main` today. So the tag is a
commit off `main` that points `repo_ref` at the tag:

```bash
git switch --detach main
# fabric_items/deploy_config.VariableLibrary/variables.json: "repo_ref" -> "v1.0.1"
git commit -am "Release v1.0.1"
git tag v1.0.1 && git push origin v1.0.1
git switch main
```

Then set the entry's `source.repo_ref` to the new tag. `main` keeps `repo_ref: main`, so
`install.yml` still installs a branch that builds itself.

## The "New Jumpstart" issue

- **Title:** fabric-medallion-dbt: medallion with dbt on Fabric Warehouse and Fabric Spark
- **Type:** Demo
- **Workloads:** Data Engineering, Data Warehouse, Data Factory, Power BI
- **Summary:** One dbt project builds the same gold layer of public AEMO electricity data on
  Fabric Warehouse (`dbt-fabric`) and Fabric Spark (`dbt-fabricspark`). A pipeline lands the
  files, builds both engines in parallel and fails if their gold tables disagree. A Direct
  Lake semantic model sits on each engine.
- **Motivation:** dbt is how many teams build a medallion. This shows it end to end on
  Fabric with Microsoft's own adapters only: install, run, serve. It also shows how to write
  one model set that holds on two engines. The parity check caught real dialect differences
  that every dbt test missed.
- **Audience:** data engineers who use dbt, or are choosing between Warehouse and Spark on
  Fabric.
- **Dependencies:** a Fabric capacity. The data is public (AEMO nemweb and a GitHub archive),
  and dbt is pip-installed by the `run` notebook at run time.
