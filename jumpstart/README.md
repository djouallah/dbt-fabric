# The Fabric Jumpstart catalog entry

This folder holds what the catalog needs to list this repo, so that
`jumpstart.install("fabric-medallion-dbt")` works. It is ready to submit, and nothing here
has been submitted yet. Nothing in this folder is installed into a workspace.

| here | goes to, in a fork of `microsoft/fabric-jumpstart` |
|---|---|
| `fabric-medallion-dbt.yml` | `src/fabric_jumpstart/fabric_jumpstart/jumpstarts/community/` |
| `fabric-medallion-dbt_light.svg`, `_dark.svg` | `assets/images/diagrams/` |
| `content/fabric-medallion-dbt/` | `src/fabric_jumpstart_web/content/scenarios/` |

`tests_py/test_jumpstart_entry.py` pins the entry to `install_jumpstart.INSTALL`, and to the
rules of the catalog's own schema.

The two diagram SVGs are the repo's own architecture diagram,
`docs/medallion-fabric-dbt.svg`. It is an Excalidraw dark-mode export, and the dark SVG is
that file unchanged. The light SVG is the same file without the root `filter="invert(...)"`.
Re-make both when the diagram changes. The entry's `mermaid_diagram` is kept too, because the
catalog's CI asks for these two files only when an entry has one.

## Before submitting

- **`id`**: the next free one in `jumpstarts/` at the time. 30 was free on 2026-09-29.
- **`date_added` and `last_updated`**: the day of the PR.
- **`minutes_to_complete_jumpstart`** is 10, from a real first run on 2026-09-29.
  **`minutes_to_deploy`** (3) is still an estimate: time an install.
- **The PR** is titled `feat: add fabric-medallion-dbt jumpstart`. It closes the issue below. Then run
  the install snippet the bot comments and reply that it worked.

## A release is a tag, and the tag must point at itself

The catalog installs at `repo_ref`, and it must be a tag. On the first run `ingest`
downloads the dbt project from GitHub at `deploy_config`'s `repo_ref`, which is `main` on
`main`. Tag `main` as it is, and an install of `v1.0.0` would build whatever `main` holds on
its first run. So the tag is a commit off `main` that points `repo_ref` at the tag:

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
