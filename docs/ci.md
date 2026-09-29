# CI

What the workflows do, and why only `ci.yml` runs on push. None of them runs the pipeline:
the run is `run_pipeline`, in the Fabric workspace.

- `ci.yml` — free and credential-less: pytest, plus `check_gating.py` as a two-way matrix
  (one environment per engine). Runs on every push. The matrix cannot be collapsed into one
  job: `dbt-fabric` and `dbt-fabricspark` shadow each other under `dbt.adapters`.
- `install.yml` and `deploy.yml` — the two installs, below. Manual only: they create Fabric
  items and decide what a workspace runs.
- `docs.yml` — after every deploy, and by hand: `dbt docs generate --static --no-compile
  --empty-catalog --target dwh`, and deploys the one self-contained page to GitHub Pages —
  **[the DAG and the model docs](https://djouallah.github.io/fabric-medallion-dbt/)**. It
  builds nothing, spends no Fabric compute and needs no credentials: the profile gets dummy
  values and nothing is contacted. What is published is the lineage and every model, column
  and test description; the catalog is left empty, because generating it would open a real
  connection.

`tests_py/test_fabric_items.py` pins that only `ci.yml` runs on push.

## Installing into Fabric

Two manual workflows, one per install in the [README](../README.md). Each fails if an item
did not land, and each installs and stops; the run is `run_pipeline`, in the workspace. They
share a concurrency group, because they write the same items.

- `install.yml`, the demo install: Microsoft Fabric Jumpstart, which clones the repo from
  GitHub. The `run` notebook then downloads the project from GitHub on every run. Both need
  the repo to be public.
- `deploy.yml`, the production install: `deploy.py` publishes the items from the checkout
  with `fabric-cicd`, which is what Jumpstart installs with, and uploads the project to
  `dbt_landing/Files/project/`, file by file, with a `COMMIT` file naming the commit. It
  publishes as the environment `production`, which activates the `deploy_config` value set
  of that name and so sets `project_source` to `onelake`: the `run` notebook then copies the
  project from the lakehouse. Nothing is fetched from GitHub, so it works from a private
  repo, and the login is OIDC, with no secret.

What is uploaded is the dbt project, not the repo: `dbt_project.yml`, `profiles.yml`,
`models/`, `macros/`, `tests/`, plus the two things the `run` notebook needs beside them,
`requirements/` to install dbt from and `provision.py` to find the warehouse and lakehouse.

The upload is of the commit, not the working tree, so a deploy from a laptop and one from CI
leave the same files. The folder is deleted first: a model removed from the repo must not
survive in the lakehouse.

Do not deploy while `run_pipeline` is running: the two engines would build from two commits.
