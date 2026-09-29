# CI

What the three workflows do and why only `ci.yml` runs on push, plus the run record: what
each engine wrote.

- `ci.yml` — free and credential-less: pytest, plus `check_gating.py` as a two-way matrix
  (one environment per engine). Runs on every push. The matrix cannot be collapsed into one
  job: `dbt-fabric` and `dbt-fabricspark` shadow each other under `dbt.adapters`.
- `pipeline.yml` — manual only, and the whole pipeline in one file:
  `checks` (ci.yml) → `plan` → `land` → `build` (matrix: dwh, spark) → `layout` → `record`.
  `land` provisions the shared items and runs `download_aemo.py` ONCE; the build legs then
  run in parallel and never land, because the downloader rewrites the archive log in place
  and concurrent legs would race on that one file. Each leg provisions its own items, does
  `dbt build` (models and tests, then `dbt retry` on failure) and fingerprints its gold
  table. dbt runs on the runner as a client; the compute is the Warehouse or a Livy session.
  The **layout** job reads both engines' tables back, and the **record** job compares the
  fingerprints and commits one run record to `history/runs/`. Every job authenticates with
  `azure/login` (OIDC). `process_limit` is a dispatch input: files each fact model folds per
  run, **newest first**, on both engines — so a partial load is recent data, and the backlog
  shows up as test WARNINGS that fall run by run rather than as a failed build.
- `docs.yml` — fires after every pipeline run: `dbt docs generate --static --no-compile
  --empty-catalog --target dwh` and deploys the one self-contained page to GitHub Pages —
  **[the DAG and the model docs](https://djouallah.github.io/fabric-medallion-dbt/)**. It
  builds nothing, spends no Fabric compute and needs no credentials: the profile gets dummy
  values and nothing is contacted. What is published is the lineage and every model, column
  and test description; the catalog is left empty, because generating it would open a real
  connection. Row counts and sizes live in the run record's `layout` instead.

`pipeline.yml` is manual because provisioning Fabric items and spending capacity is a
deliberate act, and because the record job commits to `history/`, so a push trigger would
make the commit start the next run. `tests_py/test_parity_record.py` pins the trigger set.

## What it wrote

Ported from [`direct-lake-parquet-layout`](https://github.com/djouallah/direct-lake-parquet-layout).
Every pipeline run leaves one record in `history/runs/` — the run's inputs, the parquet layout
of both engines' tables (files, row groups, `fct_summary`'s per-column encodings and physical
row order) and the parity fingerprints. [`history/README.md`](../history/README.md) has the
schema.

Note: cancelling a GitHub job does **not** stop Fabric — a Warehouse query or Livy session
keeps running, and billing.

## Installing into Fabric

`install.yml` (manual) installs `fabric_items/` into the test workspace with Microsoft
Fabric Jumpstart, the call in the [README](../README.md), and fails if an item did not
land. It installs and stops; the run is `run_pipeline`, in the workspace.
