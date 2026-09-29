# CI, and what each run cost

What the four workflows do and why only `ci.yml` runs on push, plus the run record: the
Fabric items each leg touched, what it wrote, and what it cost in capacity units.

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
- `capacity.yml` — fires after every pipeline run and once a day: reads capacity units per
  run and engine from the Fabric Capacity Metrics model (token via azure-identity after
  `azure/login`) and commits `history/cu.json`.
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

## What it cost, and what it wrote

Ported from [`direct-lake-parquet-layout`](https://github.com/djouallah/direct-lake-parquet-layout)
and adapted to shared, persistent items. Every pipeline run leaves one record in
`history/runs/` — the Fabric item GUIDs it touched, each leg's compute window, the parquet
layout of both engines' tables (files, row groups, `fct_summary`'s per-column encodings and
physical row order) and the parity fingerprints — and the `Capacity units` workflow keeps
`history/cu.json`: **compute** capacity units per run and engine, read from the Capacity
Metrics model for each leg's items inside its own hours. Storage transactions are
deliberately not attributed: the lakehouse is shared, so its OneLake operations in any window
belong to everybody. [`history/README.md`](../history/README.md) has the schemas and the
caveats.

Note: cancelling a GitHub job does **not** stop Fabric — a Warehouse query or Livy session
keeps running, and billing.

## Deploying the semantic models

Coming next: the Direct Lake semantic models in `semantic_model/` will be deployed through
Microsoft Fabric Jumpstart (fabric-jumpstart / fabric-cicd). There is no deploy job today.
