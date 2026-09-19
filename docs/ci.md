# CI, and what each run cost

What the four workflows do and why none of them but `ci.yml` runs on push, plus the run
record: the Fabric items each leg touched, what it wrote, and what it cost in capacity units.

- `ci.yml` — free and credential-less: pytest, plus `check_gating.py` as a five-way matrix
  (one environment per engine). Runs on every push. The matrix cannot be collapsed into one
  job: `dbt-fabric` and `dbt-fabricspark` shadow each other under `dbt.adapters`, and dbt OSS
  2 and `duckrun` both want to own the `dbt` console script.
- `pipeline.yml` — manual only, and the whole pipeline in one file: it lands ONCE in a shared
  `land` job, then runs all five engines in parallel as a matrix of `build` legs, then the
  **layout** job reads every engine's tables back and the **record** job compares the
  fingerprints and commits one run record to `history/runs/`. Each leg does `dbt build`
  (models and tests) → fingerprint. **duckrun, ducklake and iceberg run dbt on Fabric
  compute** — a throwaway Python notebook of 8 vCores through duckrun's `run_python`
  (`.github/scripts/remote_dbt.py`); tokens are minted inside Fabric by `notebookutils` and
  never travel. dwh and spark run dbt on the runner, where it is only a client of the
  Warehouse / Livy. The `local_runner` input (off by default) moves duckrun and iceberg onto the
  runner too — a verification mode, because the notebook is what injects their tokens, so an
  in-Fabric run cannot show whether the iceberg leg's credential vending works on an `az`-minted
  one. Pair it with a small `process_limit`: the runner has 7 GB, and being shut down
  mid-`fct_scada` is why these legs went to Fabric in the first place. ducklake opts out and
  always goes to Fabric — only `run_in_fabric.py` waits out its auto-paused catalog. A `compact` job folds the Iceberg catalog's small files afterwards, before
  `layout` measures them. `process_limit` is a dispatch input: files each fact model folds per
  run, oldest first, on every engine. `deploy` (`none` / `no_model` / `full`) deploys the
  in-Fabric demo after the build (next section).
- `capacity.yml` — fires after every pipeline run and once a day: reads capacity units per
  run and engine from the Fabric Capacity Metrics model and commits `history/cu.json`.
- `docs.yml` — fires after every pipeline run: `dbt docs generate --static` and deploys the
  one self-contained page to GitHub Pages — **[the DAG and the catalog](https://djouallah.github.io/dbt-fabric/)**.
  It builds nothing and spends no Fabric compute. **duckrun of the five**, because the DAG is
  the same on all of them and the *catalog* is not: duckrun reports `num_rows`, `bytes` and
  `last_modified` out of the Delta log, where dbt-fabric's catalog query gives an approximate
  row count and nothing else. It is also the only engine whose models guard their parse-time
  `run_query` on `flags.WHICH`, so generating its docs fires no query; on dwh the same command
  would run real `OPENROWSET` queries against the Warehouse.

Manual only because deploying Fabric items and spending capacity is a deliberate act, and
the record job commits to `history/`, so a push trigger would make the commit start the
next run.

## What it cost, and what it wrote

Ported from [`direct-lake-parquet-layout`](https://github.com/djouallah/direct-lake-parquet-layout)
and adapted to shared, persistent items. Every pipeline run leaves one record in
`history/runs/` — the Fabric item GUIDs it touched, each leg's compute window, the parquet
layout of every engine's tables (files, row groups, `fct_summary`'s per-column encodings and
physical row order) and the parity fingerprints — and the `Capacity units` workflow keeps
`history/cu.json`: **compute** capacity units per run and engine, read from the Capacity Metrics
model for each leg's items inside its own hours. Storage transactions are deliberately not
attributed: the lakehouse is shared, so its OneLake operations in any window belong to
everybody. [`history/README.md`](../history/README.md) has the schemas and the caveats.

Note: cancelling a GitHub job does **not** stop Fabric — the notebook or Livy session keeps
running, and billing.

## Scheduling it inside Fabric

```bash
gh workflow run pipeline.yml -f engines=iceberg -f deploy=full   # or Actions → pipeline → Run workflow
```

`deploy` is a dispatch input of `pipeline.yml` (`none` by default; `no_model` skips the
semantic models and their reframe, the slow part). The `deploy` job runs `deploy.py` after
the build legs, with the same OIDC identity — duckrun mints the storage, Fabric and Power BI
tokens from the GitHub assertion, so there is no login step. It copies the git-tracked repo
into the `dbt` lakehouse's `Files/dbt`, deploys `fabric_items/` (a notebook, its
`deploy_config` variable library, a pipeline), one semantic model per engine built, and
schedules the pipeline every 12 hours.

The notebook is the scheduled form of one CI leg: it runs the same `provision.py` →
`download_aemo.py` → `run_in_fabric.py` from that copy, on the engine `dbt_target` in
`fabric_items/deploy_config.VariableLibrary/variables.json` names (edit the file to change
it). The pipeline runs it at 2 vCores and again at 8 if that fails (`pipelinecore`). Do not
run it alongside `pipeline.yml`; both land into `dbt_landing`.

The semantic model deploys as `aemo_<engine>`, bound to `<engine>_mart`. Direct Lake has no
schema parameter — a partition's schema is a literal in the model — so `deploy.py` writes it
in per engine, the way duckrun writes the OneLake GUIDs. A Direct Lake model reframes on
deploy, which is why it deploys after the build. dwh's model reads the Warehouse item through
Direct Lake on OneLake.
