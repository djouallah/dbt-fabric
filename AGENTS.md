# Working on this repo

Read `README.md` first for what the project is. This file is the things that will cost you a
run if you do not know them.

## The rule that governs every change

**One gold layer. The same business logic on both engines.** If you change what a model
*computes*, change it in both `models/aemo/<engine>/` copies (`dwh`, `spark`). The only
thing allowed to differ between engines is *operational* — dialect, adapter capability,
incremental strategy, maintenance — and every such difference is commented at its site with
the reason.

Do not add a model, a column, or a filter to one engine only. If something genuinely cannot be
expressed on one engine, say so in the model and in the README; do not quietly let the engines
diverge.

## Microsoft-supported components only

This repo is used for training and must rest on supported pieces:

- **dbt adapters: `dbt-fabric` (dwh) and `dbt-fabricspark` (spark), nothing else.** No DuckDB
  dbt adapter (dbt-duckdb, duckrun, DuckLake, Iceberg-via-DuckDB). Those engines live in
  [fabric-medallion-dbt-community](https://github.com/djouallah/fabric-medallion-dbt-community);
  do not port them back.
- **No `duckrun` package, anywhere** — not for tokens, not for OneLake I/O, not for deploy.
  Tokens come from `azure/login` + azure-identity (or `notebookutils` in a Fabric notebook);
  OneLake bytes go through `ingest/onelake.py` (azure-storage-file-datalake).
- **DuckDB as a LIBRARY is fine** — the role pandas or pyarrow would play, and it ships
  preinstalled in Fabric's Python notebook. `ingest/download_aemo.py` uses it for the nemweb listings
  and the archive log; `layout.py` for parquet footers. `requirements/ops.txt` pins it to the
  version the notebook ships (1.4.4) so a script behaves the same on a runner and in Fabric.
- **Installing into a workspace is Microsoft Fabric Jumpstart** (`fabric-jumpstart`, on
  `fabric-cicd`), from `fabric_items/`; see "Running in Fabric" below.

## Verify before you spend anything

```bash
python -m pytest tests_py/ -q                  # seconds, no credentials, no dbt installed
python .github/scripts/check_gating.py dwh     # needs dbt-fabric; CI runs both engines
python .github/scripts/check_gating.py spark   # needs dbt-fabricspark, in its OWN venv
```

`dbt-fabric` and `dbt-fabricspark` cannot share an environment (they shadow each other under
`dbt.adapters`, "has no attribute 'Plugin'"), so on a laptop keep them in separate venvs and
point `DBT_BIN` at the one you are not running from. Use the OFFICIAL `dbt-fabric`, never the
`dbt-fabric-samdebruyn` fork.

`check_gating.py` is not optional. **The default failure mode of this layout is a run that
builds NOTHING and exits 0** — a target name that stops matching a folder name disables
every model, and `dbt build` reports "Nothing to do" and goes green. Nothing else catches it.

There is no Fabric-free run: both engines' compute is in Fabric, and everything lands in
OneLake — `ingest/onelake.py` accepts only `abfss://` paths.

## Running in Fabric

The user's path is two steps: install `fabric_items/` with Fabric Jumpstart (the snippet in
`README.md`), then run or schedule `run_pipeline`.

- **`run_pipeline` is land -> [dwh, spark] -> parity, four activities on the ONE notebook**,
  each passing `step` and `run_id` (`@pipeline().RunId`). Each activity is its own session,
  which is what lets both adapters run.
- **Nothing dbt-shaped lives in `run.Notebook`.** It reads the `deploy_config` Variable
  Library into env vars, downloads this repo from GitHub at `repo_ref`, and runs
  `fabric_run.py <step> <run_id>`. Change the scripts, not the notebook: a fix pushed to the
  repo reaches every installed workspace on its next run, a notebook change needs a reinstall.
- **`fabric_run.py` runs the commands `pipeline.yml` runs**, and nothing else. Do not give it
  logic the workflow does not have.
- **Fingerprints go to `dbt_landing/Files/parity/<run_id>/`** and `parity` reads that run's
  only, for the same reason the workflow never compares `history/parity/`.
- **`run_pipeline` and `pipeline.yml` must never run at the same time**: they land into the
  same lakehouse and build the same schemas.
- **Item names are rewritten as whole words when Jumpstart applies a prefix**, in every text
  file under `fabric_items/`. No prefix is applied by default. Before one is, `run` and `dbt`
  have to be renamed: as they stand, a prefix would rewrite `dbt build` and `subprocess.run`.
- `.github/workflows/install.yml` (manual) installs into the test workspace and checks every
  item landed; `tests_py/test_fabric_items.py` pins the items offline.

## Gating

- **ONE dbt PROJECT, at the repo root, two engines.** Every dbt command runs from the root
  (`--profiles-dir .`). `.github/scripts/check_gating.py`'s `ENGINES` maps engine → project
  dir; keep `tests_py/_layout.py`'s `PROJECT_OF` in step with it.
- Gate on **`target.name`**, never `target.type`. `target.type` is right only inside a macro
  that is about DIALECT rather than engine (see `parse_filename`).
- **Never put anything on the `aemo_electricity` project key.** A generic test declared in a
  patch file takes the fqn of the YML *file*, so a gate there disables every generic test —
  silently, with the run still green.
- Under `data_tests:` the `aemo` key carries **no** target clause (generic tests have no
  engine segment in their fqn) while the engine keys below do. That asymmetry is deliberate.
- `+enabled` is a scalar: a deeper folder key *clobbers* a shallower one rather than
  combining with it.

## Jinja traps, all met in this repo

- **The last tag before SQL closes `%}`, never `-%}`.** The trimming form eats the newline
  and glues the SQL onto the comment above it — `WITH` + `states` compiled to `WITHstates`.
- **Jinja comments do not nest.** A comment must never quote comment delimiters.
- **A `{% set %}` block cannot resolve `ref()` for a config value.** During dbt's config
  pass `ref()` is a stub that returns the *model's own* relation. A `pre_hook` built that way
  silently reads from `{{ this }}` instead of the archive log. Keep hook text as ONE string
  literal with its `{{ }}` and `{% %}` unevaluated — dbt renders hooks at run time, and only
  then are `ref()`, `this` and `is_incremental()` bound properly.
- Guard every parse-time `run_query` with
  `{%- if execute and flags.WHICH in ('run','build','retry') -%}` and an else-branch that
  assumes there IS work. Without it `dbt compile` / `ls` / `docs generate` fire real queries —
  which is why `docs.yml` generates with `--no-compile --empty-catalog` and dummy env vars.

## Adapter facts that are easy to get wrong

- **The engines are separated by SCHEMA** — `dwh_landing`/`dwh_mart` in the Warehouse,
  `spark_landing`/`spark_mart` in the shared `dbt` lakehouse. `generate_schema_name()` is what
  keeps them apart, so a change there is a cross-engine data-collision risk, not a cosmetic
  rename. `check_gating.py` asserts `<engine>_landing` / `<engine>_mart` offline; do not
  weaken it. The community repo writes the SAME schema names into the same workspace, so the
  two repos' pipelines must never run at the same time.
- **`LANDING_PATH` and `FILES_PATH` are different variables on purpose.** `download_aemo.py`
  writes to `LANDING_PATH`, which is identical on both legs; `FILES_PATH` is how that engine's
  dbt READS the zone (a shortcut, for dwh). They were one variable, and `provision.py`
  re-pointed it for dwh — so that leg downloaded its own private copy of the CSVs and
  `parity.py` was grading engines on different inputs. Never re-emit `FILES_PATH` to move an
  engine's data somewhere; give it its own key.
- **Every job that touches Fabric has an `azure/login` step** (OIDC, no secret). The dwh leg
  captures its `database.windows.net` token right after provisioning: the OIDC client
  assertion lasts ~5 minutes and provisioning can outlive it (AADSTS700024).
- **`dbt retry` REBUILDS THE ORIGINAL COMMAND'S FLAGS, so `flags.WHICH` is `'build'` inside a
  retried build.** dbt 1.11's `dbt/task/retry.py` calls
  `set_flags(Flags.from_dict(CMD_DICT[previous_command], ...))` before it parses or runs
  anything. The `'retry'` the parse-time guards also name is INSURANCE, not a live fix; do not
  remove it.
- **Both engines fold files NEWEST FIRST, and the direction is load-bearing.**
  `process_limit` caps how many unprocessed archive-log files a fact model ingests per run;
  `ORDER BY archive_path DESC` is what decides WHICH (`new_source_files.sql` for dwh,
  `spark_new_files.sql` for spark, and `download_aemo.py`'s `new_files()` one layer up). Change
  the direction in one place and you must change it in all three;
  `tests_py/test_process_order.py` pins it. Newest first is also what makes a partial load
  *recent* data, which is why the four `assert_all_*_files_processed_*` tests and
  `assert_summary_covers_all_scada_days` are `{{ config(severity='warn') }}`: a backlog is not
  a defect, and it converges run by run. **Investigate a count that stops falling, not a count
  that is non-zero.** Relaxed per FILE and not in `dbt_project.yml`'s `data_tests:` section,
  because that key is per engine and would also relax the grain and join assertions.
- **Fabric OPENROWSET cannot read gzip CSV.** `DATA_COMPRESSION` is only valid under PARSER
  1.0, which cannot parse the ragged/quoted AEMO rows. Plain CSV + PARSER 2.0 is the only
  working combination, and it is why everything lands uncompressed.
- **dbt-fabric wraps singular tests in a CTE of its own**, so a test's SQL cannot start with
  `WITH` — use nested derived tables (`tests/aemo/dwh/*`). Merge models are built as a CTAS
  temp table and merged FROM it, so `fct_summary` may start with `WITH` (it does). Views are
  wrapped in `EXEC('create view ... as <sql>')`, where a leading `-- {{ ref(...) }}` comment
  collapses onto the SELECT and comments it out.
- **Never `--full-refresh` on dwh.** It DROPs and recreates: a Sch-M swap that deadlocks
  Fabric background maintenance, loses grants and rebinds Direct Lake.
- **`fct_summary` is ONE design on both engines**, taken from
  `djouallah/direct-lake-parquet-layout`: recompute the dates that could still be stale each
  run (never seen, last 6 days, in the intraday feed), merge key by key, gate the intraday
  tail on `dispatch_duids`. Do not reintroduce a per-engine variant. A drifted summary is reset
  by DROPPING the table — a one-off manual drop is fine on dwh too, it is the per-run
  `--full-refresh` that is not — because merge cannot retract rows.
- **Fabric's Spark catalog base32hex-decodes every part of a multipart name** (alphabet
  `0-9A-V`). `text.\`path\`` fails on the `x` ("Failed to decode multipart name: 'text'");
  `parquet.\`path\`` works only because every letter of `parquet` is inside the alphabet. And
  on a schema-enabled lakehouse dbt-fabricspark's `__dbt_tmp` is a PERSISTENT view, so a model
  body cannot read a TEMPORARY VIEW either. The spark fact models therefore read through a
  `<model>__stage` Delta table built by two pre_hooks (`macros/spark_read_csv.sql`). Do not
  "simplify" it back to a direct read.
- **Spark's `CAST(string AS TIMESTAMP)` returns NULL for `yyyy/MM/dd` instead of erroring.**
  AEMO ships slashes. Parse the format explicitly. T-SQL accepts slashes, so only the spark
  leg was ever affected — a good example of why parity is checked.
- **Spark compares a STRING column to an INT literal by casting the STRING to INT, and that
  cast truncates** — `'0.5' != 0` is FALSE. The spark csv temp view is all-STRING, so every
  numeric predicate on it needs an explicit `CAST(... AS DOUBLE)`. A bare `SCADAVALUE != 0` in
  the stage filter dropped 12-16% of the intraday SCADA rows (everything with 0 < |value| < 1)
  and left `fct_summary`'s intraday tail 200-1,500 rows short on spark alone, with every dbt
  test green; only parity saw it (run 35188161001). `tests_py/test_spark_stage_filter.py`
  pins the cast.
- **T-SQL pads strings on comparison** (`'ERB01' = 'ERB01 '` is TRUE); Spark does not. One
  trailing space in a join key can split the engines while every test stays green.
- **`DOUBLE → DECIMAL` tie-breaking differs** (HALF_UP on Spark, something else in T-SQL),
  which is why `parity.py` gives the money columns a relative tolerance and exact-matches
  everything else.
- **dbt-fabricspark is pinned to 1.13.4** (1.13.5 rejects the profile with "'database' is a
  required property"), and CI sets `DBT_FABRICSPARK_SKIP_OPTIMIZE=true` because 1.13.x runs
  OPTIMIZE after every Delta build, rewriting the layout the run just produced.
  `requirements/spark.txt` names `dbt-spark` explicitly: dbt-fabricspark imports it without
  declaring it.
- **Direct Lake has no schema parameter.** A partition's `schemaName` is a literal in the
  model definition, so `model.bim` carries placeholders (`mart`, Desktop's GUIDs) and
  `fabric_items/parameter.yml` rewrites them at install, per engine: `aemo_dwh` to
  `dwh_mart` in the Warehouse item, `aemo_spark` to `spark_mart` in the `dbt` lakehouse.
  The two `model.bim` files are the SAME file; change both, `tests_py/test_fabric_items.py`
  pins it. A model is installed before its engine has built, so it is empty until
  `run_pipeline` has run once.

## Things not to "fix"

- **`pipeline.yml` lands once, then fans out.** The legs are a MATRIX JOB in that one file.
  They run in PARALLEL and do not land; the shared `land` job runs `download_aemo.py` before
  them. Do not move landing back into the legs to "make them independent": download_aemo.py
  rewrites `csv_raw_archive_log.parquet` in place, so concurrent legs race on that one file.
  The `land` job also provisions the folder, `dbt_landing` and the shared `dbt` lakehouse up
  front (`provision.py landing`), which stops parallel create-if-missing calls colliding.
  `tests_py/test_pipeline_compact.py` pins that the legs do not land.
- The duplicated model files. Two copies of `fct_summary.sql` is the design: they are gated so
  exactly one is live, and the duplication is what lets each engine say what its adapter
  forces without a thicket of conditionals. The shared *data* — the AEMO column layout — lives
  in `macros/aemo_columns.sql`, once, and `tests_py/test_aemo_columns.py` pins that no model
  carries its own column list. The three model patch files (`_staging.yml`, `_dimensions.yml`, `_marts.yml`)
  sit at `models/aemo/`, one level ABOVE the engine folders, so ONE patch documents and
  tests whichever tree is enabled — moving them to the root of `models/` loses the gateable
  segment.
- `pipeline.yml` being manual. It commits to `history/parity/`, so a push trigger makes the
  commit start the next run.

## What each engine wrote

Ported from `djouallah/direct-lake-parquet-layout` (`record.py`, `stats.py`);
`history/README.md` is the reader-facing account.

- **The run record is run stamp + inputs + `layout` + `parity`.** `record.py init` in `land`,
  `layout.py` merges `layout`, `record.py finish` in the `record` job folds in this run's
  fingerprints and commits it to `history/runs/`.
- **Everything in the record is keyed by name, never a list.** The fragment merge is a
  recursive dict union that REPLACES lists. Fragments merge in BASENAME order
  (`download-artifact` nests each in its own directory).
- **`RUN_RECORD` unset is a silent no-op**, so the scripts stay runnable by hand. Every
  fragment upload is `if-no-files-found: ignore`.
- **`runner.temp` is not a named value at job level.** `RUN_RECORD` is set from a step into
  `$GITHUB_ENV`, and the fragment lives outside the checkout so the committing job never sees
  an untracked `record/`.
- **Compare this run's downloaded fingerprints, never `history/parity/`.** After the first
  commit that directory also holds the previous run's, so a leg that failed this run would be
  graded — and folded into the record — on a stale fingerprint.
- **`layout.py` reads only `<prefix>_landing` and `<prefix>_mart`**, never every schema in the
  item, which would sweep each `test_*` isolation schema's footers over OneLake. It replays each
  table's `_delta_log` itself (checkpoint + later JSON commits) to get the live files. The
  schema rule is `layout.mart_schema()` — the one Python copy of `generate_schema_name()`.
  Its heavy imports (`duckdb`, `onelake`, `provision`) are lazy so `tests_py/test_layout.py`
  runs in `ci.yml`'s unit job; `provision.py` reads `FABRIC_WORKSPACE_ID` at import.
- **`pipeline.yml` must never gain a `push:` trigger.** It commits to `history/`; GITHUB_TOKEN
  pushes trigger nothing, which is what makes that safe. `tests_py/test_parity_record.py` pins
  the trigger set. On a `workflow_run` event (`docs.yml`) the checkout MUST use
  `github.event.workflow_run.head_branch`: the default is the triggering run's SHA, from before
  the record job pushed.

## Domain facts worth keeping

- **The latest day is almost always PARTIAL.** Never divide by 288.
- **`fct_summary.time` is HHMM**, not minutes past midnight. (An earlier note in one of the
  source repos claimed otherwise; it only looked right because the first hour — 0, 5, 10,
  15 — is identical either way.)
- `fct_price` is AEMO's DREGION record (all 130 columns) and `fct_scada` is the DUNIT record
  (all 53). `fct_summary` exposes 5 of those ~180 columns; the wide facts are the analytical
  surface.
- `assert_fct_summary_no_partial_dates` guards `fct_summary`'s rebuild window (never-seen
  dates, the last 6 days, dates in the intraday feed): a date can land partially during
  backfill, and the window is what revisits it. Under the old "skip any date already present"
  filter it never was — observed 2025-09-13: 49 intervals in the summary against 288 in
  `fct_scada`.
