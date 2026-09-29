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
  `deploy.py` uploads to OneLake with azure-storage-file-datalake.
- **DuckDB as a LIBRARY is fine** — the role pandas or pyarrow would play, and it ships
  preinstalled in Fabric's Python notebook. The `ingest` notebook uses it for the nemweb
  listings and the archive log, and installs nothing.
- **Installing into a workspace is Microsoft Fabric Jumpstart** (`fabric-jumpstart`, on
  `fabric-cicd`) for the demo, **and `fabric-cicd` itself from CI** for production, both from
  `fabric_items/`; see "Running in Fabric" below.

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

There is no Fabric-free run, and no run from a GitHub runner: the run is `run_pipeline`, in a
Fabric workspace. CI checks the project offline and installs it; it never builds it.

## Running in Fabric

The user's path is two steps: install `fabric_items/`, then run or schedule
`run_pipeline`. There are two installs, and `README.md` has both.

- **`run_pipeline` is ingest -> [dwh, spark] -> parity, on THREE notebooks.** `ingest` lands
  the files, `run` builds one engine (parameter `engine`, called once per engine),
  `parity` compares the two engines' gold tables. Each activity is its own session, which is what lets both adapters
  run. There is no step switch in any notebook; do not bring one back.
- **Each notebook holds its own code, and is the ONLY copy of it. NO NOTEBOOK CALLS A PYTHON
  SCRIPT**; the only things `run` runs are `pip` and `dbt`. They are there to be read: how
  the files land, how dbt gets installed, connected and run, how the engines are compared
  should not need a second file open. There is no `download_aemo.py`, `parity.py` or
  `fabric_run.py`, and `provision.py` is for a build by hand from a laptop, not for Fabric. The price is that a change to a notebook reaches a workspace at the next
  install or deploy, not at the next run.
- **Only `ingest` lands.** It rewrites `csv_raw_archive_log.parquet` in place, so two steps
  landing at once would race on that one file, and the engines would be compared on
  different inputs. `tests_py/test_fabric_items.py` pins it.
- **All three notebooks have `dbt_landing` as their default lakehouse**, so every path is a
  plain one under `/lakehouse/default/Files`. The binding in the notebook metadata is the
  lakehouse's `logicalId` and the all-zero workspace id; `fabric-cicd` swaps both at install,
  the way it does for the `notebookId` of a pipeline activity. No `parameter.yml` rule.
- **dbt runs as a subprocess of the `run` notebook, never imported into the kernel**, so the
  `pip install` needs no restart.
- **`parity` reads the tables itself, and has nothing to do with dbt.** It reads both
  engines' `<engine>_mart` tables as Delta, straight from OneLake, with the notebook's
  preinstalled DuckDB and its own credentials: `fct_summary` day by day, the two dimensions row
  by row. No engine reports on its own output, and nothing is scraped from a log. It runs only
  after both legs succeed, so it never grades a failed leg.
- **The demo install is Fabric Jumpstart, the production install is `deploy.yml`.** Jumpstart
  clones the repo from GitHub and `ingest` downloads the project from it once, so the
  demo needs a PUBLIC repo. `deploy.py` publishes the items from the CI checkout with
  `fabric-cicd` and uploads the project to `dbt_landing/Files/project/`, file by file, so
  nothing is fetched from GitHub and the repo can be private.
- **`run` ALWAYS reads the project from `dbt_landing/Files/project/`; only `ingest` fetches
  it.** The two `run` legs start together and would race on one download. `ingest` downloads
  it from GitHub at `repo_ref` when the folder has no `COMMIT` (new, deleted, or an
  unfinished download) or its `REF` is not `repo_ref` (an install of a newer release). A
  deployed folder has no `REF` and is never replaced. There is no per-run download: the
  ref is a tag, fixed.
- **A deploy blanks `repo_ref`, so there is NO fallback to GitHub.** `deploy.py` publishes
  as the environment `production`, and `fabric-cicd` activates the Variable Library value set
  of that name, which overrides `repo_ref` to `""`; `ingest` then stops rather than download
  the public repo into a private copy's workspace. The value set's name, `settings.json`'s
  `valueSetsOrder` and `deploy.py`'s `ENVIRONMENT` are one name in three files. On `main`,
  `repo_ref` is `main` and the folder is NOT refreshed by a push: delete it to pick one up.
- **The project is uploaded as a FOLDER, of the COMMIT.** Not a zip: what is deployed can be
  opened and read in the lakehouse. `git archive HEAD`, not the working tree, so a deploy from
  a laptop and one from CI leave the same files. The folder is deleted first, so a model
  removed from the repo does not survive there. Its `COMMIT` file names the commit.
- **Only the dbt project is uploaded, not the repo**: `deploy.py`'s `UPLOADED` is
  `dbt_project.yml`, `profiles.yml`, `models/`, `macros/`, `tests/`, plus `requirements/`,
  which the `run` notebook installs dbt from. `ingest`'s download keeps the same list. A path
  the notebook starts to read must be added to both, or the run breaks.
  `tests_py/test_fabric_items.py` pins the two lists equal.
- **A deploy after an install replaces the folder, and an install after a deploy keeps it**
  (it has no `REF`): each re-publishes `deploy_config` and sets the active value set. Delete
  the folder to go back to the tag. They share a concurrency group.
- **Do not deploy while `run_pipeline` is running**: the two engines would build from two
  commits.
- **The items are in `fabric_items/`, and both installs are told so.** Jumpstart looks in
  `<logical_id>/` unless it is given `workspace_path`, so the README's snippet and
  `install_jumpstart.py` pass `workspace_path="fabric_items/"`. Both installs put the items
  in a workspace folder named after the `logical_id`, `fabric-medallion-dbt`.
- **Item names are rewritten as whole words when Jumpstart applies a prefix**, in every text
  file under `fabric_items/`. No prefix is applied by default. Before one is, `run`,
  `dbt`, `ingest` and `parity` have to be renamed: as they stand, a prefix would rewrite
  `dbt build` and `subprocess.run`.
- **A release is a tag on a commit OFF `main` whose `repo_ref` is that tag.** The catalog
  entry in `jumpstart/` installs at a tag, and `ingest` downloads the project at
  `deploy_config`'s `repo_ref`: tag `main` as it is and a `v1.0.0` install builds whatever
  `main` held on its first run. `main` keeps `repo_ref: main`. Steps in `jumpstart/README.md`;
  `tests_py/test_jumpstart_entry.py` pins the entry to `install_jumpstart.INSTALL`.
- `.github/workflows/install.yml` and `deploy.yml` (both manual) install into the test
  workspace and check every item landed; `tests_py/test_fabric_items.py` pins the items, the
  notebooks and the deploy's names offline.

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
- **`LANDING_PATH` and `FILES_PATH` are different variables on purpose.** The landing zone is
  `LANDING_PATH`, which is identical on both legs; `FILES_PATH` is how that engine's dbt READS
  the zone (a shortcut, for dwh). They were one variable, and `provision.py` re-pointed it for
  dwh — so that leg downloaded its own private copy of the CSVs and parity was grading
  engines on different inputs. Never re-emit `FILES_PATH` to move an engine's data somewhere;
  give it its own key.
- **`install.yml` and `deploy.yml` log in with `azure/login`** (OIDC, no secret). In a
  notebook each adapter asks `notebookutils` for its token, and each has its own name for
  that: `notebookutils` for dbt-fabric, `fabric_notebook` for dbt-fabricspark. On a laptop it
  is the Azure CLI, and `provision.py` emits `FABRIC_AUTH=CLI` for that.
- **`dbt retry` REBUILDS THE ORIGINAL COMMAND'S FLAGS, so `flags.WHICH` is `'build'` inside a
  retried build.** dbt 1.11's `dbt/task/retry.py` calls
  `set_flags(Flags.from_dict(CMD_DICT[previous_command], ...))` before it parses or runs
  anything. The `'retry'` the parse-time guards also name is INSURANCE, not a live fix; do not
  remove it.
- **Both engines fold files NEWEST FIRST, and the direction is load-bearing.**
  `process_limit` caps how many unprocessed archive-log files a fact model ingests per run;
  `ORDER BY archive_path DESC` is what decides WHICH (`new_source_files.sql` for dwh,
  `spark_new_files.sql` for spark, and the `ingest` notebook's `new_files()` one layer up). Change
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
  which is why the `parity` notebook gives the money columns a relative tolerance and
  exact-matches everything else.
- **dbt-fabricspark is pinned to 1.13.4** (1.13.5 rejects the profile with "'database' is a
  required property"), and the `run` notebook sets `DBT_FABRICSPARK_SKIP_OPTIMIZE=true` because 1.13.x runs
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

- The duplicated model files. Two copies of `fct_summary.sql` is the design: they are gated so
  exactly one is live, and the duplication is what lets each engine say what its adapter
  forces without a thicket of conditionals. The shared *data* — the AEMO column layout — lives
  in `macros/aemo_columns.sql`, once, and `tests_py/test_aemo_columns.py` pins that no model
  carries its own column list. The three model patch files (`_staging.yml`, `_dimensions.yml`, `_marts.yml`)
  sit at `models/aemo/`, one level ABOVE the engine folders, so ONE patch documents and
  tests whichever tree is enabled — moving them to the root of `models/` loses the gateable
  segment.
- `install.yml` and `deploy.yml` being manual. They create Fabric items and decide what a
  workspace runs. Only `ci.yml` runs on push; `tests_py/test_fabric_items.py` pins it.
- `docs.yml`'s checkout naming `github.event.workflow_run.head_branch`. On a `workflow_run`
  event `github.ref_name` is the default branch, wherever the deploy ran.

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
