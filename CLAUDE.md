# Working on this repo

Read `README.md` first for what the project is. This file is the things that will cost you a
run if you do not know them.

## The rule that governs every change

**One gold layer. The same business logic on all five engines.** If you change what a model
*computes*, change it in all five `<project>/models/aemo/<engine>/` copies. Note *five across
two projects*: `dbt1/` holds duckrun, ducklake, dwh and spark; `dbt2/` holds iceberg. It is
easy to edit four and miss the one across the directory boundary. The only thing allowed to
differ between engines is *operational* — dialect, adapter capability, incremental strategy,
maintenance — and every such difference is commented at its site with the reason.

Do not add a model, a column, or a filter to one engine only. That is what the four repos
this replaced did, and it is why three correctness fixes ended up living in exactly one repo
each. If something genuinely cannot be expressed on one engine, say so in the model and in
the README table; do not quietly let the engines diverge.

## Verify before you spend anything

```bash
python -m pytest tests_py/ -q            # seconds, no credentials, no dbt installed
python .github/scripts/check_gating.py   # every engine whose dbt is installed; CI does all five
```

`check_gating.py` knows which project each engine is in and parses it there. One environment
holds either dbt 1.x or dbt OSS 2, never both, so out of the box the iceberg line reads
"skipping". To cover all five from one shell, put dbt 2 in its own venv and point `DBT2_BIN`
at it:

```bash
python -m venv /tmp/v2env && /tmp/v2env/Scripts/pip install dbt-oss     # or bin/pip on POSIX
DBT2_BIN=/tmp/v2env/Scripts/dbt.exe python .github/scripts/check_gating.py
```

Worth doing: a dbt2 mistake otherwise costs a Fabric notebook to find. Note that on a machine
behind a package proxy the newest `dbt-oss` may not be carried — the proxy index served only
up to `2.0.0rc2` here, which is fine for gating. Never install `dbt-oss` into the environment
that has `duckrun`; it pins `dbt-core<2` and the two fight over the `dbt` console script.

`check_gating.py` is not optional. **The default failure mode of this layout is a run that
builds NOTHING and exits 0** — a target name that stops matching a folder name disables
every model, and `dbt build` reports "Nothing to do" and goes green. Nothing else catches it.

For a real end-to-end check, the `duckrun` target runs entirely locally:

```bash
export FILES_PATH=./landing ONELAKE_TABLES_PATH=./warehouse
python download_aemo.py
cd dbt1 && dbt build --target duckrun --profiles-dir .
```

## Gating

- **TWO dbt PROJECTS.** `dbt1/` is dbt-core 1.x (duckrun, ducklake, dwh, spark); `dbt2/` is
  dbt OSS 2 (iceberg). They are split because `catalogs.yml` lives next to `dbt_project.yml`
  and dbt 1.x aborts on one — `Adapter 'duckdb' does not support catalogs.yml v2 yet` with
  `use_catalogs_v2`, a v1-loader error without it. Do not try to merge them back. Every dbt
  command therefore runs from inside a project dir, and `.github/scripts/check_gating.py`'s
  `ENGINES` dict is the one place the mapping lives — keep `tests_py/_layout.py` in step.
- Gate on **`target.name`**, never `target.type`. The four `dbt1` targets have four distinct
  types today, but that is an accident of the engine list, not a property to lean on.
  `target.type` is still right inside a macro that is about DIALECT rather than engine (see
  `parse_filename`).
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
  assumes there IS work. Without it `dbt compile` / `ls` / `docs generate` fire real queries.

## Adapter facts that are easy to get wrong

- **All five engines share ONE Fabric lakehouse (`dbt`) and are separated by SCHEMA** —
  `duckrun_mart`, `iceberg_mart`, ... `generate_schema_name()` is what keeps them apart, so
  a change there is a cross-engine data-collision risk, not a cosmetic rename. Two engines
  resolving to the same schema would overwrite each other's gold layer inside one item with
  every test still green, because each run would see a perfectly consistent table.
  `check_gating.py` asserts `<engine>_landing` / `<engine>_mart` offline; do not weaken it.
- **`LANDING_PATH` and `FILES_PATH` are different variables on purpose.** `download_aemo.py`
  writes to `LANDING_PATH`, which is identical on all five legs; `FILES_PATH` is how that
  engine's dbt READS the zone (a shortcut, for dwh). They were one variable, and
  `provision.py` re-pointed it for ducklake and dwh — so those legs downloaded their own
  private copy of the CSVs and `parity.py` was grading engines on different inputs. Never
  re-emit `FILES_PATH` to move an engine's data somewhere; give it its own key, the way
  `DUCKLAKE_DATA_PATH` does.
- **Only `duckrun` is exempt from `azure/login`** in `build.yml`. It mints its own tokens
  from the OIDC assertion; every other leg shells out to `az` for an audience duckrun cannot
  mint, so exempting one of them kills it before it provisions anything. `iceberg` still
  needs the login even though its dbt runs on `dbt-oss`: the login is for `provision.py` and
  for the `compact` job's OneLake token, not for the adapter. The `deploy`
  job in `pipeline.yml` is duckrun end to end (storage, Fabric and Power BI tokens all from
  the assertion), so it has no login step either.

- **dbt OSS 2 and `duckrun` must never share a Python environment.** `duckrun` pins
  `dbt-core<2` and `dbt-duckdb<2`, which lay a dbt 1.x `dbt` console script over v2's — and
  the job then silently parses the wrong project with the wrong engine. That is why the
  iceberg leg has TWO requirement files: `requirements/iceberg.txt` (just `dbt-oss`) goes to
  the Fabric notebook and the gating job, `requirements/iceberg_runner.txt` (duckdb +
  duckrun) goes to the runner, which only provisions, launches and compacts.
- **dbt 2 needs `persistent: true` on its profile secrets.** It applies `secrets:` on a
  THROWAWAY connection, so a session-scoped secret never reaches the model and the write dies
  "could not open file ... the credentials used were wrong". The ATTACH survives that
  connection because attachments are database-scoped; a secret is not. `settings:` has the
  same problem, so anything that must reach a model is an `on-run-start` hook (or a
  `+pre_hook`, which runs on the model's own connection), never `settings:`.
- **On dbt 2 a `pre_hook` does NOT share a DuckDB session with the model body.** The other
  DuckDB engines set a session variable in a pre_hook and read it back with `getvariable()`;
  on dbt 2 that reads NULL. `dbt2` therefore resolves its file list with a render-time
  `run_query` and inlines it as a literal list (`dbt2/macros/duckdb_source_files.sql`).
  **It fails SILENTLY** — `getvariable()` on an unset variable is NULL, not an error, so the
  model still succeeds. It only surfaced because `read_csv` refuses a NULL list. Never put
  session state between a hook and a model body on this engine. (Reproduced offline on
  dbt-core 2.0.0-rc.2 at threads 1 and 4; `settings:` has the same problem, and attachments
  survive only because they are database-scoped.)
- **dbt 2 infers dependencies statically and rejects a `ref()` it can only see inside a
  conditional** — "dbt was unable to infer all dependencies for the model ... This typically
  happens when ref() is placed within a conditional block." Any model whose `ref()` lives in
  an `{% if %}` or a `{% set %}` block needs an explicit `-- depends_on: {{ ref(...) }}` line
  at the top. Every dbt2 fact model has one.
- **`on_schema_change='sync_all_columns'` is unsafe on the Iceberg catalog.** It performs a
  type change as add-copy-rename (`<col>__dbt_alter`), which is not atomic there: it added
  `latitude__dbt_alter` to `iceberg_mart.dim_duid`, failed the rename, and every later run
  died on "Column with name latitude__dbt_alter already exists!" until the table was dropped.
  `dbt2` uses `append_new_columns`; a type change there means dropping the table.
- **dbt 2 has NO config key for an insert-only merge**, so `dbt2` carries a custom
  incremental strategy — `incremental_strategy='insert_only'`, defined in
  `dbt2/macros/incremental_insert_only.sql`. It emits the same MERGE the other four engines
  get from `merge_clauses={'when_matched':[{'action':'do_nothing'}]}`.
  Do not try to reach it from config again, both spellings have been tried and both are HARD
  parse errors (`UnusedConfigKey`, dbt1060, which `warn_error_options` cannot downgrade):
  v2's model-config schema
  (`crates/dbt-schemas/src/schemas/project/configs/model_config.rs`) accepts only
  `merge_update_columns`, `merge_exclude_columns` and `merge_with_schema_evolution` — not
  `merge_clauses`, not `merge_update_condition` — even though v2's own
  `duckdb__get_merge_sql` reads `config.get('merge_clauses')` and implements `do_nothing`.
  The macro supports a key the schema forbids. The allowed keys are no way out either: they
  route to the 'explicit' update mode, where an empty column list renders a bare `UPDATE SET`.
  **When you add a config to a dbt2 model, check it against that Rust file first** — the
  error names the key but not the allowed set, and each guess costs a CI round trip.
- **Do not port `iceberg_adapter_overrides.sql` into `dbt2/`.** It was deleted with the move.
  dbt 2's own DuckDB macros already do all of it — `DESCRIBE` for Iceberg column discovery,
  `DROP` without `CASCADE`, a standalone rename, a direct-create path for Iceberg REST — so
  an override there would replace v2's better version with a copy of dbt-duckdb 1.x's.
- **dbt-duckdb silently drops boolean-false attach options.** Use int `0`/`1`. This is a
  `ducklake` fact now; `dbt2`'s `catalogs.yml` takes real booleans.
- **duckdb versions per leg (2026-09-17):** ducklake is PINNED to 1.5.5 (its community
  extensions, `mssql_ducklake` and `delta_export`, publish for that line); duckrun tracks the
  latest PRE-RELEASE (`--pre duckdb`). Never pin `deltalake` for duckrun: the adapter pins it
  itself. **iceberg's dbt no longer uses the pip `duckdb` at all** — dbt OSS 2 bundles its own
  engine. The `--pre duckdb` in `requirements/iceberg_runner.txt` is for `compact_iceberg.py`
  only, which needs `iceberg_rewrite_data_files()` off the 1.6.0 dev line.
- **duckrun: `insert` and `merge_clauses={'when_matched':[{'action':'do_nothing'}]}` are the
  same operation** — a DuckDB anti-join plus a plain append, no delta-rs merge pool, no file
  rewritten. Prefer it to `merge` wherever the model only ever adds rows; a delta-rs merge
  scales with the target's partition span, not the batch, which is what OOM-kills big facts.
  `partition_by` + `incremental_predicates` on `month_key` is what makes the probe prune.
- **duckrun, ducklake and iceberg run dbt INSIDE Fabric** (`.github/scripts/remote_dbt.py` →
  duckrun's `run_python`, 8 vCores; `run_in_fabric.py` is what runs there — and it picks the
  project dir and invokes dbt 1.x in-process via `dbtRunner` but dbt 2 as a SUBPROCESS, since
  v2 is a Rust engine with no `dbtRunner` to import). Do not move them
  back onto the runner: DuckDB folds the archive in memory and the 7 GB hosted runner was shut
  down mid-`fct_scada` twice in one day. Tokens are minted in the notebook by `notebookutils`
  (the `setup` hook); only the `FORWARD` allowlist of config travels, never anything
  token-shaped. `DATA_LAKEHOUSE_ID` from provision.py is what run_python needs.
- **`delta_export()` takes no arguments** and writes each DuckLake table's `_delta_log` in
  place, which is why ducklake's `data_path` is the shared lakehouse's `Tables/` section
  (`Tables/ducklake_mart/<table>` is then a real lakehouse table). A two-argument call was
  invented once and failed every run.
- **The ducklake output needs the same OneLake `access_token` secret as iceberg**, and the
  transport hook must reach it: gate on `target.type in ('duckdb', 'duckrun')`, never a list
  of target names.
- **duckrun maintenance is built in** (compaction on byte debt, vacuum after). Do not add
  OPTIMIZE/VACUUM jobs for it. Iceberg is the opposite: it has no snapshot expiry, so
  `compact_iceberg.py` is a real job — and it speeds up reads without shrinking storage.
- **Fabric OPENROWSET cannot read gzip CSV.** `DATA_COMPRESSION` is only valid under PARSER
  1.0, which cannot parse the ragged/quoted AEMO rows. Plain CSV + PARSER 2.0 is the only
  working combination, and it is why everything lands uncompressed.
- **dbt-fabric wraps singular tests in a CTE of its own**, so a test's SQL cannot start with
  `WITH` — use nested derived tables (`dbt1/tests/aemo/dwh/*`). Merge models are built as a CTAS
  temp table and merged FROM it, so `fct_summary` may start with `WITH` (it does). Views are
  wrapped in `EXEC('create view ... as <sql>')`, where a leading `-- {{ ref(...) }}` comment
  collapses onto the SELECT and comments it out.
- **Never `--full-refresh` on dwh.** It DROPs and recreates: a Sch-M swap that deadlocks
  Fabric background maintenance, loses grants and rebinds Direct Lake.
- **`fct_summary` is ONE design on all five engines**, taken from
  `djouallah/direct-lake-parquet-layout`: recompute the dates that could still be stale each
  run (never seen, last 6 days, in the intraday feed), merge key by key, gate the intraday
  tail on `dispatch_duids`. The consolidation first shipped three generations of it (a
  has-new-daily probe on the DuckDB legs, a runner probe on dwh, the reference on spark) and
  parity split by up to 63k rows. Do not reintroduce a per-engine variant. A drifted summary
  is reset by DROPPING the table — any engine, a one-off manual drop is fine on dwh too, it is
  the per-run `--full-refresh` that is not — because merge cannot retract rows and
  `--full-refresh` on iceberg fails (`fct_summary__dbt_tmp does not exist`) — though dbt 2's
  table materialization has a direct-create path for Iceberg REST, so that may no longer hold;
  nothing depends on it either way, the reset is still a DROP.
- **Fabric's Spark catalog base32hex-decodes every part of a multipart name** (alphabet
  `0-9A-V`). `text.\`path\`` fails on the `x` ("Failed to decode multipart name: 'text'");
  `parquet.\`path\`` works only because every letter of `parquet` is inside the alphabet. And
  on a schema-enabled lakehouse dbt-fabricspark's `__dbt_tmp` is a PERSISTENT view, so a model
  body cannot read a TEMPORARY VIEW either. The spark fact models therefore read through a
  `<model>__stage` Delta table built by two pre_hooks (`dbt1/macros/spark_read_csv.sql`). Do not
  "simplify" it back to a direct read.
- **Spark's `CAST(string AS TIMESTAMP)` returns NULL for `yyyy/MM/dd` instead of erroring.**
  AEMO ships slashes. Parse the format explicitly. DuckDB and T-SQL both accept slashes, so
  only the spark leg was ever affected — a good example of why parity is checked.
- **T-SQL pads strings on comparison** (`'ERB01' = 'ERB01 '` is TRUE); DuckDB and Spark do
  not. One trailing space in a join key can split the engines while every test stays green.
- **`DOUBLE → DECIMAL` tie-breaking differs** (HALF_UP on Spark, HALF_EVEN on DuckDB, a third
  thing in T-SQL), which is why `parity.py` gives the money columns a relative tolerance and
  exact-matches everything else.
- **Direct Lake has no schema parameter.** A partition's `schemaName` is a literal in the
  model definition, so `semantic_model/.../model.bim` carries placeholders (`mart`, Desktop's
  GUIDs) and `deploy.py` rewrites the schema to `<engine>_mart` per engine; duckrun rewrites
  the GUIDs. dwh's model is Direct Lake on OneLake over the Warehouse item
  (`mode="direct_lake"`). A model reframes on deploy, so the engine must have built once
  before its model can be deployed.

## Things not to "fix"

- **`pipeline.yml` lands once, then fans out.** The five legs run in PARALLEL and pass
  `land: false`; the shared `land` job runs `download_aemo.py` before them. Do not move
  landing back into the legs to "make them independent": download_aemo.py rewrites
  `csv_raw_archive_log.parquet` with a delete-then-copy, so concurrent legs race on that one
  file. The `land` job also provisions the folder, `dbt_landing` and the shared `dbt`
  lakehouse up front, which is what stops five parallel create-if-missing calls colliding.

- The duplicated model files. Five copies of `fct_summary.sql` is the design: they are
  gated so exactly one is live, and the duplication is what lets each engine say what its
  adapter forces without a thicket of conditionals. The shared *data* — the AEMO column
  layout — lives in `macros/aemo_columns.sql`, at the REPO ROOT, and must stay there: both
  projects reach it through `macro-paths: [..., "../macros"]`. The three model patch files
  are the one thing that genuinely is duplicated (a patch file must sit in its own project's
  model-paths); `tests_py/test_patch_files_match.py` pins the two copies byte for byte.
- `fabric_items/` vs `semantic_model/` being separate directories. duckrun's `deploy()`
  takes no exclude filter, and `_scan_item_folders` validates every item folder before
  deploying any, so one stray folder makes the whole deploy ship nothing. The model is
  deployed as a FILE, once per engine, as `aemo_<engine>`.
- **The demo notebook (`fabric_items/run.Notebook`) runs the CI scripts** — `provision.py`,
  `download_aemo.py`, `run_in_fabric.py` — from the repo copy `deploy.py` puts in the `dbt`
  lakehouse's `Files/dbt`, on the engine the `deploy_config` Variable Library names. Never
  fork dbt logic into it; change the scripts and redeploy. `tests_py/test_fabric_items.py`
  pins the notebook's variables against `variables.json`. The pipeline's `pipelinecore`
  parameter is LIVE: the notebook's `%%configure` cell takes its vCores from it (2, then 8 on
  the retry activity) — it was once deleted here as "dead" because that cell had been lost.
  The notebook mounts the lakehouse and copies `Files/dbt` to the work disk; it does not
  `fs.cp`. Deploy is a `deploy` input on `pipeline.yml`, after the build, the way the source
  repo does it — not a separate workflow.
- `pipeline.yml` being manual. It commits to `history/parity/`, so a push trigger makes the
  commit start the next run.
- Do not use `NotebookEdit` on `fabric_items/run.Notebook/notebook-content.ipynb` — keep
  each cell's `source` as an array of lines (the test above checks it).

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
