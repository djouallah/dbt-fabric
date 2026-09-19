# Working on this repo

Read `README.md` first for what the project is. This file is the things that will cost you a
run if you do not know them.

## The rule that governs every change

**One gold layer. The same business logic on all five engines.** If you change what a model
*computes*, change it in all five `dbt1/models/aemo/<engine>/` copies. The only thing allowed
to differ between engines is *operational* — dialect, adapter capability, incremental strategy,
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

All five engines are dbt-core 1.x, so one environment reaches every one whose adapter is
installed — with `dbt-duckdb` present that is duckrun, iceberg and ducklake in a single run.
`dbt-fabric` and `dbt-fabricspark` still cannot share an environment (they shadow each other
under `dbt.adapters`), so those two need their own venv and a `DBT_BIN` pointing at it. There
is no `DBT2_BIN` any more; if you find a reference to one, it is stale.

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

- **ONE dbt PROJECT, `dbt1/`, five engines.** Every dbt command runs from inside it
  (`--profiles-dir .`), which is why `.github/scripts/check_gating.py`'s `ENGINES` dict still
  maps engine → project dir — keep `tests_py/_layout.py` and
  `.github/scripts/run_in_fabric.py`'s `PROJECT` in step with it. The name `dbt1` is a scar:
  iceberg spent 2026-09-17 to 2026-09-18 in a sibling `dbt2/` on dbt OSS 2, because
  `catalogs.yml` cannot sit in a dbt 1.x project root. **Do not try that again without
  reading "Why iceberg is not on dbt OSS 2" below.**
- Gate on **`target.name`**, never `target.type`, and here that is not a preference: `iceberg`
  and `ducklake` are BOTH `type: duckdb`, so `target.type` cannot tell five trees apart.
  `target.type` is still right inside a macro that is about DIALECT rather than engine (see
  `parse_filename`), and it is the WRONG discriminator for anything separating iceberg from
  ducklake — see `dbt1/macros/iceberg_adapter_overrides.sql`, where a `duckdb__` macro
  reaches both and has to branch on `target.name`.
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
- **Only `duckrun` is exempt from `azure/login`** in `pipeline.yml`'s build legs. It mints its own tokens
  from the OIDC assertion; every other leg shells out to `az` for an audience duckrun cannot
  mint, so exempting one of them kills it before it provisions anything. `iceberg` needs the
  login for `provision.py` and for the `compact` job's OneLake token, not for the adapter — its
  in-notebook `ONELAKE_TOKEN` comes from `notebookutils`. The `deploy`
  job in `pipeline.yml` is duckrun end to end (storage, Fabric and Power BI tokens all from
  the assertion), so it has no login step either.

- **THE ICEBERG LEG RUNS ON CREDENTIAL VENDING, AND THAT IS PROVEN FOR WRITES.** It holds ONE
  credential — the `token:` on the ATTACH, for the REST catalog — and every data file is read
  and written on the storage credential the catalog vends per table
  (`adls.sas-token.onelake.dfs.fabric.microsoft.com`, usable since duckdb/duckdb-iceberg#1331,
  merged 2026-08-19). **There is no `secrets:` block and no `access_delegation_mode`**; vending
  is the default mode and `'none'` is what turns it OFF. Run 35424929721 (2026-09-19) built all
  eight models green — `fct_price` 16.5s, `fct_scada` 31.3s, `fct_summary` 18.0s — so a LONG
  write survives it, not just `compact_iceberg.py`'s metadata rewrite. The duckdb pin is what
  makes it possible: before #1331 the vended credential is unusable.
  The fallback, if a credential error ever appears, is exactly two things back —
  `access_delegation_mode: 'none'` plus an `azure` secret with `ONELAKE_TOKEN`, which is what
  `djouallah/dbt_fabric_python_iceberg` still carries. Do not put them back without a failing
  run to point at.
- **OTHERWISE THE LEG IS THAT SOURCE REPO'S PROFILE.** `threads: 2` and a hand-pinned duckdb
  come straight from it. **Port from that repo rather than deriving the leg from ducklake plus
  an idea:** three deviations shipped together on 2026-09-18 (vending, a floating
  `--pre duckdb`, `threads: 4`) and the run failed, and because they moved at once none of them
  could be judged. Vending only became a fact once it went in on its own.
- **`stage_create_tables: 0` and `skip_create_table_metadata_updates: 1` are about what OneLake
  DOES, not about credentials.** It vends nothing on `createTable`, so a STAGED create-table-as
  cannot write its data files (`0` splits CTAS into create-then-insert), and it rejects the
  follow-up metadata update — "Only one instance of each update type is allowed per request".
  `default_schema: dbo` must name a namespace that EXISTS; OneLake 422s on namespace creation
  and `dbo` always exists in a lakehouse.
- **dbt-duckdb silently drops boolean-false attach options.** Use int `0`/`1`, or
  `stage_create_tables: false` vanishes and the catalog gets the staged path it rejects.
- **`iceberg` and `ducklake` are both `type: duckdb`, so a `duckdb__` macro override reaches
  BOTH.** `dbt1/macros/iceberg_adapter_overrides.sql` therefore branches on `target.name` and
  reproduces dbt-duckdb's own body verbatim for the other path — and
  `tests_py/test_adapter_overrides.py` pins those fallbacks against the INSTALLED adapter, so
  an upstream change fails loudly instead of leaving ducklake on a stale copy. Two overrides,
  both still needed against dbt-duckdb 1.11.0: `duckdb__get_columns_in_relation` filters
  Iceberg's hidden `__` column out of `DESCRIBE` (upstream uses DESCRIBE now but has no
  filter), and `duckdb__drop_relation` omits `CASCADE`, which the Iceberg extension does not
  support (upstream omits it for DuckLake only). That test `importorskip`s, so
  `requirements/dev.txt` stays dbt-free and `ci.yml`'s gating jobs run the comparison.
- **`iceberg`'s `stg_csv_archive_log` declares `database='memory'`.** The profile's default
  database is the attached Iceberg catalog, which has no `CREATE VIEW`, so without the
  override the model cannot be a view — and it was an insert-only TABLE in the catalog until
  2026-09-18, which made iceberg the one engine whose log could keep rows
  `csv_raw_archive_log.parquet` no longer has. dbt-duckdb hands every model a cursor on ONE
  in-process connection, so a named view in `memory` is visible to every later model and dies
  with the run — the same session view duckrun and ducklake get. A literal
  `CREATE TEMPORARY VIEW` would be per-cursor and invisible to the fact models' pre_hooks.
  `compact_iceberg.py`'s `TABLES` therefore does NOT list it.
- **`on_schema_change='sync_all_columns'` costs a manual DROP if a TYPE ever changes.** It
  performs a type change as add-copy-rename (`<col>__dbt_alter`), which is not atomic on the
  Iceberg catalog: it once added `latitude__dbt_alter` to `iceberg_mart.dim_duid`, failed the
  rename, and every later run died on "Column with name latitude__dbt_alter already exists!"
  until the table was dropped. `iceberg`'s `dim_duid` nevertheless uses `sync_all_columns`,
  because that is the source repo's value and matching it is what got the leg working — the
  same value as ducklake's. Adding a column is fine; a type change means dropping the table by
  hand. `append_new_columns` was tried here on 2026-09-18 and reverted with the rest.
- **duckdb versions per leg (2026-09-18):** ducklake is PINNED to 1.5.5 (its community
  extensions, `mssql_ducklake` and `delta_export`, publish for that line); iceberg is PINNED by
  hand to one dev build, `2.0.0.dev2609121639`, the way the source repo pins; duckrun alone
  tracks the latest PRE-RELEASE (`--pre duckdb`). Never pin `deltalake` for duckrun: the adapter
  pins it itself. The dev line is required for iceberg, not preferred —
  `iceberg_rewrite_data_files()` exists nowhere else — but **`--pre` is a GLOBAL flag in a
  requirements file**, and floating it there had pip considering a `dbt-core 2.0.0rc7` sdist for
  a dbt 1.x leg (seen in the notebook's install log, 2026-09-18). An exact `==` pin to a
  pre-release needs no `--pre`. The compact job installs `--pre duckdb` by itself and nothing
  else, and resolves that same build; it takes the catalog location from the `land` job's output
  instead of re-running `provision.py`.
- **There is ONE requirements file per engine.** `requirements/iceberg_runner.txt` existed only
  while that leg was `dbt-oss`, which cannot share an environment with `duckrun` (duckrun pins
  `dbt-core<2` and lays a 1.x `dbt` console script over v2's). `dbt-duckdb` and `duckrun` share
  one happily, so the runner, the Fabric notebook and the gating job all install
  `requirements/iceberg.txt`.
- **duckrun: `insert` and `merge_clauses={'when_matched':[{'action':'do_nothing'}]}` are the
  same operation** — a DuckDB anti-join plus a plain append, no delta-rs merge pool, no file
  rewritten. Prefer it to `merge` wherever the model only ever adds rows; a delta-rs merge
  scales with the target's partition span, not the batch, which is what OOM-kills big facts.
  `partition_by` + `incremental_predicates` on `month_key` is what makes the probe prune.
- **duckrun, ducklake and iceberg run dbt INSIDE Fabric** (`.github/scripts/remote_dbt.py` →
  duckrun's `run_python`, 8 vCores; `run_in_fabric.py` is what runs there, invoking dbt
  in-process via `dbtRunner`). Do not move them
  back onto the runner: DuckDB folds the archive in memory and the 7 GB hosted runner was shut
  down mid-`fct_scada` twice in one day. Tokens are minted in the notebook by `notebookutils`
  (the `setup` hook); only the `FORWARD` allowlist of config travels, never anything
  token-shaped. `DATA_LAKEHOUSE_ID` from provision.py is what run_python needs.
- **ducklake's catalog SQL DB auto-pauses, and the first connection after that FAILS.** A
  Fabric SQL DB is serverless: idle long enough and it pauses, then the next connection gets
  SQL error 40613 ("Database ... is not currently available. Please retry the connection
  later") while it resumes, which takes about a minute. dbt has no notion of this — it dies at
  the DuckLake ATTACH in ~26 seconds, before one model runs, and `dbt retry` cannot heal it
  because a crash at connection open writes no `run_results.json`. Run 35294987062 lost the
  whole ducklake leg this way, 12 hours after the previous run, with NO code change involved;
  every earlier run had been close enough behind the last to find the database awake. So a
  40613 is not a red build: `run_in_fabric.py` waits and re-runs `build`, keyed on that code
  alone (`RESUME_SIGNATURE`, pinned by `tests_py/test_fabric_resume_retry.py`). Retrying the
  whole build rather than probing the database first is the point — the connection that has to
  succeed is dbt's own, with its own token and extension. Only ducklake has a serverless
  database in its path, which is why only it is ever affected.
- **`dbt1/profiles.yml` CARRIES NO COMMENTS — the non-obvious options are documented here
  instead.** Do not "tidy" any of these away; each one is load-bearing:
  - **iceberg `database: onelake` with `path: ':memory:'`** is legal ONLY because an `attach`
    entry below it has `alias: onelake`. Without the matching alias dbt-duckdb's
    `credentials.__pre_deserialize__` raises "Inconsistency detected between 'path' and
    'database' fields in profile; the 'database' property must be set to 'memory'". The key is
    also what makes the ATTACHED CATALOG the default database for every model, which is what
    sends the gold layer to OneLake at all.
  - **ducklake's three `mssql_*_timeout: 600`** — shaping a fresh catalog takes a couple of
    minutes, well past the 30 s default, and a timeout part-way through leaves the catalog
    half-built so the NEXT attach fails on a missing object.
  - **ducklake `metadata_catalog: "__ducklake_metadata_ducklake"`** is required for
    `delta_export()`: without it the exporter cannot find the catalog. Not mssql-specific — the
    sqlite backend failed the same way.
  - **ducklake `data_path`** is recorded in the catalog on FIRST attach and refuses a different
    one afterwards ("DATA_PATH parameter ... does not match existing data path"). Do NOT reach
    for `OVERRIDE_DATA_PATH`: per the DuckLake docs it overrides for that connection only and
    leaves the stored value alone, so writes moved while `delta_export()` — which reads the
    catalog — kept writing every `_delta_log` under the old path. Change the STORED value
    (`dbo.ducklake_metadata`, key `data_path`); every schema, table and file path is relative
    to it.
  - **any ducklake attach option prefixed `meta_`** is forwarded to the CATALOG connection, and
    the `DBT_ENV_SECRET_` prefix on its value is what keeps the token out of the dbt logs.
  - **`temp_directory` is never in `settings:`** on any DuckDB leg — dbt-duckdb re-applies
    settings on every cursor and DuckDB refuses to switch it once anything has spilled. It is
    an `on-run-start` hook in `dbt_project.yml`, which still comments the reason at its site.
- **DuckLake is MULTI-WRITER, and `mssql_ducklake` must be >= 0.1.1.** ducklake ran on
  `threads: 1` until 2026-09-19, and neither reason survives: the catalog used to be a SQLite
  file, which really is single-writer, and after it moved to a Fabric SQL DB the remaining
  reason was an upstream bug — 0.1.0 keyed `ducklake_schema_versions` on
  `(begin_snapshot, schema_version)` while DuckLake writes one row per table per snapshot, so
  any commit touching two or more tables died on "Violation of PRIMARY KEY constraint
  'pk_ducklake_schema_versions'" (hugr-lab/mssql-ducklake#30). v0.1.1 widened the key to include
  `table_id` and **rebuilds it in place on attach**, so an existing catalog repairs itself and
  there is nothing to run by hand. The source repo still carries the manual patch
  (`scripts/ducklake_schema_versions_pk.sql` + `ensure_catalog_pk_fix()` in its notebook); that
  was never ported here and must not be. The extension also wants the `mssql` extension >= 0.2.5.
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
- **`iceberg_rewrite_data_files` SPLITS as readily as it merges, so give it all three size
  bounds.** It bin-packs toward `target_file_size_bytes` the way Spark's `rewrite_data_files`
  does: leave `min_file_size_bytes` / `max_file_size_bytes` defaulted and they bracket the
  target at 0.75x and 1.8x, so every file above ~1.8x target is rewritten *down*. Run
  35350150404 folded 3,000 archive files in one go and left a good layout — `fct_scada` at 7
  files of ~410 MB — and compaction then spent 2,872 MB of rewrite turning it into 43 files of
  ~67 MB, straight onto the old 64MiB target. Nothing was broken and nothing was red; the job
  just paid OneLake write traffic to make the layout worse. The parameter list is worth reading
  before tuning it (`SELECT parameters FROM duckdb_functions() WHERE function_name =
  'iceberg_rewrite_data_files'`) — a wrong name is a loud Binder Error, so it is cheap to check.
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
  `--full-refresh` on iceberg fails (`fct_summary__dbt_tmp does not exist`) — that catalog
  cannot do the rename the intermediate relation needs. The reset is a DROP on every engine.
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
- **Spark compares a STRING column to an INT literal by casting the STRING to INT, and that
  cast truncates** — `'0.5' != 0` is FALSE. The spark csv temp view is all-STRING, so every
  numeric predicate on it needs an explicit `CAST(... AS DOUBLE)`. A bare `SCADAVALUE != 0` in
  the stage filter dropped 12-16% of the intraday SCADA rows (everything with 0 < |value| < 1)
  and left `fct_summary`'s intraday tail 200-1,500 rows short on spark alone, with every dbt
  test green; only parity saw it (run 35188161001). `tests_py/test_spark_stage_filter.py`
  pins the cast.
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

## Why iceberg is not on dbt OSS 2

It was, for one day (`1f8c602` .. run 35338716074, 2026-09-17 to 2026-09-18), in a sibling
`dbt2/` project. **It never built a single model against the OneLake catalog:**

```
[error] [DbDriverFailed (dbt1308)]: Database Error in model stg_csv_archive_log
  HTTP Error: GetTableInformation endpoint returned response code BadRequest_400
  with message "Malformed request"
```

v2's Iceberg REST client sends a request the catalog rejects, and there is nothing in this repo
to fix. Do not re-attempt the move on a v2 release that has not fixed that. What it cost, and
what is now dead code you will not find by grepping — every line of this was in the tree:

- **A whole second dbt project**, because `catalogs.yml` lives next to `dbt_project.yml` and
  dbt 1.x aborts on one: `Adapter 'duckdb' does not support catalogs.yml v2 yet` with
  `use_catalogs_v2` set, a v1-loader "no `write_integrations`" error without it. Two
  `profiles.yml`, two copies of the three model patch files, two `macro-paths`, and a
  `tests_py/test_patch_files_match.py` to pin the copies byte for byte.
- **A custom incremental strategy** (`insert_only`), because v2's model-config schema
  (`crates/dbt-schemas/src/schemas/project/configs/model_config.rs`) accepts only
  `merge_update_columns`, `merge_exclude_columns` and `merge_with_schema_evolution` — not
  `merge_clauses`, not `merge_update_condition` — even though v2's own `duckdb__get_merge_sql`
  reads `config.get('merge_clauses')` and implements `do_nothing`. `UnusedConfigKey` (dbt1060)
  is a HARD parse error `warn_error_options` cannot downgrade. Both spellings were tried.
- **A render-time file list**, because on v2 a `pre_hook` does NOT share a DuckDB session with
  the model body: the `SET VARIABLE` / `getvariable()` pattern the other DuckDB engines use
  reads NULL, and **fails SILENTLY** — `getvariable()` on an unset variable is NULL, not an
  error. It only surfaced because `read_csv` refuses a NULL list. Reproduced offline on
  dbt-core 2.0.0-rc.2 at threads 1 and 4.
- **`persistent: true` on every profile secret**, because v2 applies `secrets:` on a THROWAWAY
  connection — the write dies "could not open file ... the credentials used were wrong". The
  ATTACH survives because attachments are database-scoped; a secret is not. `settings:` has the
  same problem, so anything that must reach a model was an `on-run-start` hook.
- **An explicit `-- depends_on: {{ ref(...) }}` on every fact model**, because v2 infers
  dependencies statically and rejects a `ref()` it can only see inside a conditional.
- **An `azure` secret and `access_delegation_mode: NONE`**, because v2 bundles duckdb 1.5.3,
  which cannot consume a vended storage credential. That is the one thing the move back BOUGHT:
  see the credential-vending bullet above.

What v2 did better, and is worth remembering if it is ever reconsidered: its own DuckDB macros
need no adapter overrides — `DESCRIBE` for Iceberg column discovery, `DROP` without `CASCADE`,
a standalone rename, and a direct-create path for Iceberg REST that dbt-duckdb has never had.
That last one is why a `table` materialization was briefly possible for the archive log on v2
and is not here.

## Things not to "fix"

- **`pipeline.yml` lands once, then fans out.** The five legs are a MATRIX JOB in that one file
  (there is no per-engine reusable workflow any more — `build.yml` was merged in on 2026-09-18,
  because a reusable workflow is listed in the Actions sidebar as if it were a third top-level
  pipeline and read like one). They run in PARALLEL and do not land; the shared `land` job runs
  `download_aemo.py` before them. Do not move landing back into the legs to "make them
  independent": download_aemo.py rewrites `csv_raw_archive_log.parquet` with a delete-then-copy,
  so concurrent legs race on that one file. The `land` job also provisions the folder,
  `dbt_landing` and the shared `dbt` lakehouse up front, which is what stops five parallel
  create-if-missing calls colliding — and it is where `compact` gets `DATA_LAKEHOUSE_ID` from.
  **Never take that GUID from `build`**: a matrix job's entries overwrite one another's outputs,
  and `compact_iceberg.py` treats an empty GUID as "nothing to compact", warns and exits 0 — so
  iceberg would silently stop being compacted with every run still green. `layout` must keep
  `compact` in its `needs` for the same family of reason: it measures the very files
  `iceberg_rewrite_data_files` rewrites, and a read mid-rewrite is a plausible number, not an
  error. `tests_py/test_pipeline_compact.py` pins all three.

- The duplicated model files. Five copies of `fct_summary.sql` is the design: they are
  gated so exactly one is live, and the duplication is what lets each engine say what its
  adapter forces without a thicket of conditionals. The shared *data* — the AEMO column
  layout — lives in `macros/aemo_columns.sql`, at the REPO ROOT, and must stay there:
  `dbt1` reaches it through `macro-paths: [..., "../macros"]`. That path looks redundant with
  one project and is not: keeping the shared spec outside `dbt1/macros` is what says it is
  shared data rather than engine logic, and `tests_py/test_aemo_columns.py` pins it there.
  The three model patch files (`_staging.yml`, `_dimensions.yml`, `_marts.yml`) sit at
  `dbt1/models/aemo/`, one level ABOVE the engine folders, so ONE patch documents and tests
  whichever tree is enabled — moving them to the root of `models/` loses the gateable segment.
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

## Measuring what it cost and what it wrote

Ported from `djouallah/direct-lake-parquet-layout` (`record.py`, `cu/measure.py`, `stats.py`);
`history/README.md` is the reader-facing account. What differs here, and why:

- **Nothing is torn down, so a GUID does not belong to one run.** The source deletes every item
  when a run ends and reads CU cumulatively per item. Here the lakehouse is shared and the
  warehouse persists, so every leg records `started`/`finished` (`record.py leg-start` /
  `leg-end` in `pipeline.yml`, bracketing build + tests + **fingerprint** — on spark the
  run-operation opens a new Livy session, on dwh it is a full Warehouse Query) and the GUIDs
  its compute bills against (`legs.<engine>.compute`). `measure_cu.py` sums the hour grain of
  `Metrics By Item Operation And Hour` over those items in those hours, per run × engine.
- **Compute only.** Every `OneLake …` operation is excluded, in the DAX and again in Python:
  on a shared lakehouse the storage transactions in any window are all five legs' plus
  whatever else touched the item. Compute is unambiguous per engine, measured 2026-09-17 —
  each DuckDB leg's notebook (`Jupyter Notebook Scheduled Run`), ducklake's catalog SQL DB
  (`Sql Usage`), dwh's `Warehouse Query` on its own item, spark's Livy run on the lakehouse
  (only the spark leg opens Livy sessions there). `history/README.md` carries the table.
- **Caveats the window cannot fix:** two runs under an hour apart share an hour on dwh/spark;
  a Livy session idling past leg-end bills into the next hour. Documented, not fixed.
- **`items` and `legs` are dicts, never lists.** The fragment merge is a recursive dict union
  that REPLACES lists, so a list would let the leg-end fragment wipe the leg-start.
  Fragments merge in BASENAME order (`download-artifact` nests each in its own directory).
  The one list that accumulates is `legs.<engine>.compute`, and `record.leg()` does the union
  itself: ducklake's compute is two items written by two scripts in one job — the catalog SQL
  DB (`Sql Usage`, measured 19.5k CU in three days, nothing else queries it) from
  `provision.py` and the notebook from `remote_dbt.py`.
- **`RUN_RECORD` unset is a silent no-op** — `provision.py` and `remote_dbt.py` must stay
  runnable by hand and from the demo notebook. The cost: a job that forgets it produces a
  record missing those items with nothing red. Every fragment upload is
  `if-no-files-found: ignore`; `record.py finish` logs the item table it assembled.
- **`runner.temp` is not a named value at job level.** `RUN_RECORD` is set from a step into
  `$GITHUB_ENV`, and the fragment lives outside the checkout so the committing jobs never see
  an untracked `record/`.
- **Compare this run's downloaded fingerprints, never `history/parity/`.** After the first
  commit that directory also holds the previous run's, so a leg that failed this run would be
  graded — and folded into the record — on a stale fingerprint.
- **`layout.py` globs `<prefix>_*.*`, never a bare `get_stats()`**, which would sweep every
  `test_*` isolation schema's footers over OneLake. The prefix comes from `deploy.mart_schema`
  — do not add a third copy of the schema rule (`compact_iceberg.py` already has a second).
  Its heavy imports (`duckrun`, `provision`, `obstore`) are lazy so `tests_py/test_layout.py`
  runs in `ci.yml`'s unit job; `provision.py` reads `FABRIC_WORKSPACE_ID` at import.
- **`capacity.yml` and `pipeline.yml` must never gain a `push:` trigger.** Both commit to
  `history/`. GITHUB_TOKEN pushes trigger nothing, which is the only reason two committers are
  safe; `tests_py/test_parity_record.py` pins the trigger set. On a `workflow_run` event the
  checkout MUST use `github.event.workflow_run.head_branch`: the default is the triggering
  run's SHA, from before the record job pushed.
- **Secrets the CU read needs:** `CU_METRICS_WORKSPACE_ID`, `CU_METRICS_MODEL_ID`,
  `CU_CAPACITY_ID` (the same values as the source repo). The Power BI token comes from
  `duckrun.auth.get_powerbi_token()` — the audience `deploy.py` already mints — so no
  `azure/login`. `CU_MODEL_OFFSET_HOURS` is the app's own clock (+10 on this tenant); a wrong
  value reads as "no activity", not as an error.

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
