# Working on this repo

Read `README.md` first for what the project is. This file is the things that will cost you a
run if you do not know them.

## The rule that governs every change

**One gold layer. The same business logic on all five engines.** If you change what a model
*computes*, change it in all five `models/aemo/<engine>/` copies. The only thing allowed to
differ between engines is *operational* — dialect, adapter capability, incremental strategy,
maintenance — and every such difference is commented at its site with the reason.

Do not add a model, a column, or a filter to one engine only. That is what the four repos
this replaced did, and it is why three correctness fixes ended up living in exactly one repo
each. If something genuinely cannot be expressed on one engine, say so in the model and in
the README table; do not quietly let the engines diverge.

## Verify before you spend anything

```bash
python -m pytest tests_py/ -q            # seconds, no credentials
python .github/scripts/check_gating.py   # all five targets through dbt parse
```

`check_gating.py` is not optional. **The default failure mode of this layout is a run that
builds NOTHING and exits 0** — a target name that stops matching a folder name disables
every model, and `dbt build` reports "Nothing to do" and goes green. Nothing else catches it.

For a real end-to-end check, the `duckrun` target runs entirely locally:

```bash
export FILES_PATH=./landing ONELAKE_TABLES_PATH=./warehouse DBT_SCHEMA=dbt
python download_aemo.py && dbt build --target duckrun --profiles-dir .
```

## Gating

- Gate on **`target.name`**, never `target.type` — `iceberg` and `ducklake` are both
  `type: duckdb`. `target.type` is still right inside a macro that is about DIALECT rather
  than engine (see `parse_filename`).
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
  from the OIDC assertion; `iceberg` is dbt-duckdb and shells out to `az` for the OneLake
  token, so exempting it there kills the leg before it provisions anything.

- **`duckdb__` macros reach BOTH duckdb targets.** `macros/iceberg_adapter_overrides.sql`
  therefore branches on `target.name == 'iceberg'` and reproduces dbt-duckdb's own body
  otherwise. `tests_py/test_adapter_overrides.py` pins those fallbacks against the INSTALLED
  adapter — if it fails, dbt-duckdb changed upstream and the fallback needs re-syncing, or
  ducklake keeps running a stale copy of dbt's SQL.
- **dbt-duckdb silently drops boolean-false attach options.** Use int `0`/`1`.
- **Do not pin `duckdb` or `deltalake` for the duckrun target** — duckrun pins exact versions
  for upstream-bug reasons and overriding them is the usual way to break it. The iceberg
  target's pin is deliberate and documented in `requirements/iceberg.txt`.
- **duckrun: `insert` and `merge_clauses={'when_matched':[{'action':'do_nothing'}]}` are the
  same operation** — a DuckDB anti-join plus a plain append, no delta-rs merge pool, no file
  rewritten. Prefer it to `merge` wherever the model only ever adds rows; a delta-rs merge
  scales with the target's partition span, not the batch, which is what OOM-kills big facts.
  `partition_by` + `incremental_predicates` on `month_key` is what makes the probe prune.
- **duckrun maintenance is built in** (compaction on byte debt, vacuum after). Do not add
  OPTIMIZE/VACUUM jobs for it. Iceberg is the opposite: it has no snapshot expiry, so
  `compact_iceberg.py` is a real job — and it speeds up reads without shrinking storage.
- **Fabric OPENROWSET cannot read gzip CSV.** `DATA_COMPRESSION` is only valid under PARSER
  1.0, which cannot parse the ragged/quoted AEMO rows. Plain CSV + PARSER 2.0 is the only
  working combination, and it is why everything lands uncompressed.
- **dbt-fabric wraps merge models in `MERGE ... USING (<sql>)`**, so a leading top-level
  `WITH` is invalid — use nested derived tables. It wraps views in
  `EXEC('create view ... as <sql>')`, where a leading `-- {{ ref(...) }}` comment collapses
  onto the SELECT and comments it out.
- **Never `--full-refresh` on dwh.** It DROPs and recreates: a Sch-M swap that deadlocks
  Fabric background maintenance, loses grants and rebinds Direct Lake.
- **Spark's `CAST(string AS TIMESTAMP)` returns NULL for `yyyy/MM/dd` instead of erroring.**
  AEMO ships slashes. Parse the format explicitly. DuckDB and T-SQL both accept slashes, so
  only the spark leg was ever affected — a good example of why parity is checked.
- **T-SQL pads strings on comparison** (`'ERB01' = 'ERB01 '` is TRUE); DuckDB and Spark do
  not. One trailing space in a join key can split the engines while every test stays green.
- **`DOUBLE → DECIMAL` tie-breaking differs** (HALF_UP on Spark, HALF_EVEN on DuckDB, a third
  thing in T-SQL), which is why `parity.py` gives the money columns a relative tolerance and
  exact-matches everything else.

## Things not to "fix"

- The duplicated model files. Five copies of `fct_summary.sql` is the design: they are
  gated so exactly one is live, and the duplication is what lets each engine say what its
  adapter forces without a thicket of conditionals. The shared *data* — the AEMO column
  layout — lives in `macros/aemo_columns.sql` and must stay there.
- `fabric_items/` vs `semantic_model/` being separate directories. duckrun's `deploy()`
  takes no exclude filter, and `_scan_item_folders` validates every item folder before
  deploying any, so one stray folder makes the whole deploy ship nothing.
- `pipeline.yml` being manual. It commits to `history/parity/`, so a push trigger makes the
  commit start the next run.
- Do not use `NotebookEdit` on `fabric_items/run.Notebook/notebook-content.ipynb` — keep
  each cell's `source` as an array of lines.

## Domain facts worth keeping

- **The latest day is almost always PARTIAL.** Never divide by 288.
- **`fct_summary.time` is HHMM**, not minutes past midnight. (An earlier note in one of the
  source repos claimed otherwise; it only looked right because the first hour — 0, 5, 10,
  15 — is identical either way.)
- `fct_price` is AEMO's DREGION record (all 130 columns) and `fct_scada` is the DUNIT record
  (all 53). `fct_summary` exposes 5 of those ~180 columns; the wide facts are the analytical
  surface.
- The crater-heal filter in `fct_summary` (`HAVING COUNT(DISTINCT time) >= 280`) and
  `assert_fct_summary_no_partial_dates` are a matched pair: a date can land partially during
  backfill, and the old "skip any date already present" filter meant it was never revisited.
  Observed 2025-09-13: 49 intervals in the summary against 288 in `fct_scada`.
