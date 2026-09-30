# Layout and gating

How the repo is laid out, how one `--target` selects one engine's tree, and what the eight
models are. The gating is the part worth reading twice: its default failure mode is a run that
builds NOTHING and exits 0.

```
dbt_project.yml, profiles.yml              the dbt project, at the repo root: both engines
models/aemo/<engine>/<layer>/<model>.sql
                                           the same 8 model names in both trees (dwh, spark)
models/aemo/_staging.yml _dimensions.yml _marts.yml
                                           ONE patch file per layer, above the engine folders,
                                           so one patch documents whichever tree is enabled
macros/                                    the AEMO CSV layout (aemo_columns.sql, the single
                                           source of truth), the schema rule, and what
                                           one engine's adapter forces:
                                           the T-SQL OPENROWSET reader, the Spark staging
                                           tables, the Warehouse schema pre-create
tests/aemo/<engine>/                       the same 12 assertions, per dialect
.github/scripts/check_gating.py            proves the gating, offline
.github/scripts/provision.py               finds or creates the Fabric items (REST helpers
                                           for the scripts below)
fabric_items/                              what gets installed in Fabric: the lakehouses, the
                                           warehouse, the pipeline and its three notebooks
                                           (ingest: one landing zone, plain CSV; run: dbt on
                                           one engine; parity: proves the engines agree), and
                                           a Direct Lake semantic model per engine
.github/scripts/install_jumpstart.py       the demo install, Fabric Jumpstart from GitHub
.github/scripts/deploy.py                  the production install, from the CI checkout,
                                           into DEV or PROD
docs/                                      this, and the rest of docs/README.md
```

Two copies of each model is the design, not an accident. They are gated so exactly one is
live, and the duplication is what lets each engine say what its adapter forces in plain SQL,
without a thicket of `{% if target.type %}` conditionals. The shared *data* — the AEMO
column layout — lives once, in `macros/aemo_columns.sql`.

### How one run selects one engine

`dbt build --target dwh` sets `target.name == 'dwh'`, so only
`models/aemo/dwh/**` is `+enabled` and the spark tree parses into `manifest['disabled']`.
The model file names are *identical* across both trees — legal only because exactly one tree
is enabled per run. There is no `--select` anywhere.

Gating is on **`target.name`**, which is the folder name — what selection actually means.
`target.type` stays the right discriminator inside a macro that is about *dialect* rather
than engine (`parse_filename`).

**The default failure mode of this design is a green run that built nothing.** A target name
that matches no folder disables everything, and `dbt build` then reports "Nothing to do" and
exits 0. That is what `check_gating.py` is for, and why CI runs it before anything spends.

Two more rules, each of which has already cost a silent failure (`dbt_project.yml`
carries them at their site):

- **Nothing on the `aemo_electricity` project key.** A generic test declared in a patch file
  takes the fqn of the YML *file*, so a gate there disables every generic test, green.
- **`+enabled` is a scalar**: a deeper folder key clobbers a shallower one rather than
  combining with it. Under `data_tests:` the `aemo` key therefore carries no target clause
  (generic tests have no engine segment), while the engine keys below it do.

## The gold layer

Eight models, identical on both engines:

`stg_csv_archive_log` · `dim_calendar` · `dim_duid` · `fct_price` · `fct_price_today` ·
`fct_scada` · `fct_scada_today` · `fct_summary`

`fct_summary` is the Power BI-facing table and the one parity compares: one row per
`(date, time, DUID)` joining generation to the matching regional price. `time` is HHMM, not
minutes past midnight, and the latest day is almost always partial. `fct_price` is AEMO's
DREGION record (all 130 columns) and `fct_scada` the DUNIT record (all 53); the summary
exposes five of those columns, the wide facts are the analytical surface.

Models that existed on only one engine are deliberately **not** carried over — that is
exactly what this repo exists to stop.
