# Layout and gating

How the repo is laid out, how one `--target` selects one engine's tree, and what the eight
models are. The gating is the part worth reading twice: its default failure mode is a run that
builds NOTHING and exits 0.

```
dbt1/                                      the dbt project: all five engines, dbt-core 1.x
dbt1/models/aemo/<engine>/<layer>/<model>.sql
                                           the same 8 model names in all five trees
dbt1/models/aemo/_staging.yml _dimensions.yml _marts.yml
                                           ONE patch file per layer, above the engine folders,
                                           so one patch documents whichever tree is enabled
macros/aemo_columns.sql                    the AEMO CSV layout — single source of truth, at the
                                           REPO ROOT: dbt1 reads it through ../macros
dbt1/macros/                               what one engine's adapter forces: the T-SQL
                                           OPENROWSET reader, the Spark staging dance, the
                                           iceberg adapter overrides
dbt1/tests/aemo/<engine>/                  the same 12 assertions, per dialect
download_aemo.py                           one downloader, one landing zone, plain CSV
.github/scripts/check_gating.py            proves the gating, offline
.github/scripts/parity.py                  proves the engines agree
.github/scripts/check_catalog_stats.py     proves the published page is not empty
.github/scripts/remote_dbt.py              runs a DuckDB leg inside Fabric (8 vCores)
.github/scripts/record.py                  the run record: items by GUID, each leg's window
.github/scripts/layout.py                  what each engine wrote: files, row groups, encodings, order
.github/scripts/measure_cu.py              what each engine cost: capacity units per run and engine
history/                                   parity/ fingerprints, runs/ records, cu.json ledger
docs/                                      run.md, layout.md (this), engine-nuances.md,
                                           fabric.md, ci.md, candidate-engines.md
deploy.py                                  the in-Fabric demo: repo copy, notebook + pipeline, semantic model
fabric_items/                              the scheduled notebook, its variable library, the pipeline
semantic_model/                            one Direct Lake model, deployed once per engine
```

Five copies of each model is the design, not an accident. They are gated so exactly one is
live, and the duplication is what lets each engine say what its adapter forces in plain SQL,
without a thicket of `{% if target.type %}` conditionals. The shared *data* — the AEMO
column layout — lives once, in `macros/aemo_columns.sql`.

**Why the project is called `dbt1`.** For one day it had a sibling. `catalogs.yml` is how dbt
OSS 2 declares an Iceberg REST catalog, and dbt reads it from the directory holding
`dbt_project.yml` — put one next to a dbt 1.x project and every engine in it dies. So the
`iceberg` leg moved to its own `dbt2/` on 2026-09-17, failed to build a single model against
the OneLake catalog, and came back the next day. [Engine
nuances](engine-nuances.md) has what that cost and what it bought.

### How one run selects one engine

`dbt build --target dwh` in `dbt1/` sets `target.name == 'dwh'`, so only
`models/aemo/dwh/**` is `+enabled` and its four sibling trees parse into
`manifest['disabled']`. The model file names are *identical* across all five trees — legal
only because exactly one tree is enabled per run. There is no `--select` anywhere.

Gating is on **`target.name`, not `target.type`**, and that is forced rather than chosen:
`iceberg` and `ducklake` are both `type: duckdb`, so the type cannot tell five trees apart.
`target.name` is the folder name, which is what selection actually means.

**The default failure mode of this design is a green run that built nothing.** A target name
that matches no folder disables everything, and `dbt build` then reports "Nothing to do" and
exits 0. That is what `check_gating.py` is for, and why CI runs it before anything spends.

## The gold layer

Eight models, identical on all five engines:

`stg_csv_archive_log` · `dim_calendar` · `dim_duid` · `fct_price` · `fct_price_today` ·
`fct_scada` · `fct_scada_today` · `fct_summary`

`fct_summary` is the Power BI-facing table and the one parity compares: one row per
`(date, time, DUID)` joining generation to the matching regional price. `fct_price` is
AEMO's DREGION record (all 130 columns) and `fct_scada` the DUNIT record (all 53); the
summary exposes five of those columns, the wide facts are the analytical surface.

Models that existed on only one engine are deliberately **not** carried over — that is
exactly what this repo exists to stop. They remain in the original repos' history.
