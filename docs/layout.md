# Layout and gating

How the repo is laid out, how one `--target` selects one engine's tree, and what the eight
models are. The gating is the part worth reading twice: its default failure mode is a run that
builds NOTHING and exits 0.

```
dbt1/                                      dbt-core 1.x: duckrun, ducklake, dwh, spark
dbt2/                                      dbt OSS 2: iceberg (+ its catalogs.yml)
<project>/models/aemo/<engine>/<layer>/<model>.sql
                                           the same 8 model names in all five trees
<project>/models/aemo/_staging.yml _dimensions.yml _marts.yml
                                           ONE patch file per layer; the two projects' copies
                                           are pinned identical by tests_py
macros/aemo_columns.sql                    the AEMO CSV layout — single source of truth,
                                           SHARED: both projects read ../macros
<project>/tests/aemo/<engine>/             the same 12 assertions, per dialect
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
column layout — lives once, in `macros/aemo_columns.sql`, which both projects read.

**Why two projects.** `catalogs.yml` is how dbt 2 declares an Iceberg REST catalog, and dbt
reads it from the directory holding `dbt_project.yml`. Put one next to a dbt 1.x project and
every engine in it dies — `Adapter 'duckdb' does not support catalogs.yml v2 yet` with
`use_catalogs_v2` set, or a v1-loader validation error without it. So the split is by dbt
major version and nothing else: same models, same patch files, same shared macros, same
`iceberg_landing` / `iceberg_mart` schemas the dbt-duckdb leg wrote before it.

### How one run selects one engine

`dbt build --target dwh` in `dbt1/` sets `target.name == 'dwh'`, so only
`models/aemo/dwh/**` is `+enabled` and its three sibling trees parse into
`manifest['disabled']`. The model file names are *identical* across all five trees — legal
only because exactly one tree is enabled per run. There is no `--select` anywhere.

Gating is on **`target.name`, not `target.type`**. The four `dbt1` targets happen to have
four distinct types today, but that is an accident of the engine list — `target.name` is the
folder name, which is what selection actually means. `dbt2` holds one tree and keeps the same
gate anyway: without it a run under the wrong target name would build into the wrong schema
rather than building nothing.

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
