# Engine nuances — what differs between engines, and why

The business logic is the same on all five engines, and `.github/scripts/parity.py` proves it
on every run. Everything below is what is *not* the same: forced by the engine, the adapter or
the catalog, and documented at its site in the code as well as here.

Grouped by *kind*, because the kind is the finding — dialect and adapter capability show up on
every model, while the compute shape, the axis people expect to matter, shows up in one place
and changed no model.

## Dialect — the same sentence, five grammars

| engine | difference |
|---|---|
| dwh | T-SQL: `[date]`/`[time]` bracket quoting, `DATEPART(HOUR) * 100 + DATEPART(MINUTE)` for the HHMM `time` column, no `GREATEST` (a `UNION ALL` instead), `CAST(... AS VARCHAR(256))` because the Warehouse cannot store the `NVARCHAR` its string functions return |
| dwh | T-SQL **pads strings on comparison** (`'ERB01' = 'ERB01 '` is TRUE); DuckDB and Spark do not. One trailing space in a join key split the engines for over a year with every test green |
| spark | `to_timestamp(..., 'yyyy/MM/dd HH:mm:ss')`: Spark's `CAST(string AS TIMESTAMP)` returns `NULL` for slash dates instead of erroring, which silently nulled the whole column. DuckDB and T-SQL both parse slashes, so only this leg was affected |
| spark | no `DOUBLE PRECISION`, `date_format` for `strftime`, `TIMESTAMP` for `TIMESTAMPTZ` — the fingerprint macro carries a dialect branch for exactly this |
| all | `DOUBLE → DECIMAL` tie-breaking is HALF_UP on Spark, HALF_EVEN on DuckDB and a third thing in T-SQL, which is why `parity.py` gives the money columns a relative tolerance and exact-matches everything else |

## Feature implementation — what the engine, adapter or catalog will actually do

| engine | difference |
|---|---|
| iceberg | insert-only merge on every model — the OneLake Iceberg catalog rejects a matched-UPDATE (data files + delete files in one commit) with `BadRequest 400`, "Only one instance of each update type is allowed per request" |
| iceberg | **two credentials, and vending covers only half of it** — the ATTACH's bearer authorises the CATALOG, the catalog vends a per-table credential for the Iceberg data files under `Tables/` ([duckdb-iceberg#1331](https://github.com/duckdb/duckdb-iceberg/pull/1331), so the duckdb pin is load-bearing), and a separate `azure` secret authorises the LANDING ZONE — plain `Files/`, which every model reads and which is never vended. Plus `stage_create_tables: 0` and `skip_create_table_metadata_updates: 1`, because OneLake vends nothing on `createTable` and rejects the follow-up metadata update |
| iceberg | **a green in-Fabric run is not evidence the credentials are right.** A Fabric notebook's own identity already authorises OneLake, so the leg built all eight models with no `azure` secret at all; the same profile on a GitHub runner died on the first model with `Unauthorized` reading `Files/csv_raw_archive_log.parquet`. That asymmetry is what `pipeline.yml`'s `local_runner` toggle exists to expose |
| iceberg | two adapter overrides on dbt-duckdb: `DESCRIBE` must filter Iceberg's hidden `__` column, and `DROP` must omit `CASCADE`. Both branch on `target.name` because `ducklake` is `type: duckdb` too, and a test pins their fallback bodies against the installed adapter |
| iceberg | `stg_csv_archive_log` declares `database='memory'` — the catalog has no `CREATE VIEW`, so the model would otherwise have to be a table in it, and iceberg would be the one engine whose log keeps rows the parquet no longer has. No `--full-refresh` either: the catalog cannot do the intermediate-relation rename, so a reset is a DROP |
| iceberg | no snapshot expiry, so `compact_iceberg.py` is a real job — and it speeds up reads without shrinking storage. It must be told all three size bounds: `iceberg_rewrite_data_files` bin-packs toward the target, so defaulted min/max brackets it at 0.75x/1.8x and it rewrites big files *down* — one run spent 2.8 GB turning `fct_scada` from 7 files of 410 MB into 43 of 67 MB |
| iceberg | it spent 2026-09-17 to 2026-09-18 on **dbt OSS 2**, in a separate `dbt2/` project because `catalogs.yml` cannot sit in a dbt 1.x project root. v2's Iceberg REST client never built a single model here (`GetTableInformation` → `BadRequest_400`, "Malformed request"), and getting that far cost a second project, a custom incremental strategy, a render-time file list, `persistent: true` on every secret, and an explicit `-- depends_on:` per model. `AGENTS.md` has the full list under "Why iceberg is not on dbt OSS 2". What v2 does better: its own DuckDB macros need neither override above, and it has a direct-create path for Iceberg REST that dbt-duckdb has never had |
| duckrun | `merge` with *do nothing on match* is a DuckDB anti-join plus a plain append — no file rewritten; compaction and vacuum are built into the adapter, so there is no maintenance job |
| ducklake | community `mssql_ducklake` extension (which *replaces* stock `ducklake`), compaction hooks, and `delta_export()` — no arguments, it writes each table's `_delta_log` in place, which is why its `data_path` is the lakehouse `Tables/` section |
| ducklake | the catalog DB needs `COLLATE Latin1_General_100_BIN2_UTF8`, which cannot be changed after creation |
| ducklake | **two bearers, for two different services** — OneLake storage for the parquet and the Delta export, and a `database.windows.net` one for the catalog SQL DB (the profile forwards it as `meta_access_token`). In Fabric both come from `notebookutils`; under `local_runner` `pipeline.yml` mints both with `az` and masks them, which is the only way to find out whether they are the right ones |
| all | `process_limit` caps the files a fact model folds per run and every engine selects them **newest first** (`ORDER BY archive_path DESC`). The direction is not cosmetic: the three DuckDB pre-hooks had no `ORDER BY` at all until 2026-09-19, so the five engines could fold different subsets of one backlog and `parity.py` graded them on different inputs. Newest first also makes a partial load recent data, which is why the `assert_all_*_files_processed_*` tests are `severity: warn` |
| dwh | `OPENROWSET(... PARSER_VERSION='2.0')` over **plain** CSV: Fabric cannot read gzip CSV at all, and PARSER 1.0 cannot parse the ragged AEMO rows; at most 1024 explicit BULK paths *per statement*, hence `process_limit` |
| dwh | `append`, not merge — Fabric has no compare-and-swap, and the file list already excludes what is loaded; never `--full-refresh`, which DROPs and recreates and deadlocks Fabric's background maintenance, loses grants and rebinds Direct Lake |
| dwh | `fct_summary` merges on the full `[date],[time],[DUID]` key like spark. Its full-history lever is `REBUILD_SUMMARY=1` or `--vars 'rebuild_summary: true'` (emit every date, same write path), never `--full-refresh` |
| dwh | the adapter wraps merge models in `MERGE ... USING (<sql>)`, so a top-level `WITH` is illegal, and wraps views in `EXEC('create view ... as <sql>')`, where a leading comment swallows the `SELECT` |
| spark | dbt-fabricspark's `__dbt_tmp` is a **persistent** view on a schema-enabled lakehouse, so a model body can read neither a `TEMPORARY VIEW` nor a path datasource whose name Fabric's base32hex decoder rejects (`text.\`path\`` dies on the `x`; `parquet.\`path\`` only works because every letter of `parquet` is inside `0-9A-V`). The CSV read therefore lands in a `<model>__stage` Delta table from two pre_hooks, and the body reads that |
| spark | `threads: 4` is a hard cap — one Spark REPL per thread, five REPLs per Livy session; the adapter runs `OPTIMIZE` after every build unless told not to; V-Order is a workspace *resource profile*, not a session conf |
| all | `fct_summary` is ONE design on all five, taken from [`direct-lake-parquet-layout`](https://github.com/djouallah/direct-lake-parquet-layout): every run recomputes exactly the dates that could still be stale (never seen, the last 6 days, in the intraday feed) and merges key by key, with the intraday tail gated to units the daily archive can reproduce. Insert-only on the DuckDB family because the OneLake Iceberg catalog rejects a matched UPDATE, and the three DuckDB trees run one config; update+insert on spark and dwh. To reset one engine's summary, drop the table — the next run recomputes it (`--full-refresh` fails on iceberg) |

## Compute shape — the axis that turned out not to matter

| engine | difference |
|---|---|
| duckrun, iceberg, ducklake | single-node DuckDB folds the archive in memory, so in CI these legs run **inside Fabric** on an 8-vCore Python notebook rather than on the 7 GB hosted runner (which was shut down mid-`fct_scada`). The runner provisions, launches and collects the log. **No model changed.** Unless `pipeline.yml`'s `local_runner` input is on: all three then build on the runner through the same `.github/scripts/run_dbt.py` the notebook runs — a verification mode, and the only thing that exercises these profiles' own credentials, so pair it with a small `process_limit` |
| dwh, spark | dbt runs on the runner as a client; the fold happens in the Warehouse / the Livy session. A first build over the whole archive costs cluster time, which is what `process_limit` bounds |
