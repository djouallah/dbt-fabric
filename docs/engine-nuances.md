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
| iceberg | a whole separate dbt project (`dbt2/`) on dbt OSS 2, because `catalogs.yml` cannot sit in a dbt 1.x project root. In exchange the adapter patches disappear: dbt 2's own DuckDB macros already use `DESCRIBE` for Iceberg column discovery, `DROP` without `CASCADE`, a standalone rename and a direct-create path for Iceberg REST |
| iceberg | no snapshot expiry, so `compact_iceberg.py` is a real job — and it speeds up reads without shrinking storage |
| duckrun | `merge` with *do nothing on match* is a DuckDB anti-join plus a plain append — no file rewritten; compaction and vacuum are built into the adapter, so there is no maintenance job |
| ducklake | community `mssql_ducklake` extension (which *replaces* stock `ducklake`), `threads: 1` for a single-writer catalog, compaction hooks, and `delta_export()` — no arguments, it writes each table's `_delta_log` in place, which is why its `data_path` is the lakehouse `Tables/` section |
| ducklake | the catalog DB needs `COLLATE Latin1_General_100_BIN2_UTF8`, which cannot be changed after creation |
| dwh | `OPENROWSET(... PARSER_VERSION='2.0')` over **plain** CSV: Fabric cannot read gzip CSV at all, and PARSER 1.0 cannot parse the ragged AEMO rows; at most 1024 explicit BULK paths *per statement*, hence `process_limit` |
| dwh | `append`, not merge — Fabric has no compare-and-swap, and the file list already excludes what is loaded; never `--full-refresh`, which DROPs and recreates and deadlocks Fabric's background maintenance, loses grants and rebinds Direct Lake |
| dwh | `fct_summary` merges on the full `[date],[time],[DUID]` key like spark. Its full-history lever is `REBUILD_SUMMARY=1` or `--vars 'rebuild_summary: true'` (emit every date, same write path), never `--full-refresh` |
| dwh | the adapter wraps merge models in `MERGE ... USING (<sql>)`, so a top-level `WITH` is illegal, and wraps views in `EXEC('create view ... as <sql>')`, where a leading comment swallows the `SELECT` |
| spark | dbt-fabricspark's `__dbt_tmp` is a **persistent** view on a schema-enabled lakehouse, so a model body can read neither a `TEMPORARY VIEW` nor a path datasource whose name Fabric's base32hex decoder rejects (`text.\`path\`` dies on the `x`; `parquet.\`path\`` only works because every letter of `parquet` is inside `0-9A-V`). The CSV read therefore lands in a `<model>__stage` Delta table from two pre_hooks, and the body reads that |
| spark | `threads: 4` is a hard cap — one Spark REPL per thread, five REPLs per Livy session; the adapter runs `OPTIMIZE` after every build unless told not to; V-Order is a workspace *resource profile*, not a session conf |
| all | `fct_summary` is ONE design on all five, taken from [`direct-lake-parquet-layout`](https://github.com/djouallah/direct-lake-parquet-layout): every run recomputes exactly the dates that could still be stale (never seen, the last 6 days, in the intraday feed) and merges key by key, with the intraday tail gated to units the daily archive can reproduce. Insert-only on the DuckDB family because the OneLake Iceberg catalog rejects a matched UPDATE; update+insert on spark and dwh. To reset one engine's summary, drop the table — the next run recomputes it (`--full-refresh` fails on iceberg) |

## Compute shape — the axis that turned out not to matter

| engine | difference |
|---|---|
| duckrun, iceberg, ducklake | single-node DuckDB folds the archive in memory, so in CI these legs run **inside Fabric** on an 8-vCore Python notebook rather than on the 7 GB hosted runner (which was shut down mid-`fct_scada`). The runner provisions, launches and collects the log. **No model changed.** |
| dwh, spark | dbt runs on the runner as a client; the fold happens in the Warehouse / the Livy session. A first build over the whole archive costs cluster time, which is what `process_limit` bounds |
