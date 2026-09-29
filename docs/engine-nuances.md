# Engine nuances — what differs between the two engines, and why

The business logic is the same on Fabric Warehouse (`dwh`) and Fabric Spark (`spark`), and
the `parity` notebook proves it on every run. Everything below is what is *not* the
same: forced by the engine or the adapter, and documented at its site in the code as well as
here.

Grouped by *kind*, because the kind is the finding — dialect and adapter capability show up on
every model, while the compute shape shows up nowhere in the models.

## Dialect — the same sentence, two grammars

| engine | difference |
|---|---|
| dwh | T-SQL: `[date]`/`[time]` bracket quoting (both are reserved words, and dbt interpolates merge keys raw into the `ON` clause), `DATEPART(HOUR) * 100 + DATEPART(MINUTE)` for the HHMM `time` column, no `GREATEST` (a `UNION ALL` instead), `CAST(... AS VARCHAR(256))` because the Warehouse cannot store the `NVARCHAR` its string functions return |
| dwh | T-SQL **pads strings on comparison** (`'ERB01' = 'ERB01 '` is TRUE); Spark does not. One trailing space in a join key can split the engines with every test green |
| spark | `to_timestamp(..., 'yyyy/MM/dd HH:mm:ss')`: Spark's `CAST(string AS TIMESTAMP)` returns `NULL` for slash dates instead of erroring, which silently nulls the whole column. T-SQL parses slashes, so only this leg is affected |
| spark | comparing a STRING to an INT literal casts the string to INT, which **truncates**: `'0.5' != 0` is FALSE. The CSV stage is all-STRING, so every numeric predicate on it needs an explicit `CAST(... AS DOUBLE)`. A bare `SCADAVALUE != 0` once dropped 12-16% of the intraday SCADA rows with every dbt test green; only parity saw it. `tests_py/test_spark_stage_filter.py` pins the cast |
| spark | `date_format` for the HHMM `time` column |
| both | `DOUBLE → DECIMAL` tie-breaking is HALF_UP on Spark and something else in T-SQL, which is why the `parity` notebook gives the money columns a relative tolerance (1e-7) and exact-matches everything else |

## Feature implementation — what the engine or adapter will actually do

| engine | difference |
|---|---|
| both | `process_limit` caps the archive files a fact model folds per run, and both engines select them **newest first** (`ORDER BY archive_path DESC`). The direction is load-bearing: engines folding different subsets of one backlog would be graded by parity on different inputs. Newest first also makes a partial load recent data, which is why the `assert_all_*_files_processed_*` tests and `assert_summary_covers_all_scada_days` are `severity: warn` — a backlog converges run by run. `tests_py/test_process_order.py` pins the direction |
| dwh | `OPENROWSET(... PARSER_VERSION='2.0')` over **plain** CSV: Fabric cannot read gzip CSV (`DATA_COMPRESSION` is valid only under PARSER 1.0, which cannot parse the ragged AEMO rows); at most 1024 explicit BULK paths *per statement*, hence `process_limit` |
| dwh | the four facts are `append`: the explicit file list already excludes what `{{ this }}` holds, so dedup is done by file selection and a key-join merge would be redundant work. Never `--full-refresh`, which DROPs and recreates — a Sch-M swap that deadlocks Fabric's background maintenance, loses grants and rebinds Direct Lake |
| dwh | `fct_summary` merges on the full `[date],[time],[DUID]` key like spark. Its full-history lever is `REBUILD_SUMMARY=1` or `--vars 'rebuild_summary: true'` (emit every date, same write path), never `--full-refresh` |
| dwh | dbt-fabric wraps singular tests in a CTE of its own, so a test's SQL cannot start with `WITH` (nested derived tables instead). Merge models are built as a CTAS temp table and merged from it, so `fct_summary` may start with `WITH`. Views are wrapped in `EXEC('create view ... as <sql>')`, where a leading `--` comment collapses onto the `SELECT` and comments it out |
| dwh | the Warehouse does not auto-create schemas, and `CREATE SCHEMA` must be alone in its batch — an `on-run-start` hook runs it through `EXEC()` |
| spark | the facts are an **insert-only merge** (`skip_matched_step=true`), not `append`: the file list is computed before the write, so two overlapping runs could both append one file; the key match is the write-time guard underneath it |
| spark | dbt-fabricspark's `__dbt_tmp` is a **persistent** view on a schema-enabled lakehouse, so a model body can read neither a `TEMPORARY VIEW` nor a path datasource whose name Fabric's base32hex decoder rejects (`text.\`path\`` dies on the `x`; `parquet.\`path\`` only works because every letter of `parquet` is inside `0-9A-V`). The CSV read therefore lands in a `<model>__stage` Delta table from two pre_hooks, and the body reads that (`macros/spark_read_csv.sql`) |
| spark | dbt-fabricspark 1.13 runs `OPTIMIZE` after every Delta build, rewriting the layout the run just produced, unless `DBT_FABRICSPARK_SKIP_OPTIMIZE=true`; V-Order is a workspace *resource profile* (`readHeavyForPBI`), not a session conf. The adapter is pinned to 1.13.4 because 1.13.5 rejects the profile (`requirements/spark.txt` has the detail) |
| both | `fct_summary` is ONE design on both, taken from [`direct-lake-parquet-layout`](https://github.com/djouallah/direct-lake-parquet-layout): every run recomputes exactly the dates that could still be stale (never seen, the last 6 days, in the intraday feed) and merges key by key (update matched, insert new), with the intraday tail gated to units the daily archive can reproduce. Merge cannot retract a row, so a drifted summary is reset by dropping the table — the next run recomputes it |

## Compute shape — the axis that turned out not to matter

| engine | difference |
|---|---|
| dwh, spark | dbt runs on the GitHub runner as a client; the fold happens in the Warehouse or the Livy session. A first build over the whole archive costs capacity, which is what `process_limit` bounds. No model mentions the compute shape |
