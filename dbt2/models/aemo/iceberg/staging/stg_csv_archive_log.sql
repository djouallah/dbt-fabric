{#-- The archive log download_aemo.py writes: one row per landed source file. Every engine
     has its own copy of this model reading the SAME parquet file, which is what makes the
     fact models' file-selection logic identical across engines.

     table rather than the folder default (view): the DuckDB Iceberg catalog has no
     CREATE VIEW. It is the ONE engine that has to materialize this at all.

     REPLACED EVERY RUN, not incremental. Incremental + insert_only was the shape while the
     table materialization meant a temp-table RENAME the Iceberg catalog cannot do; dbt 2's
     own DuckDB macros have a direct-create path for Iceberg REST, so a plain table works.
     It also has to be a replace: a cumulative copy DIVERGES from the other four. They are
     views, so they show exactly what csv_raw_archive_log.parquet holds; an insert-only copy
     keeps rows the parquet no longer has (download_aemo.py rewrites it delete-then-copy, and
     starts an EMPTY log if it cannot read the old one) -- and iceberg would then fold a CSV
     no other engine can see. --#}
{#-- The pre_hook is what makes it DROP then CREATE rather than create-and-rename. dbt has no
     materialization for that: `table` builds <model>__dbt_tmp and RENAMES it over the target,
     and the rename is the one step this catalog cannot do. With the target already gone the
     create has nothing to swap over, whichever path v2's macros take. Safe to drop first
     because this model is a pure read of csv_raw_archive_log.parquet -- a failed run leaves
     no table, and the next run rebuilds it whole. --#}
{{ config(
    materialized='table',
    schema='landing',
    pre_hook="DROP TABLE IF EXISTS {{ this }}"
) }}

SELECT
  source_type,
  source_filename,
  archive_path,
  archived_at,
  row_count,
  source_url,
  etag,
  csv_filename
FROM read_parquet('{{ get_archive_log_path() }}')
WHERE csv_filename IS NOT NULL
