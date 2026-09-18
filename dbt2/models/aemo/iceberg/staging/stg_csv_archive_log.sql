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
{{ config(materialized='table', schema='landing') }}

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
