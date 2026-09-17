{#-- The archive log download_aemo.py writes: one row per landed source file. Every engine
     has its own copy of this model reading the SAME parquet file, which is what makes the
     fact models' file-selection logic identical across engines.

     A plain view: it is a thin read over the parquet log, so there is nothing to
     materialize. (The iceberg copy has to be incremental instead — that catalog supports
     neither CREATE VIEW nor the table materialization's temp-table RENAME.) --#}
{{ config(
    materialized='view',
    schema='landing'
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
