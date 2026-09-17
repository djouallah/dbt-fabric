{#-- The archive log download_aemo.py writes: one row per landed source file. Every engine
     has its own copy of this model reading the SAME parquet file, which is what makes the
     fact models' file-selection logic identical across engines.

     incremental + merge rather than the folder default (view): the DuckDB Iceberg catalog
     supports neither CREATE VIEW nor the table materialization's temp-table RENAME, but it
     does support CREATE TABLE AS + INSERT. --#}
{#-- Insert-only merge; the `insert_only` strategy is defined in
     dbt2/macros/incremental_insert_only.sql, and it is there because dbt 2 has no config key
     for "MERGE but do not touch matched rows". Same semantics as the other four engines. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='insert_only',
    unique_key=['source_type', 'source_filename'],
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
{% if is_incremental() %}
  AND (source_type, source_filename) NOT IN (
    SELECT source_type, source_filename FROM {{ this }}
  )
{% endif %}
