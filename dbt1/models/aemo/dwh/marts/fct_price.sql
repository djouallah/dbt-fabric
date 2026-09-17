{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

{#-- Reads the new AEMO daily files, filtering to the DREGION price records. The file set comes
     from the archive log (new_source_files) and is passed to OPENROWSET as an EXPLICIT BULK (...)
     list — NOT a folder glob, which would re-read the whole archive every run. append (not merge):
     the file list already excludes anything in {{ this }}, so dedup is done by file selection — a
     key-join merge would be redundant work that scans the target. The duckrun original used
     'safeappend' (DuckDB compare-and-swap); Fabric has no such thing, but the explicit new-file
     list keeps the append idempotent at file grain. No partition_by — Fabric Warehouse has no
     table partitioning; month_key is kept as a plain column. --#}

{#-- Column layout comes from macros/aemo_columns.sql, the single source of truth shared
     by all five engines. --#}
{%- set read_cols = aemo_columns('price') -%}
{%- set num_cols = aemo_cast_columns('price') -%}

{%- set new_files = new_source_files('daily', this if is_incremental() else none) -%}
{%- if is_incremental() and new_files | length == 0 -%}
{#-- No new daily files this run: compile to a zero-row no-op (append inserts nothing). --#}
SELECT * FROM {{ this }} WHERE 1 = 0
{%- else -%}
SELECT
  [UNIT],
  [REGIONID],
  {{ cast_floats(num_cols) }}
  {{ parse_filename('src.filepath()') }} AS [file],
  TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6)) AS [SETTLEMENTDATE],
  TRY_CAST([SETTLEMENTDATE] AS DATE) AS [DATE],
  YEAR(TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6))) AS [YEAR],
  -- Monthly partition key (YYYYMM): low-cardinality column kept for downstream pruning
  -- (Fabric has no native partitioning, but the column is still useful as a filter).
  YEAR(TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6))) * 100
    + MONTH(TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6))) AS [month_key]
FROM {{ openrowset_csv_files(new_files, read_cols) }} AS src
WHERE [I] = 'D' AND [UNIT] = 'DREGION' AND [VERSION] = '3'
{%- endif %}
