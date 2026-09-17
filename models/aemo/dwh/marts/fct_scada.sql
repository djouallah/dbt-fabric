{{ config(
    materialized='incremental',
    incremental_strategy='append'
) }}

{#-- Reads the new AEMO daily files, filtering to the DUNIT SCADA records. The set of files is
     resolved from the archive log (new_source_files) and passed to OPENROWSET as an EXPLICIT
     BULK (...) list — NOT a folder glob, which would re-read the whole archive every run. append
     stays idempotent at file grain: the file list already excludes anything in {{ this }} (see
     fct_price for the full rationale on why this replaces duckrun's safeappend). No partition_by
     in Fabric; month_key kept as a plain column. --#}

{#-- Column layout comes from macros/aemo_columns.sql, the single source of truth shared
     by all five engines. --#}
{%- set read_cols = aemo_columns('scada') -%}
{%- set num_cols = aemo_cast_columns('scada') -%}

{%- set new_files = new_source_files('daily', this if is_incremental() else none) -%}
{%- if is_incremental() and new_files | length == 0 -%}
{#-- No new daily files this run: compile to a zero-row no-op (append inserts nothing). --#}
SELECT * FROM {{ this }} WHERE 1 = 0
{%- else -%}
SELECT
  [UNIT],
  [DUID],
  {{ cast_floats(num_cols) }}
  {{ parse_filename('src.filepath()') }} AS [file],
  TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6)) AS [SETTLEMENTDATE],
  TRY_CAST([SETTLEMENTDATE] AS DATE) AS [DATE],
  YEAR(TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6))) AS [YEAR],
  YEAR(TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6))) * 100
    + MONTH(TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6))) AS [month_key]
FROM {{ openrowset_csv_files(new_files, read_cols) }} AS src
WHERE [I] = 'D' AND [UNIT] = 'DUNIT' AND [VERSION] = '3'
{%- endif %}
