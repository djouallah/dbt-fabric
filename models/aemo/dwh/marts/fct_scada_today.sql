{{ config(
    materialized='incremental',
    incremental_strategy='append',
    schema='landing'
) }}

{#-- Intraday SCADA — reads the new scada_today files. The file set comes from the archive log
     (new_source_files) and is passed to OPENROWSET as an EXPLICIT BULK (...) list, not a folder
     glob (~288 small files land per day; globbing would re-read the whole archive each run). The
     list already excludes files in {{ this }}, so the append stays idempotent at file grain. --#}

{#-- Column layout comes from macros/aemo_columns.sql, the single source of truth shared
     by all five engines. It used to be a copy of the list in this file; the four copies
     were verified byte-identical as data before they were collapsed. --#}
{%- set read_cols = aemo_columns('scada_today') -%}

{%- set new_files = new_source_files('scada_today', this if is_incremental() else none) -%}
{%- if is_incremental() and new_files | length == 0 -%}
{#-- No new scada_today files this run: compile to a zero-row no-op. --#}
SELECT * FROM {{ this }} WHERE 1 = 0
{%- else -%}
SELECT
  [DUID],
  TRY_CAST([SCADAVALUE] AS FLOAT) AS [INITIALMW],
  {{ parse_filename('src.filepath()') }} AS [file],
  TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6)) AS [SETTLEMENTDATE],
  TRY_CAST([LASTCHANGED] AS DATETIME2(6)) AS [LASTCHANGED],
  TRY_CAST([SETTLEMENTDATE] AS DATE) AS [DATE],
  YEAR(TRY_CAST([SETTLEMENTDATE] AS DATETIME2(6))) AS [YEAR]
FROM {{ openrowset_csv_files(new_files, read_cols) }} AS src
WHERE [I] = 'D' AND TRY_CAST([SCADAVALUE] AS FLOAT) <> 0
{%- endif %}
