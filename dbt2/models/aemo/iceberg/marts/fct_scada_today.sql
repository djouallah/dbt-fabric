-- depends_on: {{ ref('stg_csv_archive_log') }}
{#-- Intraday DISPATCHSCADA: an 8-column record, landed every 5 minutes. Narrow enough that
     the reader types the columns directly rather than reading all-varchar and casting.
     Insert-only merge; see the fct_price.sql and fct_summary.sql headers for why. --#}
{%- set spec = aemo_spec('scada_today') -%}
{#-- Insert-only merge; the `insert_only` strategy is defined in
     dbt2/macros/incremental_insert_only.sql, and it is there because dbt 2 has no config key
     for "MERGE but do not touch matched rows". Same semantics as the other four engines. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='insert_only',
    unique_key=spec['unique_key']
) }}

{#-- The files this run folds, resolved by a query and inlined as a literal list.
     Replaces the pre_hook + getvariable() the other DuckDB engines use: on dbt 2 a
     pre_hook does not share a session with the model body, so the variable reads back
     NULL. See dbt2/macros/duckdb_source_files.sql. An empty list means nothing new has
     landed, and the model compiles to its no-op branch below. --#}
{%- set source_paths = duckdb_source_files('scada_today') -%}
{%- set has_files = source_paths | length > 0 -%}

{% if has_files %}
WITH scada_staging AS (
  SELECT *
  FROM {{ duckdb_read_csv(duckdb_path_list(source_paths), 'scada_today') }}
  WHERE {{ duckdb_record_filter('scada_today') }}
)

SELECT
  DUID,
  SCADAVALUE AS INITIALMW,
  {{ parse_filename('filename') }} AS file,
  CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
  CAST(LASTCHANGED AS TIMESTAMPTZ) AS LASTCHANGED,
  CAST(SETTLEMENTDATE AS DATE) AS DATE,
  CAST(YEAR(SETTLEMENTDATE) AS INT) AS YEAR
FROM scada_staging
{% else %}
-- No unprocessed files: an empty result keeps the existing data untouched.
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
