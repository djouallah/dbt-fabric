-- depends_on: {{ ref('stg_csv_archive_log') }}
{#-- AEMO DUNIT record from the daily archive files: all 53 source columns.
     Insert-only merge; see the fct_price.sql and fct_summary.sql headers for why. --#}
{%- set spec = aemo_spec('scada') -%}
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
{%- set source_paths = duckdb_source_files('scada') -%}
{%- set has_files = source_paths | length > 0 -%}

{% if has_files %}
WITH scada_staging AS (
  SELECT *
  FROM {{ duckdb_read_csv(duckdb_path_list(source_paths), 'scada') }}
  WHERE {{ duckdb_record_filter('scada') }}
)

SELECT
  UNIT,
  DUID,
  {%- for name in aemo_cast_columns('scada') %}
  CAST({{ name }} AS DOUBLE) AS {{ name }},
  {%- endfor %}
  {{ parse_filename('filename') }} AS file,
  CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
  CAST(SETTLEMENTDATE AS DATE) AS DATE,
  CAST(YEAR(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) AS YEAR,
  CAST(YEAR(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) * 100
    + CAST(MONTH(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) AS month_key
FROM scada_staging
{% else %}
-- No unprocessed files: an empty result keeps the existing data untouched.
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
