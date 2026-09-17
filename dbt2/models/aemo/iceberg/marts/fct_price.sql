-- depends_on: {{ ref('stg_csv_archive_log') }}
{#-- AEMO DREGION record from the daily archive files: all 130 source columns.

     Insert-only merge (WHEN MATCHED DO NOTHING): every commit stays a single append
     snapshot — the OneLake catalog rejects multi-snapshot commits, see the fct_summary.sql
     header — while re-processed files dedupe on the unique_key instead of double-inserting.
     That only holds ACROSS batches: MERGE inserts every not-matched source row, so the
     SELECT DISTINCT in duckdb_source_files() is what stops one file being read twice
     WITHIN a batch (the log is append-only and lists a file once per run it waited). --#}
{%- set spec = aemo_spec('price') -%}
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
{%- set source_paths = duckdb_source_files('price') -%}
{%- set has_files = source_paths | length > 0 -%}

{% if has_files %}
WITH price_staging AS (
  SELECT *
  FROM {{ duckdb_read_csv(duckdb_path_list(source_paths), 'price') }}
  WHERE {{ duckdb_record_filter('price') }}
)

SELECT
  UNIT,
  REGIONID,
  {%- for name in aemo_cast_columns('price') %}
  CAST({{ name }} AS DOUBLE) AS {{ name }},
  {%- endfor %}
  {{ parse_filename('filename') }} AS file,
  CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
  CAST(SETTLEMENTDATE AS DATE) AS DATE,
  CAST(YEAR(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) AS YEAR,
  {#-- Monthly partition key (YYYYMM). Low-cardinality, deterministic, and a useful filter on
       every engine — it is emitted on both daily facts by all five so the fact schemas match. --#}
  CAST(YEAR(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) * 100
    + CAST(MONTH(CAST(SETTLEMENTDATE AS TIMESTAMP)) AS INT) AS month_key
FROM price_staging
{% else %}
-- No unprocessed files: an empty result keeps the existing data untouched.
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
