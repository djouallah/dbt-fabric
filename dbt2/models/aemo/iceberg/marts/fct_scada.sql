{#-- AEMO DUNIT record from the daily archive files: all 53 source columns.
     Insert-only merge; see the fct_price.sql and fct_summary.sql headers for why. --#}
{%- set spec = aemo_spec('scada') -%}
{#-- THE PRE-HOOK MUST STAY ONE STRING LITERAL, WITH ITS {{ }} AND {% %} UNEVALUATED.
     dbt renders hooks at RUN time, and only then are `ref()`, `this` and `is_incremental()`
     bound properly. Moving this into a `{%- set -%}` block instead renders it during dbt's
     CONFIG pass, where `ref()` is a stub that returns the MODEL'S OWN relation — the hook
     then silently reads from {{ this }} instead of the archive log, and the run fails with
     "Table with name <this model> does not exist". Verified the hard way. Only
     `source_type`, a parse-time constant, is concatenated in. --#}
{#-- Insert-only merge; see dim_duid.sql for why dbt 2 cannot spell this
     merge_clauses={'when_matched': [{'action': 'do_nothing'}]} the way the other
     four engines do. Same semantics: matched rows are never touched. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    merge_update_condition='false',
    unique_key=spec['unique_key'],
    pre_hook="SET VARIABLE scada_daily_paths = (SELECT COALESCE(NULLIF(list('{{ get_csv_archive_path() }}' || archive_path), []), ['']) FROM (SELECT DISTINCT archive_path FROM {{ ref('stg_csv_archive_log') }} WHERE source_type = '" ~ spec['source_type'] ~ "'{% if is_incremental() %} AND csv_filename NOT IN (SELECT DISTINCT file FROM {{ this }}){% endif %} LIMIT {{ env_var('process_limit', '1000') }}))"
) }}

{%- set check_files_query -%}
SELECT COUNT(*) AS cnt FROM {{ ref('stg_csv_archive_log') }}
WHERE source_type = '{{ spec['source_type'] }}'
{%- if is_incremental() %}
AND csv_filename NOT IN (SELECT DISTINCT file FROM {{ this }})
{%- endif -%}
{%- endset -%}

{%- if execute and flags.WHICH in ('run', 'build', 'retry') -%}
  {%- set files_result = run_query(check_files_query) -%}
  {%- set has_files = files_result and files_result.rows[0][0] > 0 -%}
{%- else -%}
  {%- set has_files = true -%}
{%- endif -%}

{% if has_files %}
WITH scada_staging AS (
  SELECT *
  FROM {{ duckdb_read_csv("getvariable('scada_daily_paths')", 'scada') }}
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
