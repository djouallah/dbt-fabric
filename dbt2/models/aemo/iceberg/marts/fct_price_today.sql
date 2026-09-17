{#-- Intraday DISPATCHIS price: the same 70-column record, landed every 5 minutes.
     Insert-only merge; see the fct_price.sql and fct_summary.sql headers for why. --#}
{%- set spec = aemo_spec('price_today') -%}
{#-- THE PRE-HOOK MUST STAY ONE STRING LITERAL, WITH ITS {{ }} AND {% %} UNEVALUATED.
     dbt renders hooks at RUN time, and only then are `ref()`, `this` and `is_incremental()`
     bound properly. Moving this into a `{%- set -%}` block instead renders it during dbt's
     CONFIG pass, where `ref()` is a stub that returns the MODEL'S OWN relation — the hook
     then silently reads from {{ this }} instead of the archive log, and the run fails with
     "Table with name <this model> does not exist". Verified the hard way. Only
     `source_type`, a parse-time constant, is concatenated in. --#}
{#-- Insert-only merge; the `insert_only` strategy is defined in
     dbt2/macros/incremental_insert_only.sql, and it is there because dbt 2 has no config key
     for "MERGE but do not touch matched rows". Same semantics as the other four engines. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='insert_only',
    unique_key=spec['unique_key'],
    pre_hook="SET VARIABLE price_today_paths = (SELECT COALESCE(NULLIF(list('{{ get_csv_archive_path() }}' || archive_path), []), ['']) FROM (SELECT DISTINCT archive_path FROM {{ ref('stg_csv_archive_log') }} WHERE source_type = '" ~ spec['source_type'] ~ "'{% if is_incremental() %} AND csv_filename NOT IN (SELECT DISTINCT file FROM {{ this }}){% endif %} LIMIT {{ env_var('process_limit', '1000') }}))"
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
WITH price_staging AS (
  SELECT *
  FROM {{ duckdb_read_csv("getvariable('price_today_paths')", 'price_today') }}
  WHERE {{ duckdb_record_filter('price_today') }}
)

SELECT
  REGIONID,
  {%- for name in aemo_cast_columns('price_today') %}
  CAST({{ name }} AS DOUBLE) AS {{ name }},
  {%- endfor %}
  CAST(SETTLEMENTDATE AS TIMESTAMPTZ) AS SETTLEMENTDATE,
  CAST(SETTLEMENTDATE AS DATE) AS DATE,
  {{ parse_filename('filename') }} AS file,
  CAST(YEAR(SETTLEMENTDATE) AS INT) AS YEAR
FROM price_staging
{% else %}
-- No unprocessed files: an empty result keeps the existing data untouched.
SELECT * FROM {{ this }} WHERE FALSE
{% endif %}
