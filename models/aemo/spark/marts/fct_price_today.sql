-- Intraday regional prices (Spark), selected by name from the landed PUBLIC_DISPATCHIS files.
{#-- Column layout comes from macros/aemo_columns.sql, the single source of truth shared
     by all five engines. --#}
{%- set cast_cols = aemo_cast_columns('price_today') -%}
{#-- The CSV read happens in the two pre_hooks and lands in <model>__stage; this body only
     casts and parses. See macros/spark_read_csv.sql and the fct_price.sql header. --#}
{#-- Insert-only merge, not append -- skip_matched_step drops the WHEN MATCHED branch. See the
     fct_price.sql header for why. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='delta',
    unique_key=['file', 'REGIONID', 'SETTLEMENTDATE', 'INTERVENTION'],
    skip_matched_step=true,
    pre_hook=["{{ spark_stage_view('price_today') }}", "{{ spark_stage_table('price_today') }}"],
    post_hook="{{ spark_drop_stage() }}"
) }}

-- depends_on: {{ ref('stg_csv_archive_log') }}

SELECT
  REGIONID,
  {%- for name in cast_cols %}
  CAST({{ name }} AS DOUBLE) AS {{ name }},
  {%- endfor %}
  -- AEMO ships SETTLEMENTDATE as 'yyyy/MM/dd HH:mm:ss'. Spark's CAST(string AS TIMESTAMP)
  -- accepts only yyyy-MM-dd and returns NULL for slashes instead of erroring (non-ANSI mode),
  -- which silently nulled the whole column here. DuckDB and T-SQL both parse slashes, so only
  -- this leg was affected. Parse the format explicitly.
  to_timestamp(SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss') AS SETTLEMENTDATE,
  to_date(SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss') AS DATE,
  {{ parse_filename('_fname') }} AS file,
  CAST(YEAR(to_timestamp(SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss')) AS INT) AS YEAR
FROM {{ spark_stage_relation() }}
-- The stage was already filtered to this record; repeated here as the correctness check.
WHERE I = 'D' AND PRICE = 'PRICE'
