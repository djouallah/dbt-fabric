-- Generation SCADA from the daily AEMO files (Spark). from_csv over the landed CSV folder
-- with an explicit schema; select by column name.
{#-- Column layout comes from macros/aemo_columns.sql, the single source of truth shared
     by all five engines. --#}
{%- set csv_cols = aemo_columns('scada') -%}
{%- set cast_cols = aemo_cast_columns('scada') -%}
{%- set view_schema %}{% for c in csv_cols %}`{{ c }}` STRING{{ ', ' if not loop.last }}{% endfor %}{% endset %}
{#-- The raw CSVs are read inline via from_csv over the text.`path` datasource — no pre-created
     raw object AT ALL. This is NOT because Spark has no CSV reader: the obvious fix, and the
     one a notebook uses, is `spark.read.format("csv").schema(user_schema).load(paths)`, which
     is vectorized and prunes columns the way DuckDB's read_csv does. dbt cannot reach it.
     Both catalog-object approaches are illegal on a schema-enabled lakehouse:
     dbt-fabricspark builds its <model>__dbt_tmp intermediate as a PERSISTENT view, which cannot
     reference a TEMPORARY VIEW (INVALID_TEMP_OBJ_REFERENCE), and Fabric Spark rejects an external
     CSV table with an explicit schema ("External tables with partition columns or schema or
     properties are not supported"). A path scan is not a catalog object, so the persistent tmp
     view may reference it, and from_csv carries the explicit schema the ragged AEMO rows need.
     On incremental runs the path is an explicit brace glob of THIS RUN's new files (see
     spark_new_files) — a bare folder scan re-read the whole archive every run and took 30+
     minutes. A first/full-refresh build DOES read the bare folder: everything is new then. --#}
{#-- Read shape is picked by is_incremental(); see the long note in fct_price.sql. Short form:
     the non-incremental path is a bare CTAS with no __dbt_tmp, and a CTAS may reference a
     TEMPORARY VIEW, so it gets the real CSV datasource. The incremental path goes through a
     persistent __dbt_tmp view, which may not, so it keeps from_csv over text.`path`. --#}
{%- set daily_root = get_csv_archive_path() ~ '/daily' -%}
{%- set raw_view = 'raw_daily_scada' -%}
{#-- Insert-only merge, not append -- skip_matched_step drops the WHEN MATCHED branch. See the
     fct_price.sql header for why, and for why the CSV read below is unaffected. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    file_format='delta',
    unique_key=['file', 'DUID', 'SETTLEMENTDATE', 'INTERVENTION'],
    skip_matched_step=true,
    pre_hook=(none if is_incremental() else
      "CREATE OR REPLACE TEMPORARY VIEW " ~ raw_view ~ " (" ~ view_schema ~ ")"
      ~ " USING csv OPTIONS (path '" ~ daily_root ~ "', header 'true', mode 'PERMISSIVE')")
) }}

-- depends_on: {{ ref('stg_csv_archive_log') }}

{% set new_files = spark_new_files('daily', this) if is_incremental() else [] %}
{#-- Plain (non-trimming) tags: {%- -%} here would eat the newline that ends the depends_on
     comment above and glue `WITH raw AS (` onto it, commenting out the CTE header. --#}
{% if is_incremental() and new_files | length == 0 %}
{#-- No new daily files this run: compile to a zero-row no-op (the merge source is empty). --#}
SELECT * FROM {{ this }} WHERE 1 = 0
{% elif not is_incremental() %}
{#-- CTAS path: columns arrive already split from the temp view, so no `r.` prefix, and the
     file name comes from input_file_name() — a view with a declared column list does not
     expose _metadata. --#}
SELECT
  UNIT,
  DUID,
  {%- for name in cast_cols %}
  CAST({{ name }} AS DOUBLE) AS {{ name }},
  {%- endfor %}
  {{ parse_filename('input_file_name()') }} AS file,
  -- See the SETTLEMENTDATE note in the incremental branch below.
  to_timestamp(SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss') AS SETTLEMENTDATE,
  to_date(SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss') AS DATE,
  CAST(YEAR(to_timestamp(SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss')) AS INT) AS YEAR,
  {#-- YYYYMM partition key. Emitted by all five engines so the fact schemas match. --#}
  CAST(YEAR(to_timestamp(SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss')) AS INT) * 100 + CAST(MONTH(to_timestamp(SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss')) AS INT) AS month_key
FROM {{ raw_view }}
WHERE I = 'D' AND UNIT = 'DUNIT' AND VERSION = '3'
{% else %}
WITH raw AS (
  SELECT
    from_csv(value, '{{ view_schema }}', map('mode', 'PERMISSIVE')) AS r,
    _metadata.file_name AS _fname
  FROM text.`{{ get_csv_archive_path() }}/daily/{{ '{' ~ new_files | join(',') ~ '}' }}`
  {# Non-trimming comment tags on purpose; the trimming form glues WHERE onto the backtick
     path line. See the longer note on the same guard in fct_price.sql.
     Discard non-DUNIT lines BEFORE from_csv.
     DUNIT is a much larger share of a PUBLIC_DAILY file than DREGION is, so the win here is
     smaller, but it still skips a 53-column parse per rejected row. #}
  WHERE value LIKE 'D,DUNIT,%'
)
SELECT
  r.UNIT,
  r.DUID,
  {%- for name in cast_cols %}
  CAST(r.{{ name }} AS DOUBLE) AS {{ name }},
  {%- endfor %}
  {{ parse_filename('_fname') }} AS file,
  -- AEMO ships SETTLEMENTDATE as 'yyyy/MM/dd HH:mm:ss'. Spark's CAST(string AS TIMESTAMP)
  -- accepts only yyyy-MM-dd and returns NULL for slashes instead of erroring (non-ANSI mode),
  -- which silently nulled the whole column here. DuckDB and T-SQL both parse slashes, so only
  -- this leg was affected. Parse the format explicitly.
  to_timestamp(r.SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss') AS SETTLEMENTDATE,
  to_date(r.SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss') AS DATE,
  CAST(YEAR(to_timestamp(r.SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss')) AS INT) AS YEAR,
  {#-- YYYYMM partition key. Emitted by all five engines so the fact schemas match. --#}
  CAST(YEAR(to_timestamp(r.SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss')) AS INT) * 100 + CAST(MONTH(to_timestamp(r.SETTLEMENTDATE, 'yyyy/MM/dd HH:mm:ss')) AS INT) AS month_key
FROM raw
WHERE r.I = 'D' AND r.UNIT = 'DUNIT' AND r.VERSION = '3'
{% endif %}
