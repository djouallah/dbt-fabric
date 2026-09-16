{#-- Spark-side helpers. There is no `spark_read_csv` that mirrors duckdb_read_csv one-to-one,
     because Spark needs TWO different read shapes in the same model and the choice is made by
     is_incremental() — so the models call these three small pieces instead.

     Why not just `spark.read.format("csv").schema(...)`, which is what a notebook would do and
     what prunes columns the way DuckDB's read_csv does? dbt cannot reach it. Both catalog-object
     routes are illegal on a schema-enabled lakehouse: dbt-fabricspark builds its <model>__dbt_tmp
     intermediate as a PERSISTENT view, which may not reference a TEMPORARY VIEW
     (INVALID_TEMP_OBJ_REFERENCE), and Fabric Spark rejects an external CSV table with an explicit
     schema ("External tables with partition columns or schema or properties are not supported").

     So:
       first build / --full-refresh -> a bare CTAS, no __dbt_tmp, so a TEMPORARY VIEW is legal
                                       -> spark_csv_view() in a pre_hook, the real CSV datasource
       incremental                  -> goes through the persistent __dbt_tmp view
                                       -> from_csv() over the text.`path` datasource, which is a
                                          path scan rather than a catalog object --#}

{#-- The backtick-quoted STRING schema both read shapes need. --#}
{% macro spark_csv_schema(record) %}
  {%- set cols = aemo_columns(record) -%}
  {{ return(cols | map('regex_replace', '^(.*)$', '`\1` STRING') | join(', ')) }}
{% endmacro %}


{#-- pre_hook body for the non-incremental branch: a real CSV datasource over the folder. --#}
{% macro spark_csv_view(view_name, folder, record) %}
  {%- set schema = spark_csv_schema(record) -%}
  CREATE OR REPLACE TEMPORARY VIEW {{ view_name }} ({{ schema }})
  USING csv OPTIONS (path '{{ folder }}', header 'true', mode 'PERMISSIVE')
{% endmacro %}


{#-- The record-selection predicate, Spark quoting. Same rule as every other dialect. --#}
{% macro spark_record_filter(record, prefix='') %}
  {%- set spec = aemo_spec(record) -%}
  {%- set parts = [] -%}
  {%- for col, val in spec['equals'] %}{% do parts.append(prefix ~ col ~ " = '" ~ val ~ "'") %}{% endfor -%}
  {%- if spec['nonzero'] %}{% do parts.append(prefix ~ spec['nonzero'] ~ ' != 0') %}{% endif -%}
  {{ parts | join(' AND ') }}
{% endmacro %}


{#-- The T-SQL predicate, bracket quoting. Kept beside the others so the three renderings of
     one rule are read together; dwh models call this one. --#}
{% macro tsql_record_filter(record, prefix='') %}
  {%- set spec = aemo_spec(record) -%}
  {%- set parts = [] -%}
  {%- for col, val in spec['equals'] %}{% do parts.append(prefix ~ '[' ~ col ~ "] = '" ~ val ~ "'") %}{% endfor -%}
  {%- if spec['nonzero'] %}{% do parts.append(prefix ~ '[' ~ spec['nonzero'] ~ '] != 0') %}{% endif -%}
  {{ parts | join(' AND ') }}
{% endmacro %}
