{#-- Basename without extension, from a full path expression.

     This is the ONE macro with a dialect branch inside it, rather than the sibling-macro
     pattern used everywhere else, because the call site must stay identical across the five
     model trees — each dialect passes its own way of naming the source file, and the models
     are otherwise the same text:
       DuckDB  parse_filename('filename')          -- read_csv's filename column
       Fabric  parse_filename('src.filepath()')    -- OPENROWSET alias
       Spark   parse_filename('_metadata.file_name')

     The T-SQL branch casts to VARCHAR because Fabric Warehouse cannot store NVARCHAR, which
     is what the string functions return. --#}
{% macro parse_filename(filepath) %}
  {%- if target.type == 'fabric' -%}
    {%- set fn -%}RIGHT({{ filepath }}, CHARINDEX('/', REVERSE({{ filepath }}) + '/') - 1){%- endset -%}
    CAST(LEFT({{ fn }}, CHARINDEX('.', {{ fn }} + '.') - 1) AS VARCHAR(256))
  {%- elif target.type == 'fabricspark' -%}
    substring_index(element_at(split({{ filepath }}, '/'), -1), '.', 1)
  {%- else -%}
    {#-- DuckDB family: duckrun, iceberg, ducklake #}
    split_part(split_part({{ filepath }}, '/', -1), '.', 1)
  {%- endif -%}
{% endmacro %}
