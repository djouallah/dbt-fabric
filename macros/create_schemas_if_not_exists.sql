{#-- on-run-start hook: Fabric Warehouse does not auto-create schemas, and dbt-fabric's
     create-schema runs per-model anyway, but the landing/mart schemas must exist before
     the FIRST model (and before the downloader inserts into landing.csv_archive_log).
     Idempotent: only creates each schema when missing. CREATE SCHEMA must be the only
     statement in its batch, hence the EXEC.

     TAKES LAYERS ('landing', 'mart'), NOT FINISHED SCHEMA NAMES, and resolves each through
     generate_schema_name() — the same function the models go through. Passing literals
     here would create `landing`/`mart` while the models wrote to `dwh_landing`/`dwh_mart`,
     and the run would then fail at the first model on a schema that does not exist. One
     resolver, so the hook cannot drift from the models it is preparing for. --#}
{% macro create_schemas_if_not_exists(layers) %}
  {%- if execute -%}
    {%- for layer in layers -%}
      {%- set s = generate_schema_name(layer, none) -%}
      {% set sql %}
        IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{{ s }}')
          EXEC('CREATE SCHEMA [{{ s }}]');
      {% endset %}
      {%- do run_query(sql) -%}
    {%- endfor -%}
  {%- endif -%}
{% endmacro %}
