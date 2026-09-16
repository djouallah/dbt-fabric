{#-- The model's +schema (landing / mart) used to be returned VERBATIM in all four source
     repos, which made target.schema (DBT_SCHEMA) dead config: no profile or env var could
     redirect a run away from the production schemas. That bites for real the moment a test
     run points at a catalog that already holds the real data — the "test" merges straight
     into production landing/mart because nothing can point it elsewhere.

     target.schema is the redirect lever instead:
       DBT_SCHEMA unset/'mart' -> +schema verbatim            (landing, mart)
       DBT_SCHEMA=anything_else -> '<DBT_SCHEMA>_<+schema>'   (dbt_landing, dbt_mart)

     All five engines go through this macro, so one env var isolates a whole run. --#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.schema == 'mart' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema ~ '_' ~ (custom_schema_name | trim) }}
    {%- endif -%}
{%- endmacro %}
