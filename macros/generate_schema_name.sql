{#-- TWO THINGS ARE FOLDED INTO THE SCHEMA NAME HERE, AND BOTH MATTER.

     1. THE ENGINE PREFIX. The engine name keeps the two gold layers apart: spark_mart in the
        shared `dbt` lakehouse, dwh_mart in the Warehouse. Two engines resolving to the same
        schema would have them overwriting each other's gold layer, with every test still
        green — so this prefix is load-bearing, not cosmetic, and check_gating.py asserts it
        offline.

     2. THE ISOLATION LEVER. Returning the model's +schema (landing / mart) VERBATIM
        would make target.schema (DBT_SCHEMA) dead config: no profile or env var could
        redirect a run away from the production schemas. That bites for real the moment a test run points at a catalog that already
        holds the real data — the "test" merges straight into production.

       DBT_SCHEMA unset/'mart' -> '<engine>_<layer>'               (spark_landing, spark_mart)
       DBT_SCHEMA=anything_else -> '<DBT_SCHEMA>_<engine>_<layer>' (test_spark_landing)

     The `custom_schema_name is none` branch folds into 'mart' rather than returning
     target.schema verbatim: verbatim would put a bare `mart` schema OUTSIDE the engine
     namespace, which is the one thing this macro exists to prevent. --#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set layer = (custom_schema_name | trim) if custom_schema_name is not none else 'mart' -%}
    {%- if target.schema == 'mart' -%}
        {{ target.name ~ '_' ~ layer }}
    {%- else -%}
        {{ target.schema ~ '_' ~ target.name ~ '_' ~ layer }}
    {%- endif -%}
{%- endmacro %}
