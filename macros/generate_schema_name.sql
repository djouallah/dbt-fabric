{#-- TWO THINGS ARE FOLDED INTO THE SCHEMA NAME HERE, AND BOTH MATTER.

     1. THE ENGINE PREFIX. All five engines write into ONE shared Fabric lakehouse (`dbt`),
        so the engine name is what keeps them apart: duckrun_mart, iceberg_mart,
        ducklake_mart, spark_mart, and dwh_mart over in the Warehouse. Before this they had
        an item each and could share the bare `mart`; they cannot now. Two engines resolving
        to the same schema would have them overwriting each other's gold layer inside one
        item, with every test still green — so this prefix is load-bearing, not cosmetic,
        and check_gating.py asserts it offline.

     2. THE ISOLATION LEVER. The model's +schema (landing / mart) used to be returned
        VERBATIM in all four source repos, which made target.schema (DBT_SCHEMA) dead
        config: no profile or env var could redirect a run away from the production
        schemas. That bites for real the moment a test run points at a catalog that already
        holds the real data — the "test" merges straight into production.

       DBT_SCHEMA unset/'mart' -> '<engine>_<layer>'               (iceberg_landing, iceberg_mart)
       DBT_SCHEMA=anything_else -> '<DBT_SCHEMA>_<engine>_<layer>' (test_iceberg_landing)

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
