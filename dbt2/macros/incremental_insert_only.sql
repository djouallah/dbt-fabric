{#--
    A custom incremental strategy: MERGE that inserts new keys and never touches matched rows.

    WHY THIS EXISTS. The other four engines say the same thing in config:

        incremental_strategy='merge',
        merge_clauses={'when_matched': [{'action': 'do_nothing'}]}

    dbt OSS 2 cannot. `merge_clauses` is not in its model-config schema
    (crates/dbt-schemas/src/schemas/project/configs/model_config.rs accepts only
    merge_update_columns, merge_exclude_columns and merge_with_schema_evolution), and
    UnusedConfigKey (dbt1060) is a HARD parse error in v2 that warn_error_options cannot
    downgrade. The irony is that v2's own duckdb__get_merge_sql DOES read
    config.get('merge_clauses') and implements a do_nothing action -- the macro supports a key
    the schema forbids, so there is no way to reach it from a model.

    Nor is there a way to neuter the matched branch through the keys that ARE allowed:
    `merge_update_columns` / `merge_exclude_columns` route to the 'explicit' update mode, and
    an empty column list there renders a bare `UPDATE SET` with nothing after it.

    THIS IS NOT A BEHAVIOUR CHANGE. dbt's custom-strategy hook resolves
    `incremental_strategy='insert_only'` to this macro by name, and the SQL below is what the
    other four engines' config compiles to. Matched rows are left exactly as they are; only
    unmatched keys insert.

    AND IT IS WHAT THE CATALOG REQUIRES, not a preference. A matched UPDATE makes DuckDB write
    positional delete files alongside the new data files, and the OneLake Iceberg REST catalog
    rejects that commit with BadRequest 400, "Only one instance of each update type is allowed
    per request". Every commit from this strategy is a single append snapshot.

    The join clause is written out here rather than delegated to dbt-duckdb's own
    `duckdb__merge_join_clause`: that macro is an internal of the bundled adapter, and a
    project macro calling it would bind this leg to a name dbt is free to rename. The shape is
    the same one it produces.
--#}

{% macro get_incremental_insert_only_sql(arg_dict) -%}
  {%- set target = arg_dict['target_relation'] -%}
  {%- set source = arg_dict['temp_relation'] -%}
  {%- set unique_key = arg_dict['unique_key'] -%}
  {%- set predicates = arg_dict.get('incremental_predicates') or [] -%}

  {%- if not unique_key -%}
    {{ exceptions.raise_compiler_error(
         "incremental_strategy='insert_only' needs a unique_key: it is the MERGE join "
         ~ "condition, and without one every row is unmatched and every run re-inserts the "
         ~ "whole batch. Model: " ~ target) }}
  {%- endif -%}

  {#-- A single string key is legal; every model here passes a list. --#}
  {%- if unique_key is string -%}
    {%- set keys = [unique_key] -%}
  {%- else -%}
    {%- set keys = unique_key -%}
  {%- endif -%}

  {%- set join_predicates = [] -%}
  {%- for key in keys -%}
    {%- do join_predicates.append("DBT_INTERNAL_SOURCE." ~ key ~ " = DBT_INTERNAL_DEST." ~ key) -%}
  {%- endfor -%}

  MERGE INTO {{ target }} AS DBT_INTERNAL_DEST
    USING {{ source }} AS DBT_INTERNAL_SOURCE
    ON ({{ (join_predicates + predicates) | join(") AND (") }})

  {#-- Spelled out rather than omitted. An absent WHEN MATCHED behaves identically, but this
       way the intent is legible in the query log and in a Fabric capacity trace, and it reads
       the same as the merge_clauses the other four engines declare. --#}
  WHEN MATCHED THEN
    DO NOTHING

  {#-- BY NAME, not BY POSITION: the temp relation is built from the model's own SELECT, so
       the names line up and the column ORDER does not have to. --#}
  WHEN NOT MATCHED THEN
    INSERT BY NAME

{%- endmacro %}
