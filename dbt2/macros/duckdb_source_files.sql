{#--
    The list of archive files a fact model should fold this run, resolved by a query at
    RENDER time and inlined into the compiled SQL as a literal list.

    WHY NOT THE pre_hook THE OTHER DuckDB ENGINES USE. duckrun and ducklake set a DuckDB
    session variable in a pre_hook and read it back with getvariable() in the model body:

        pre_hook="SET VARIABLE price_daily_paths = (SELECT ... )"
        ... FROM read_csv(getvariable('price_daily_paths'), ...)

    On dbt OSS 2 that returns NULL. A pre_hook does not share a DuckDB session with the model
    body, so the variable the hook sets is simply not there when the model runs. Reproduced
    offline against a local DuckDB on dbt-core 2.0.0-rc.2, at threads 1 and 4 alike, with a
    model whose only job was to read back a variable a pre_hook had just set.

    AND IT FAILS SILENTLY, which is the part worth remembering. The model still SUCCEEDS --
    getvariable() on an unset variable is NULL, not an error. Here it happened to surface,
    because read_csv refuses a NULL list ("read_csv cannot take NULL list as parameter"); a
    model that tolerated NULL would have quietly written wrong data and stayed green. Do not
    reintroduce session state between a hook and a model body on this engine.

    THE `-- depends_on:` COMMENT IN EACH CALLER IS LOAD-BEARING. dbt 2 infers dependencies
    statically and refuses a ref() it can only find inside a conditional:
    "dbt was unable to infer all dependencies for the model ... This typically happens when
    ref() is placed within a conditional block." The ref() below sits inside `{% if %}`, so
    every model calling this macro states the dependency in a comment at the top. Without it
    the model fails to render, and the archive log might not even be built first.
--#}

{% macro duckdb_source_files(record) %}
  {%- set spec = aemo_spec(record) -%}

  {#-- Guarded the way every parse-time run_query in this repo is: `dbt compile`, `ls` and
       `docs generate` must not fire a real query. The else-branch returns an EMPTY list,
       which is the safe direction here -- the caller then compiles to its no-op branch
       rather than to a read_csv over a list it could not resolve. --#}
  {%- if not (execute and flags.WHICH in ('run', 'build', 'retry')) -%}
    {{ return([]) }}
  {%- endif -%}

  {%- set q -%}
    SELECT DISTINCT archive_path
    FROM {{ ref('stg_csv_archive_log') }}
    WHERE source_type = '{{ spec['source_type'] }}'
    {%- if is_incremental() %}
      AND csv_filename NOT IN (SELECT DISTINCT file FROM {{ this }})
    {%- endif %}
    ORDER BY archive_path
    LIMIT {{ env_var('process_limit', '1000') }}
  {%- endset -%}

  {%- set root = get_csv_archive_path() -%}
  {%- set paths = [] -%}
  {%- for archive_path in run_query(q).columns[0].values() -%}
    {%- do paths.append(root ~ archive_path) -%}
  {%- endfor -%}
  {{ return(paths) }}
{% endmacro %}


{#-- Render a list of paths as a DuckDB list literal, for read_csv's first argument.

     Single quotes are doubled rather than rejected: these are OneLake paths built from AEMO
     filenames, so a quote is not expected, but silently producing broken SQL if one ever
     appears is worse than the two characters this costs. --#}
{% macro duckdb_path_list(paths) %}
  {%- set quoted = [] -%}
  {%- for p in paths -%}
    {%- do quoted.append("'" ~ (p | replace("'", "''")) ~ "'") -%}
  {%- endfor -%}
  [{{ quoted | join(', ') }}]
{% endmacro %}
