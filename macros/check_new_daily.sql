{% macro check_new_daily() %}
  {#-- Run-operation the dwh leg's runner (build.yml) calls to decide whether fct_summary is
       REBUILT this run. Ported from dbt_fabric_python_dwh, which is where the dwh fct_summary
       model comes from: that model's rebuild-vs-append choice is made by the RUNNER through
       `--vars '{rebuild_summary: true}'` (delete+insert, never --full-refresh). Without this
       probe nothing ever set the var -- the summary appended intraday forever while the daily
       backfill kept adding days to fct_scada, and assert_summary_covers_all_scada_days failed
       from the second run on.

       "New daily" = daily files already in the archive log but NOT yet ingested into fct_scada,
       i.e. landing this run. It must run BEFORE `dbt build` ingests them.

       Signals through the run-operation's exit status, exactly as the original did:
         - quiet success  -> no new daily -> fct_summary appends intraday
         - raises / fails  -> new daily pending -> runner builds with rebuild_summary: true

       Reads the archive log straight from Files/csv_raw_archive_log.parquet (OPENROWSET, the
       same read dwh's stg_csv_archive_log does) and compares against fct_scada, with an
       OBJECT_ID guard so it is safe on the very first run before fct_scada exists. --#}
  {%- if execute -%}
    {%- if target.name != 'dwh' -%}
      {{ exceptions.raise_compiler_error("check_new_daily is the dwh leg's probe; the DuckDB and Spark fct_summary models decide for themselves") }}
    {%- endif -%}
    {%- set log_path = get_root_path() ~ '/csv_raw_archive_log.parquet' -%}
    {%- set scada = ref('fct_scada') -%}
    {#-- Probe table existence in a separate query first: a single CASE referencing fct_scada
         binds BOTH branches at compile time, so it errors with "Invalid object name" on the
         very first (cold) run before fct_scada exists. --#}
    {%- set probe = "SELECT OBJECT_ID('" ~ scada.schema ~ "." ~ scada.identifier ~ "', 'U') AS oid" -%}
    {%- set fct_scada_exists = run_query(probe).rows[0][0] is not none -%}
    {%- if fct_scada_exists -%}
      {%- set q -%}
        SELECT COUNT(*) AS n FROM OPENROWSET(BULK '{{ log_path }}', FORMAT = 'PARQUET') AS l
        WHERE l.source_type = 'daily'
          AND l.csv_filename NOT IN (SELECT DISTINCT [file] FROM {{ scada }})
      {%- endset -%}
    {%- else -%}
      {%- set q -%}
        SELECT COUNT(*) AS n FROM OPENROWSET(BULK '{{ log_path }}', FORMAT = 'PARQUET') AS l
        WHERE l.source_type = 'daily'
      {%- endset -%}
    {%- endif -%}
    {%- set n = run_query(q).rows[0][0] -%}
    {{ log("pipeline: new daily files pending = " ~ n, info=true) }}
    {%- if n and n > 0 -%}
      {{ exceptions.raise_compiler_error("NEW_DAILY_PENDING") }}
    {%- endif -%}
  {%- endif -%}
{% endmacro %}
