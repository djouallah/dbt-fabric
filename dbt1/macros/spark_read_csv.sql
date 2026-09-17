{#-- Spark-side helpers: how the four spark fact models read the landed CSVs.

     ONE read shape, for the first build and for every incremental run:

       pre_hook 1  spark_stage_view(record)   CREATE OR REPLACE TEMPORARY VIEW raw_<record>
                                              (<explicit STRING schema>) USING csv OPTIONS
                                              (path '<folder>/{this run's files}', header, PERMISSIVE)
       pre_hook 2  spark_stage_table(record)  CREATE OR REPLACE TABLE <model>__stage USING DELTA
                                              AS SELECT *, input_file_name() FROM raw_<record>
                                              WHERE <record filter>
       model body                             SELECT <casts> FROM <model>__stage
       post_hook   spark_drop_stage()         DROP TABLE IF EXISTS <model>__stage

     Why a stage TABLE and not a direct read. On a schema-enabled lakehouse dbt-fabricspark
     builds the incremental <model>__dbt_tmp as a PERSISTENT view (its incremental.sql says so:
     a temp view there trips REQUIRES_SINGLE_PART_NAMESPACE during the DML), and a persistent
     view may not reference a TEMPORARY view (INVALID_TEMP_OBJ_REFERENCE) -- so the model body
     cannot read the csv temp view directly. The previous workaround, from_csv(value) over the
     `text` path datasource, never worked either: Fabric's catalog base32hex-decodes every part
     of a multipart name and the word text contains an x, which is outside that alphabet
     (0-9, A-V), so every incremental run died with "Failed to decode multipart name: 'text'
     ... Unrecognized character: x". (The parquet path datasource survives only because every
     letter of the word parquet happens to be inside it.) A CTAS may read a temp view -- that is
     exactly how the first build always worked -- and a persistent view may read a persistent
     table. Hence the stage.

     Why not spark.read.format("csv").schema(...): dbt cannot reach it, and Fabric rejects an
     external CSV TABLE with an explicit schema ("External tables with partition columns or
     schema or properties are not supported"). The temp view is the one form that carries the
     explicit schema the ragged AEMO rows need without being a catalog object.

     Both hooks resolve THIS RUN's file list at run time (spark_new_files, capped at
     process_limit) and render to nothing when there is nothing new, which dbt skips. The
     stage is then created EMPTY so the model body always has something to read; the merge
     source is zero rows and the target is untouched. --#}

{#-- The backtick-quoted STRING schema both the temp view and the empty stage need. --#}
{% macro spark_csv_schema(record) %}
  {%- set parts = [] -%}
  {%- for c in aemo_columns(record) %}{% do parts.append('`' ~ c ~ '` STRING') %}{% endfor -%}
  {{ return(parts | join(', ')) }}
{% endmacro %}


{#-- The files this model folds this run: all of the source type on a first build, else the
     ones {{ this }} does not hold yet. Same rule as every other engine. --#}
{% macro spark_stage_files(record) %}
  {%- set spec = aemo_spec(record) -%}
  {{ return(spark_new_files(spec['source_type'], this if is_incremental() else none)) }}
{% endmacro %}


{#-- <model>__stage, in the model's own lakehouse.schema -- the same way dbt-fabricspark
     names <model>__dbt_tmp, so it renders with the same quoting as {{ this }}. --#}
{% macro spark_stage_relation() %}
  {{ return(this.incorporate(path={'identifier': this.identifier ~ '__stage'})) }}
{% endmacro %}


{#-- pre_hook 1: the csv temp view over an explicit brace glob of this run's files. --#}
{% macro spark_stage_view(record) %}
  {%- set files = spark_stage_files(record) -%}
  {%- if files | length == 0 -%}{{ return('') }}{%- endif -%}
  {%- set spec = aemo_spec(record) -%}
  {%- set folder = get_csv_archive_path() ~ '/' ~ spec['source_type'] -%}
  {%- if files | length == 1 -%}
    {%- set path = folder ~ '/' ~ files[0] -%}
  {%- else -%}
    {%- set path = folder ~ '/{' ~ files | join(',') ~ '}' -%}
  {%- endif -%}
  CREATE OR REPLACE TEMPORARY VIEW raw_{{ record }} ({{ spark_csv_schema(record) }})
  USING csv OPTIONS (path '{{ path }}', header 'true', mode 'PERMISSIVE')
{% endmacro %}


{#-- pre_hook 2: materialise the record's rows into the stage table. The record filter runs
     HERE, on the raw strings, so the stage holds only DREGION/DUNIT/PRICE/SCADA rows -- a
     PUBLIC_DAILY file is mostly other report types. input_file_name() is captured now: it is
     the only place the provenance exists, and the models' `file` column is parsed from it. --#}
{% macro spark_stage_table(record) %}
  {%- set files = spark_stage_files(record) -%}
  {%- set stage = spark_stage_relation() -%}
  {%- if files | length == 0 -%}
  CREATE OR REPLACE TABLE {{ stage }} ({{ spark_csv_schema(record) }}, `_fname` STRING) USING DELTA
  {%- else -%}
  CREATE OR REPLACE TABLE {{ stage }} USING DELTA AS
  SELECT *, input_file_name() AS _fname
  FROM raw_{{ record }}
  WHERE {{ spark_record_filter(record) }}
  {%- endif -%}
{% endmacro %}


{#-- post_hook: the stage is scratch. A run that dies before this leaves one behind; the next
     run's CREATE OR REPLACE overwrites it. --#}
{% macro spark_drop_stage() %}
  DROP TABLE IF EXISTS {{ spark_stage_relation() }}
{% endmacro %}


{#-- The record-selection predicate, Spark quoting. Same rule as every other dialect.

     The `nonzero` column MUST be cast before the comparison. This predicate runs on the csv
     temp view, where every column is STRING, and Spark resolves `STRING != 0` by casting the
     STRING to the literal's type -- INT -- and that cast TRUNCATES a decimal fraction
     (TypeCoercion.findCommonTypeForBinaryComparison + UTF8String.toInt). So `'0.5' != 0` and
     `'-0.3' != 0` are both FALSE, and a bare `SCADAVALUE != 0` silently dropped every intraday
     SCADA row with 0 < |value| < 1 -- 12-16% of the non-zero rows (solar and battery aux load,
     wind at low speed). The DuckDB reader types the column as double and dwh TRY_CASTs to
     FLOAT, so only this leg lost them: fct_scada_today was 27,757 rows against 31,803 on the
     other four, and fct_summary's intraday tail ran 200-1,500 rows short on every run. --#}
{% macro spark_record_filter(record, prefix='') %}
  {%- set spec = aemo_spec(record) -%}
  {%- set parts = [] -%}
  {%- for col, val in spec['equals'] %}{% do parts.append(prefix ~ col ~ " = '" ~ val ~ "'") %}{% endfor -%}
  {%- if spec['nonzero'] %}{% do parts.append('CAST(' ~ prefix ~ spec['nonzero'] ~ ' AS DOUBLE) != 0') %}{% endif -%}
  {{ parts | join(' AND ') }}
{% endmacro %}
