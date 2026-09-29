{#--
    Emit a comparable fingerprint of the gold table, for parity checking across engines.

    THIS IS THE MEASUREMENT THE REPO EXISTS TO MAKE. Two adapters run the same business
    logic; this is what turns "the same" from a claim into a check.

    Run per engine, and capture stdout:
        dbt run-operation parity_fingerprint --target <engine> --profiles-dir . \
            | python .github/scripts/parity.py capture history/parity
    then compare with `parity.py compare history/parity`.

    Aggregates rather than a row-by-row diff on purpose: each engine reports through its OWN
    adapter (a Fabric Warehouse, a Fabric Lakehouse), and only the numbers are compared.

    Two known reasons the numbers can differ WITHOUT the logic differing, both measured
    elsewhere and both handled in parity.py rather than here:
      * T-SQL pads strings on comparison ('ERB01' = 'ERB01 ' is TRUE); Spark does not. A single trailing space in a join key split the engines for over a year.
      * DOUBLE -> DECIMAL tie-breaking is HALF_UP on Spark and something else in T-SQL, so the money columns get a relative tolerance, not equality.
--#}

{% macro parity_fingerprint(model='fct_summary') %}
  {%- if not execute -%}{{ return('') }}{%- endif -%}

  {%- set rel = ref(model) -%}
  {#-- `dbl` is per-dialect for the same reason the date casts are: Spark SQL has no
       DOUBLE PRECISION and rejects it outright ("extra input 'PRECISION'"), which failed
       the spark leg AFTER a clean 60/62 build -- the fingerprint is the last step, so a
       dialect slip here throws away the whole run's measurement. T-SQL has no DOUBLE. --#}
  {%- if target.name == 'dwh' -%}
    {%- set d, t, mw, price = '[date]', '[time]', 'mw', 'price' -%}
    {%- set dbl = 'FLOAT' -%}
    {%- set to_text = "CONVERT(VARCHAR(10), MIN([date]), 23)" -%}
    {%- set to_text_max = "CONVERT(VARCHAR(10), MAX([date]), 23)" -%}
  {%- elif target.name == 'spark' -%}
    {%- set d, t, mw, price = '`date`', '`time`', 'mw', 'price' -%}
    {%- set dbl = 'DOUBLE' -%}
    {%- set to_text = "CAST(MIN(`date`) AS STRING)" -%}
    {%- set to_text_max = "CAST(MAX(`date`) AS STRING)" -%}
  {%- else -%}
    {{ exceptions.raise_compiler_error("parity_fingerprint: unknown target '" ~ target.name ~ "' (expected dwh | spark)") }}
  {%- endif -%}

  {%- set q -%}
    SELECT
      COUNT(*)                         AS rows_total,
      COUNT(DISTINCT DUID)             AS duids,
      COUNT(DISTINCT {{ d }})          AS days,
      {{ to_text }}                    AS date_min,
      {{ to_text_max }}                AS date_max,
      SUM(CAST({{ mw }} AS {{ dbl }}))    AS mw_sum,
      SUM(CAST({{ price }} AS {{ dbl }})) AS price_sum
    FROM {{ rel }}
  {%- endset -%}

  {%- set r = run_query(q).rows[0] -%}
  {%- set out -%}
{
  "engine": "{{ target.name }}",
  "adapter": "{{ target.type }}",
  "model": "{{ model }}",
  "rows_total": {{ r[0] }},
  "duids": {{ r[1] }},
  "days": {{ r[2] }},
  "date_min": "{{ r[3] }}",
  "date_max": "{{ r[4] }}",
  "mw_sum": {{ r[5] }},
  "price_sum": {{ r[6] }}
}
  {%- endset -%}
  {{ log(out, info=True) }}
{% endmacro %}
