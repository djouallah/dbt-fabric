-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

-- Insert-only merge on the (date, time, DUID) grain — one operation, one commit.
-- The OneLake Iceberg REST catalog allows only ONE add-snapshot update per commit
-- ("Only one instance of each update type is allowed per request"), so a merge with
-- a WHEN MATCHED UPDATE branch (delete files + data files in one commit) is rejected
-- with BadRequest 400. WHEN MATCHED DO NOTHING keeps every run a single append
-- snapshot: the daily branch backfills dates missing from the summary, the intraday
-- branch tops up after the cutoff, and re-runs dedupe on the key instead of
-- double-appending. Consequences: once a key exists (e.g. from intraday), the daily
-- value never overwrites it — same measurement, so acceptable — and within-date key
-- gaps aren't revisited; `dbt run --full-refresh` is the reconciliation lever.
-- assert_fct_summary_grain is the duplicate tripwire.
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date', 'time', 'DUID'],
    merge_clauses={'when_matched': [{'action': 'do_nothing'}]},
    schema='mart'
) }}

{% if is_incremental() %}

-- Deliberately still counts DISTINCT dates, not fully-captured ones. Making this gate fire on
-- partial dates would pin it permanently true — source-limited dates can never reach a full
-- day — and the daily branch would then run every time, starving the intraday branch and
-- letting today's data go stale. So crater healing (see the filter below) rides on the daily
-- branch whenever it does run, which during backfill is most runs.
{%- set has_new_daily_query -%}
SELECT
  (SELECT COUNT(DISTINCT DATE) FROM {{ ref('fct_scada') }} WHERE INTERVENTION = 0) as scada_days,
  (SELECT COUNT(DISTINCT date) FROM {{ this }}) as summary_days
{%- endset -%}

{%- if execute and flags.WHICH in ('run', 'build', 'retry') -%}
  {%- set result = run_query(has_new_daily_query) -%}
  {%- set has_new_daily = result and result.rows[0][0] > result.rows[0][1] -%}
{%- else -%}
  {%- set has_new_daily = true -%}
{%- endif -%}

{% if has_new_daily %}

-- New daily data found: backfill ONLY the dates missing from the summary. Computing
-- full history here OOMs the 7GB CI runner — the merge materializes its source as a
-- temp relation before joining the target, so the source must stay small. Within-date
-- gaps (a key missing for a date the summary already has) need `--full-refresh`.
WITH daily_summary AS (
  SELECT
    s.DATE as date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) as time,
    (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada') }}) as cutoff,
    s.DUID,
    MAX(s.INITIALMW) as mw,
    MAX(p.RRP) as price
  FROM {{ ref('fct_scada') }} s
  LEFT JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  LEFT JOIN {{ ref('fct_price') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INTERVENTION = 0
    AND s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    {% if is_incremental() %}
    -- Skip only the dates the summary already holds IN FULL. A partially-loaded date has to
    -- be revisited: during backfill a date can land with just the intervals fct_scada
    -- happened to hold at that moment (typically the 49 that came from the previous day's
    -- archive file), and the old `NOT IN (SELECT date FROM this)` filter meant it was never
    -- looked at again once the rest of the day arrived — a permanent crater. Observed
    -- 2025-09-13: 49 intervals in the summary against 288 in fct_scada.
    -- Reprocessing is safe and cheap under the insert-only merge — the missing
    -- (date, time, DUID) keys insert, the ones already present DO NOTHING, and it stays a
    -- single append snapshot, so OneLake's one-add-per-commit rule still holds.
    -- 280 rather than 288 matches the tolerance in assert_fct_summary_no_partial_dates.
    -- Dates the source itself is still short on (the backfill has not reached the preceding
    -- archive file yet) get re-read on each daily run and insert nothing until it does — a
    -- few dates' worth of scan, bounded and harmless, and they fill in on their own.
    AND s.DATE NOT IN (
      SELECT date FROM {{ this }} GROUP BY date HAVING COUNT(DISTINCT time) >= 280
    )
    {% endif %}
  GROUP BY ALL
)

SELECT
  date,
  time,
  DUID,
  CAST(mw AS DECIMAL(18, 4)) AS mw,
  CAST(price AS DECIMAL(18, 4)) AS price,
  cutoff
FROM daily_summary

{% else %}

-- No new daily data: append intraday after cutoff
WITH max_cutoff AS (
  SELECT MAX(cutoff) as cutoff FROM {{ this }}
),

incremental_data AS (
  SELECT
    s.DATE as date,
    s.SETTLEMENTDATE,
    s.DUID,
    MAX(s.INITIALMW) AS mw,
    MAX(p.RRP) AS price
  FROM {{ ref('fct_scada_today') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price_today') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  CROSS JOIN max_cutoff mc
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    AND s.SETTLEMENTDATE > mc.cutoff
  GROUP BY ALL
)

SELECT
  date,
  CAST(strftime(SETTLEMENTDATE, '%H%M') AS INT) AS time,
  DUID,
  CAST(mw AS DECIMAL(18, 4)) AS mw,
  CAST(price AS DECIMAL(18, 4)) AS price,
  CAST(MAX(SETTLEMENTDATE) OVER () AS TIMESTAMPTZ) AS cutoff
FROM incremental_data

{% endif %}

{% else %}

-- Full refresh from daily + today data
WITH daily_summary AS (
  SELECT
    s.DATE as date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) as time,
    s.DUID,
    MAX(s.INITIALMW) as mw,
    MAX(p.RRP) as price
  FROM {{ ref('fct_scada') }} s
  LEFT JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  LEFT JOIN {{ ref('fct_price') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INTERVENTION = 0
    AND s.INITIALMW <> 0
    AND p.INTERVENTION = 0
  GROUP BY ALL

  UNION ALL

  SELECT
    s.DATE as date,
    CAST(strftime(s.SETTLEMENTDATE, '%H%M') AS INT) as time,
    s.DUID,
    MAX(s.INITIALMW) as mw,
    MAX(p.RRP) as price
  FROM {{ ref('fct_scada_today') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price_today') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    AND s.SETTLEMENTDATE > (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada') }})
  GROUP BY ALL
)

SELECT
  date,
  time,
  DUID,
  CAST(mw AS DECIMAL(18, 4)) AS mw,
  CAST(price AS DECIMAL(18, 4)) AS price,
  (SELECT GREATEST(
    (SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada') }}),
    COALESCE((SELECT MAX(CAST(SETTLEMENTDATE AS TIMESTAMPTZ)) FROM {{ ref('fct_scada_today') }}), CAST('1900-01-01' AS TIMESTAMPTZ))
  )) AS cutoff
FROM daily_summary
ORDER BY date

{% endif %}
