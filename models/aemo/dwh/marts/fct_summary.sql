-- depends_on: {{ ref('fct_scada_today') }}
-- depends_on: {{ ref('fct_price_today') }}

{#-- Determinism contract (see the duckrun version for the full story): every run recomputes,
     with the SAME SQL as a full rebuild, exactly the dates whose stored content could be
     stale, and reconciles that batch key by key. Incremental == full-rebuild by construction
     for every date it touches; no cutoff watermark, no run-history dependence, no
     runner-decided branch. This is the dwh fct_summary of djouallah/direct-lake-parquet-layout.

     The previous copy here was the original dwh repo's: `append` of today's intraday on a
     plain run and a runner-decided delete+insert of the whole history when new daily files
     landed (a `check_new_daily` probe before the build). Correct only while the runner ran
     the probe, and it retracted whole dates, which made dwh the one engine whose row count
     could differ from the others on identical inputs.

     STRATEGY: `merge` on the full [date],[time],[DUID] grain — the SAME semantics as spark
     (update matched + insert new). dbt-fabric's merge is default__get_merge_sql, which always
     emits WHEN MATCHED THEN UPDATE SET <every column>, so a revised mw/price overwrites the
     stored row exactly as it does on spark. Bracket every key column: dbt interpolates them
     raw into the ON clause and `date`/`time` are T-SQL reserved words.

     We deliberately do NOT use --full-refresh on this engine: on dbt-fabric that DROPs +
     recreates the table (a Sch-M DDL swap that deadlocks Fabric's background stats
     maintenance, loses grants, and rebinds Direct Lake every run). The full-history rebuild
     lever is REBUILD_SUMMARY=1 in the environment or --vars 'rebuild_summary: true', which
     keeps the same merge write path and just emits every date.

     The intraday branch is gated on dispatch_duids because the two branches read AEMO
     tables with DIFFERENT UNIT UNIVERSES: 26 non-scheduled units publish SCADA telemetry
     but have zero rows in fct_scada ever. Merge cannot retract a row, so that gate is the
     only thing keeping those units out. See the duckrun version for the full story. --#}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['[date]', '[time]', '[DUID]'],
    schema='mart'
) }}
{#-- cluster_by was REMOVED: on a CLUSTER BY table Fabric runs automatic background
     clustering/compaction that holds a lock on the table, and every fct_summary write then
     deadlocked against it *reproducibly* — the retry deadlocked too, same process id.
     Dropping cluster_by is what stops the deadlocks; the summary is small and date/DUID
     filtering is fine without physical clustering. Do not re-add it here. --#}

{%- set rebuild = var('rebuild_summary', false) or env_var('REBUILD_SUMMARY', '0') == '1' -%}
{# Closes with `%}`, NOT `-%}`: a right-strip swallows the newlines after this tag and
   glues WITH onto the `-- depends_on` comment line above, commenting the keyword out —
   which is exactly how this model failed with "Incorrect syntax near 'scada_cutoff'". #}
{%- set scoped = is_incremental() and not rebuild %}

WITH
-- The unit universe the DAILY branch can reproduce. Deliberately UNBOUNDED, not a trailing
-- window: fct_scada is append-only, so this set only ever GROWS and can never orphan a row
-- it previously admitted. Outside the `scoped` block on purpose — a REBUILD_SUMMARY run
-- emits the intraday branch too and must apply the identical filter.
dispatch_duids AS (
  SELECT DISTINCT DUID FROM {{ ref('fct_scada') }}
),
{% if scoped %}
-- Dates whose stored content could differ from a clean recomputation; everything older
-- is settled. Shrinking this window silently reduces what can be repaired.
rebuild_dates AS (
  -- Never seen before: archive backfill, or a first build catching up.
  SELECT DISTINCT s.[DATE] AS [date] FROM {{ ref('fct_scada') }} s
  WHERE s.INTERVENTION = 0
    AND s.[DATE] NOT IN (SELECT DISTINCT [date] FROM {{ this }})
  UNION
  -- Recently settled: incomplete until the daily file lands, which is several days later
  -- if the pipeline missed a run — so a window, not just the newest daily date.
  SELECT DISTINCT s.[DATE] FROM {{ ref('fct_scada') }} s
  WHERE s.[DATE] >= (SELECT DATEADD(DAY, -6, MAX([DATE])) FROM {{ ref('fct_scada') }})
  UNION
  -- Still in flux until their daily file lands.
  SELECT DISTINCT s.[DATE] FROM {{ ref('fct_scada_today') }} s
  UNION
  -- Partially written and never completed. A calendar date straddles TWO PUBLIC_DAILY files
  -- (they roll at 04:00), so a date first computed when only one had landed holds ~48 or
  -- ~240 intervals; when the second file lands in a LATER run, a 60-file backfill batch has
  -- moved MAX(DATE) two months past the 6-day window above and the date is never revisited.
  -- Every batch boundary of a backfill left one (measured 2026-09-17: spark short ~30k rows
  -- on each of 2019-01-27, 2019-11-23, 2020-01-23 after three incremental runs; the reference
  -- repo never saw it because it loads the whole archive at once). 280 matches
  -- assert_fct_summary_no_partial_dates. A date the SOURCE itself still lacks stays in this
  -- set and recomputes each run until its file lands -- a few dates' scan, nothing inserts.
  SELECT [date] FROM {{ this }} GROUP BY [date] HAVING COUNT(DISTINCT [time]) < 280
),
{% endif %}
scada_cutoff AS (
  SELECT MAX(SETTLEMENTDATE) AS c FROM {{ ref('fct_scada') }}
),
cutoff_calc AS (
  -- T-SQL has no GREATEST: max of (daily max, intraday max) via UNION ALL.
  SELECT MAX(v) AS cutoff FROM (
    SELECT MAX(SETTLEMENTDATE) AS v FROM {{ ref('fct_scada') }}
    UNION ALL
    SELECT COALESCE(MAX(SETTLEMENTDATE), CAST('1900-01-01' AS DATETIME2(6))) FROM {{ ref('fct_scada_today') }}
  ) u
),
daily_summary AS (
  SELECT
    s.[DATE] AS [date],
    DATEPART(HOUR, s.SETTLEMENTDATE) * 100 + DATEPART(MINUTE, s.SETTLEMENTDATE) AS [time],
    s.DUID,
    MAX(s.INITIALMW) AS mw,
    MAX(p.RRP) AS price
  FROM {{ ref('fct_scada') }} s
  -- INNER joins: `WHERE p.INTERVENTION = 0` always discarded null-price rows anyway.
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INTERVENTION = 0
    AND s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    {% if scoped %}
    AND s.[DATE] IN (SELECT [date] FROM rebuild_dates)
    {% endif %}
  GROUP BY s.[DATE], DATEPART(HOUR, s.SETTLEMENTDATE) * 100 + DATEPART(MINUTE, s.SETTLEMENTDATE), s.DUID

  UNION ALL

  -- Intraday tail: intervals beyond the daily horizon. Every date here is in
  -- rebuild_dates by construction.
  SELECT
    s.[DATE] AS [date],
    DATEPART(HOUR, s.SETTLEMENTDATE) * 100 + DATEPART(MINUTE, s.SETTLEMENTDATE) AS [time],
    s.DUID,
    MAX(s.INITIALMW) AS mw,
    MAX(p.RRP) AS price
  FROM {{ ref('fct_scada_today') }} s
  JOIN {{ ref('dim_duid') }} d ON s.DUID = d.DUID
  JOIN {{ ref('fct_price_today') }} p
    ON s.SETTLEMENTDATE = p.SETTLEMENTDATE AND d.Region = p.REGIONID
  WHERE
    s.INITIALMW <> 0
    AND p.INTERVENTION = 0
    -- Only units the daily branch will be able to reproduce once this date settles.
    AND s.DUID IN (SELECT DUID FROM dispatch_duids)
    AND s.SETTLEMENTDATE > (SELECT c FROM scada_cutoff)
  GROUP BY s.[DATE], DATEPART(HOUR, s.SETTLEMENTDATE) * 100 + DATEPART(MINUTE, s.SETTLEMENTDATE), s.DUID
)

SELECT
  [date],
  [time],
  DUID,
  CAST(mw AS DECIMAL(18, 4)) AS mw,
  CAST(price AS DECIMAL(18, 4)) AS price,
  -- Provenance column only — no read path depends on it anymore. Kept (and kept
  -- populated) to avoid a schema change that would force a DROP here.
  (SELECT cutoff FROM cutoff_calc) AS cutoff
FROM daily_summary
-- Parity with the duckdb and spark copies, which both end with the same sort. It makes NO claim
-- about physical layout: this SQL is a merge SOURCE on all engines, so nothing about the
-- ordering reaches the stored table. It is here so the legs pay the same cost — deleting it
-- from one tree is a fairness regression, not a cleanup. Lands in the outer SELECT of a Fabric
-- CTAS (dbt-fabric builds `CREATE TABLE <temp> AS <model sql>` and merges from that relation, it
-- does NOT wrap this in `MERGE ... USING (<sql>)`), so the derived-table ORDER BY restriction
-- does not apply here.
ORDER BY [date], [time]
