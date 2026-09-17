-- Crater tripwire: a date that lands PARTIALLY in the summary and is then never completed.
-- Mechanism: during backfill a date enters the summary with only the intervals fct_scada
-- happened to hold at that moment (typically the 49 from the previous day's archive file);
-- the daily branch used to skip any date already present, so the rest of the day never
-- arrived. fct_summary now reprocesses dates it holds partially, which heals these -- this
-- test is the guard that it keeps working. Observed 2025-09-13: 49 in the summary, 288 in
-- fct_scada.
--
-- Compares against fct_scada, NOT against a fixed 288, because "short" and "wrong" are not
-- the same thing. A date can be short at the SOURCE: its first ~4h live in the PREVIOUS
-- day's archive file, so until the backfill loads that file the date sits legitimately
-- capped. fct_scada agrees with the summary on those, and flagging them is noise.
-- Do not re-derive this as "the previous day is missing, so the cap is permanent" -- it
-- usually is not; comparing against the source is what makes the distinction
-- self-correcting.
--
-- The 280 pre-filter is a cheap shortlist off fct_summary alone, not the assertion; only
-- those few dates are probed against fct_scada (measured: ~46s for 2 dates vs ~233s for a
-- full-year aggregate). The latest date is excluded -- it is legitimately still filling.

-- NESTED DERIVED TABLES, NOT CTEs, AND NO TOP-LEVEL `WITH`. dbt-fabric wraps a singular
-- test's SQL inside a CTE of its own, and T-SQL does not allow a WITH clause in a CTE
-- body: the CTEs here parsed as detached and the run died on "Invalid object name
-- 'per_date'". Same trap as the merge models (see CLAUDE.md); it bites tests too because
-- of the wrapper, even though a plain table model with a leading WITH is fine.
--
-- `MAX([date]) OVER ()` rather than a second scan for the max: it runs over the GROUPed
-- result, so the latest date is excluded without repeating the aggregate.
SELECT *
FROM (
  SELECT
    s.[date],
    s.intervals AS summary_intervals,
    (SELECT COUNT(DISTINCT sc.SETTLEMENTDATE)
     FROM {{ ref('fct_scada') }} sc
     WHERE sc.[DATE] = s.[date] AND sc.INTERVENTION = 0 AND sc.INITIALMW <> 0) AS scada_intervals
  FROM (
    SELECT [date], intervals
    FROM (
      SELECT [date],
             COUNT(DISTINCT [time]) AS intervals,
             MAX([date]) OVER () AS max_date
      FROM {{ ref('fct_summary') }}
      WHERE [date] >= DATEADD(MONTH, -12, CAST(GETDATE() AS DATE))
      GROUP BY [date]
    ) per_date
    WHERE [date] < max_date AND intervals < 280
  ) s
) x
WHERE summary_intervals < scada_intervals
