{{ config(tags=['heavy']) }}
-- The summary should hold at least as many distinct days as fct_scada.
-- Tagged heavy: with a small process_limit some ingested files are all INTERVENTION=1 or
-- carry unmapped DUIDs and produce zero summary rows for that date, so the assertion only
-- holds at full data volume.
SELECT scada_days, summary_days
FROM (
  SELECT
    (SELECT COUNT(DISTINCT `DATE`) FROM {{ ref('fct_scada') }} WHERE INTERVENTION = 0) AS scada_days,
    (SELECT COUNT(DISTINCT `date`) FROM {{ ref('fct_summary') }}) AS summary_days
) t
WHERE scada_days > summary_days
