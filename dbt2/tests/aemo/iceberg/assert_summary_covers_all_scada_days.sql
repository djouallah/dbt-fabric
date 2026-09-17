-- The summary should hold at least as many distinct days as fct_scada.
-- With a small process_limit a date whose ingested files are all INTERVENTION=1 or carry
-- unmapped DUIDs produces zero summary rows and trips this until the backlog has converged.
SELECT scada_days, summary_days
FROM (
  SELECT
    (SELECT COUNT(DISTINCT DATE) FROM {{ ref('fct_scada') }} WHERE INTERVENTION = 0) AS scada_days,
    (SELECT COUNT(DISTINCT date) FROM {{ ref('fct_summary') }}) AS summary_days
) t
WHERE scada_days > summary_days
