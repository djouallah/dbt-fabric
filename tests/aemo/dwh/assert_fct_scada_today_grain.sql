-- Grain check: the unique key of fct_scada_today must be unique. This is the tripwire for a
-- residual concurrent-writer race, and for an incremental strategy that stopped deduping.
SELECT [file], [DUID], [SETTLEMENTDATE], COUNT(*) AS n
FROM {{ ref('fct_scada_today') }}
GROUP BY [file], [DUID], [SETTLEMENTDATE]
HAVING COUNT(*) > 1
