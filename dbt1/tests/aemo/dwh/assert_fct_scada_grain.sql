-- Grain check: the unique key of fct_scada must be unique. This is the tripwire for a
-- residual concurrent-writer race, and for an incremental strategy that stopped deduping.
SELECT [file], [DUID], [SETTLEMENTDATE], [INTERVENTION], COUNT(*) AS n
FROM {{ ref('fct_scada') }}
GROUP BY [file], [DUID], [SETTLEMENTDATE], [INTERVENTION]
HAVING COUNT(*) > 1
