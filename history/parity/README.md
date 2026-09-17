# Parity fingerprints

One JSON per engine. `parity_fingerprint` (a dbt run-operation) logs it at the end of that
engine's CI leg, `parity.py capture` lifts it out of the log into `<engine>.json`, and
`parity.py compare` grades them: rows_total, duids, days, date_min and date_max must match
exactly; mw_sum and price_sum get a 1e-7 relative tolerance, because DOUBLE -> DECIMAL
tie-breaking differs per engine. This is the acceptance test for the whole repo: five
adapters, one gold layer, one set of numbers.

Fingerprints are committed by the `record` job of `pipeline.yml` (when the comparison
passes) so a run can be compared against the last one; the same fingerprints are folded into
that run's record under `history/runs/`. Do not commit a fingerprint from a laptop run — a local build covers a couple of days
of data and would make the next real comparison fail for the wrong reason.
