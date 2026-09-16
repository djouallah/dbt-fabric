# Parity fingerprints

One JSON per engine, written by the `parity_fingerprint` macro at the end of that engine's
CI leg and compared by `.github/scripts/parity.py compare`. This is the acceptance test for
the whole repo: five adapters, one gold layer, one set of numbers.

Fingerprints are committed by the `parity` job so a run can be compared against the last
one. Do not commit a fingerprint from a laptop run — a local build covers a couple of days
of data and would make the next real comparison fail for the wrong reason.
