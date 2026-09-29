# The thesis, and what the repo actually found

**In theory the engine is abstract.** dbt's promise is that a model is a `SELECT` and the
adapter does the rest: change `--target`, get the same table somewhere else. For the
*business logic* that holds. The eight models compute the same numbers on all five engines,
and [`.github/scripts/parity.py`](../.github/scripts/parity.py) fingerprints the gold table on each and compares them, so
that is a check rather than a claim.

**In practice the engine is not abstract at all — but the axis people expect is the wrong
one.** Whether an engine is distributed or single-node barely shows up. Read the five
`fct_summary.sql` files side by side and nothing in them cares that Spark has executors and
DuckDB has one process. What does show up, on every single model, is:

1. **The SQL dialect.** Bracket quoting, `DATEPART` against `strftime`, no `GREATEST` in
   T-SQL, `to_timestamp` with an explicit format because Spark's `CAST` turns a slash date into
   `NULL` rather than an error, `DOUBLE PRECISION` that Spark rejects, string comparison that
   pads in T-SQL and does not elsewhere, three different ways to round a `DOUBLE` into a
   `DECIMAL`.
2. **What each engine and adapter actually implements.** Which `MERGE` shapes the catalog
   accepts, whether the adapter's temporary relation is a temp view or a persistent one, how
   a CSV can be read at all, how many file paths one statement may name, whether
   `--full-refresh` is safe, whether compaction is built in, what a hook may call.

[`engine-nuances.md`](engine-nuances.md) is what it took to get five engines to the
same table. Almost every row there is one of those two kinds. The compute shape appears
in exactly one place — the DuckDB legs need a machine with memory, so in CI they run
*inside* Fabric too — and it changed no model.

This replaces four repos that were the same project four times over —
`dbt_fabric_python_iceberg`, `_dwh`, `_ducklake` and `_delta` — which had drifted far enough
that three correctness fixes each existed in exactly one of them.
