# The thesis, and what the repo found

**In theory the engine is abstract.** dbt's promise is that a model is a `SELECT` and the
adapter does the rest: change `--target`, get the same table somewhere else. For the
*business logic* that holds. The eight models compute the same numbers on Fabric Warehouse
(`dbt-fabric`) and Fabric Spark (`dbt-fabricspark`), and
[`.github/scripts/parity.py`](../.github/scripts/parity.py) fingerprints the gold table on
each and compares them, so that is a check rather than a claim.

**In practice the engine is not abstract — but the axis people expect is the wrong one.**
How each engine distributes the work barely shows up. Read the two `fct_summary.sql` files
side by side and nothing in them cares how the Warehouse or a Spark pool schedules a query.
What does show up, on every model, is:

1. **The SQL dialect.** Bracket quoting, `DATEPART` against `date_format`, no `GREATEST` in
   T-SQL, `to_timestamp` with an explicit format because Spark's `CAST` turns a slash date
   into `NULL` rather than an error, `DOUBLE PRECISION` that Spark rejects, string comparison
   that pads in T-SQL and not in Spark, a string-to-number comparison that truncates in Spark,
   two different ways to round a `DOUBLE` into a `DECIMAL`.
2. **What each engine and adapter actually implements.** Which incremental strategy fits,
   whether the adapter's temporary relation is a temp view or a persistent one, how a CSV can
   be read at all, how many file paths one statement may name, whether `--full-refresh` is
   safe, what the adapter does after a build unless told not to.

[`engine-nuances.md`](engine-nuances.md) is what it takes to get both engines to the same
table. Every row there is one of those two kinds. In both cases dbt is only a client — the
compute is the Warehouse or a Livy session — and the compute shape changed no model.

The project started as separate per-engine repos, which had drifted far enough that three
correctness fixes each existed in exactly one of them. One project with the same logic per
engine, checked by parity on every run, is the fix.
