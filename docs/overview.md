# Overview

One dbt project builds the same AEMO gold layer on two Microsoft-supported adapters: Fabric
Warehouse (`dbt-fabric`) and Fabric Spark (`dbt-fabricspark`). The business logic is the same
on both; every pipeline run fingerprints the gold table on each engine and compares them
(the `parity` notebook).

Each engine keeps its own copy of the models, because two things differ between them:

1. **The SQL dialect** — quoting, date functions, how a string is cast to a timestamp or
   compared with a number, how a `DOUBLE` rounds into a `DECIMAL`.
2. **What each adapter supports** — which incremental strategy fits, how a CSV is read, how
   temporary relations behave, whether `--full-refresh` is safe.

[`engine-nuances.md`](engine-nuances.md) lists each difference and why it exists. On both
engines dbt runs as a client; the compute is the Warehouse or a Spark (Livy) session.
