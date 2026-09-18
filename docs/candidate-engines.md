# Candidate engines

Two engines have been looked at as a sixth leg. Neither is being built, and the reasons are
different in kind: **Sail** works and its *packaging* does not, while **Polars** has no dbt
adapter to point a `--target` at in the first place.

Both are recorded here rather than dropped, because "we tried it and this is what stopped it"
is the part that goes missing. A candidate engine's gaps are worked around inside its own leg,
never by changing the working five — so what matters about each is exactly which gaps a leg
would have to carry.

---

## Sail — the engine passes, the packaging does not

**Sail** ([LakeSail](https://github.com/lakehq/sail)) was evaluated as a sixth and **is not
being built**, for a reason that has nothing to do with the engine: `dbt-sail`'s declared
dependency is `dbt-spark[session]`, which pulls **full PySpark with its jars** (and conflicts
with `pyspark-client`). Shipping the entire Spark distribution in order to avoid Spark is the
opposite of the point — a JVM need never run, but it is in the image. That is a packaging
choice, fixable upstream, so the evaluation below stands for whenever it is.

The engine itself is a Rust Spark replacement with no JVM, and its native OneLake catalog
takes the same bearer token the `iceberg` leg mints, so a sail leg would be a second Iceberg
writer against the same gold layer.
`.github/workflows/sail_smoke.yml` probes it — `workflow_dispatch` only, gates nothing — in
two phases, because "the catalog accepts SQL" and "this repo's models could run on it" are
different claims.

**Phase A, the adapter contract: all green** on Sail 0.7.1. Schema and table creation, insert,
both `MERGE INTO` shapes dbt-spark emits, `show table extended` (how dbt-spark decides an
incremental model exists), and a read-back proving the merges applied rather than merely
returning.

**Phase B, what the models actually do: everything, bar one design decision.** Sail reads the real
ragged AEMO CSVs off OneLake — 666k rows across two `PUBLIC_DAILY` files — keeps one record
type, parses the slash date, counts DUIDs, and does `sequence()`/`explode()`, window
functions, a 130-column record, a multi-column merge key and a genuinely temporary view.

Reading a ragged file takes two things **together**, and either alone fails:

| | |
|---|---|
| a schema **padded to at least the widest record** in the file | a `PUBLIC_DAILY` holds many record types — DUNIT is 53 columns, DREGION 130 — so a narrower schema is `expected 4, got 10` |
| **`allowTruncatedRows`** for every row narrower than that | without it the padded schema fails the other way: `expected 131, got 14` |

`mode 'PERMISSIVE'`, which is how the spark leg says this, does **not** do it on Sail. The
probe keeps a control (probe 24) that is identical but for the option, so the read's success
is attributable to it rather than to the padding.

**The one Sail limitation: it cannot name the source file.** `input_file_name()` is
`UnsupportedOperationException` and `_metadata.file_name` is `cannot resolve attribute`
([lakehq/sail#1210](https://github.com/lakehq/sail/issues/1210), open). Every other engine
here has one — DuckDB's `filename`, Fabric's `src.filepath()`, Spark's `input_file_name()`,
which is why `macros/parse_filename.sql` has a dialect branch at all.

Not a blocker: `file` is a *choice* of `unique_key`, and within a `source_type`
`(DUID|REGIONID, SETTLEMENTDATE, INTERVENTION)` is already the natural grain, so a sail leg
keys its facts on those instead. That is the sail leg's own workaround for a Sail gap —
the same kind of per-engine operational difference this repo already carries five of.

A direct ``csv.`path`` read also fails, since that form carries no options and so cannot pass
`allowTruncatedRows` — but the view form is what `spark_read_csv.sql` emits anyway.

Two dialect facts fell out as well: Sail rounds `DOUBLE` → `DECIMAL` **HALF_UP** (like Spark,
unlike DuckDB), and a bare `CAST` of AEMO's `yyyy/MM/dd` is a hard parse error rather than
Spark's silent `NULL` — strictly better, since it cannot reach the gold layer unnoticed.

Three things the probe had to get right before any of the above was measurable, each
found by a failed run and each a cost a real leg would carry:

| what | why |
|---|---|
| the catalog url as `<workspace-id>/<lakehouse-id>` | a name comes back `Failed to load config: 400 Bad Request` |
| `AZURE_STORAGE_TOKEN` as well as the catalog's `bearer_token` | the catalog token authorises the CATALOG; the data files are a separate credential. The same split `dbt1/profiles.yml` spells out for the `iceberg` leg, which brings both too. Without it Sail falls through to the instance metadata endpoint |
| `USING iceberg` + `tblproperties('write.merge.mode'='merge-on-read')` | Sail's `CREATE TABLE` defaults to parquet, which the REST catalog refuses; and it will not merge a copy-on-write table. dbt-spark spells both in config, on every merged model |

---

## Polars — the adapter is not there yet

**There is nothing to point a `--target` at.** Polars is an engine, not a dbt adapter: what
exists is community work rather than a released adapter you can `pip install`, gate on
`target.name` and hand a profile to the way the five legs here are. Until that exists there is
no leg to write, and no smoke probe worth building either — `sail_smoke.yml` could be written
because `dbt-sail` *is* installable and its contract could be exercised; there is no
equivalent to exercise here.

**And if the adapter arrived tomorrow, the gold layer would still not survive the move.**
Polars' SQL runs through `pl.SQLContext` rather than a database — no `dateadd`/`datediff`/
`current_timestamp`/`hash`, and `date_trunc` limited to day/hour/minute/month/year — so the
eight models would have to become Python models. That is the one-gold-layer rule breaking,
which is the thing this repo exists to prevent: the value of a sixth leg is that it computes
the same numbers from the same SQL, and a leg that rewrites the models in a dataframe API
proves nothing about the engine, only about the rewrite.

So the order matters. The adapter is the blocker to *evaluating* Polars; the SQL surface is
the blocker to *adopting* it. The second does not become interesting until the first is
fixed — and fixing the first does not fix the second.
