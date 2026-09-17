"""Probe whether LakeSail's Sail can drive dbt against the OneLake Iceberg REST catalog.

NOT AN ENGINE. This is a smoke test for a candidate sixth engine, and its output is meant to
be read by a human — or pasted into an upstream issue. It never fails the workflow: a red
result IS the deliverable.

WHY IT EXISTS. `dbt-sail` is a thin wrapper around `dbt-spark` that talks Spark Connect to
Sail, a Rust Spark replacement with no JVM, and Sail has a native OneLake catalog that takes
the same Entra bearer token the `iceberg` leg already mints. That makes it a plausible sixth
engine — a second Iceberg writer against the same gold layer, on an engine that is neither
DuckDB nor JVM Spark. `CREATE TABLE` through that catalog is confirmed working by hand.
MERGE INTO is not, and every fact model in this repo is an incremental merge on a unique key.
Sail's changelog only ever names merge against DELTA (0.4.5 "basic support for the Delta Lake
merge operation", 0.6.2 deletion vectors); the SQL feature matrix lists MERGE INTO without
saying which table formats it covers. Iceberg MERGE is undocumented either way, so it gets
measured rather than assumed.

THE TWO PROBES THAT DECIDE IT are 6/7 (merge) and 8 (`show table extended`). The second is
not obvious and matters just as much: it is how dbt-spark decides whether an incremental
model already exists. If it comes back empty, dbt rebuilds every model from scratch on every
run while reporting success — the same "green run, wrong answer" failure mode check_gating.py
exists to catch elsewhere. An empty result there is worse than a hard error.

Probe 10 re-reads the table and compares it against the expected rows. A merge that returns
without error but does not apply is exactly the kind of thing this repo has been bitten by
(dbt2's getvariable() reading NULL instead of erroring), so "it did not raise" is not
accepted as "it worked".

The connection shape is the one already proven by hand against Fabric: SAIL_CATALOG__LIST
must be in the environment BEFORE the server starts — it is read at startup, not per session.

Env (the workflow supplies all of these):
    WAREHOUSE_PATH   "{workspace_id}/{lakehouse_id}" — from provision.py's iceberg branch
    ONELAKE_TOKEN    the https://storage.azure.com/ token, same one dbt2/profiles.yml uses
    SAIL_SMOKE_SCHEMA  schema to build in (default sail_smoke — deliberately outside the
                       <engine>_landing / <engine>_mart namespace the five engines share)
    SAIL_SMOKE_KEEP    "1" leaves the tables behind for inspection in Fabric

Usage:
    python .github/scripts/sail_smoke.py
"""

import os
import sys
import traceback

WAREHOUSE = os.environ["WAREHOUSE_PATH"]
TOKEN = os.environ["ONELAKE_TOKEN"]
SCHEMA = os.environ.get("SAIL_SMOKE_SCHEMA", "sail_smoke")
KEEP = os.environ.get("SAIL_SMOKE_KEEP") == "1"

CATALOG = "onelake"

# Sail reads its catalog list once, at server start. Setting this after SparkConnectServer()
# is running has no effect and the failure looks like "catalog not found", so it goes here at
# import time, before anything touches pysail.
#
# api="iceberg" is the Fabric Iceberg REST endpoint. There is no api="delta" variant to try:
# the Unity Catalog endpoint is not available to us, so this leg would write Iceberg.
# A function, not a template string: the config is TOML-ish and full of literal braces, so
# anything .format()-shaped here dies on them ("unexpected '{' in field name"). The token is
# the only variable, and the banner wants the same text with it redacted.
def catalog_cfg(token):
    return (
        f'[{{type="onelake", name="{CATALOG}", url="{WAREHOUSE}", '
        f'api="iceberg", bearer_token="{token}", '
        f'table_cache_type="session", table_cache_ttl_secs=300, '
        f'database_cache_type="session", database_cache_ttl_secs=300}}]'
    )


os.environ["SAIL_CATALOG__LIST"] = catalog_cfg(TOKEN)
os.environ.setdefault("SAIL_CATALOG__DEFAULT_CATALOG", CATALOG)
os.environ.setdefault("SAIL_OPTIMIZER__ENABLE_JOIN_REORDER", "true")
os.environ.setdefault("SAIL_EXECUTION__COLLECT_STATISTICS", "true")


# The rows each probe should leave behind, tracked so probe 10 can tell "the merge ran" from
# "the merge returned". Start (1,10),(2,20); insert (3,30); merge updates 2 and adds 4; the
# insert-only merge must LEAVE 1 ALONE and add 5.
EXPECTED = [(1, 10), (2, 99), (3, 30), (4, 40), (5, 50)]

# Created BETWEEN probes 6 and 7 -- probe 6 has to read its own source before this table
# exists, so it cannot be a probe of its own. Hoisted to a constant anyway so tests_py can
# check the keys it introduces against EXPECTED.
INSERT_ONLY_SOURCE = "select 1 as k, 111 as v union all select 5, 50"


def banner(conn):
    """Everything an upstream issue needs to reproduce, with the token redacted."""
    # Guarded like pyspark below: a missing pysail is an install problem, and the banner
    # saying so plainly beats a traceback from inside the version report.
    try:
        import pysail
        lines = [f"pysail        {getattr(pysail, '__version__', '?')}"]
    except ImportError:
        lines = ["pysail        (not importable)"]
    try:
        import pyspark
        lines.append(f"pyspark       {getattr(pyspark, '__version__', '?')}")
    except ImportError:
        lines.append("pyspark       (not importable)")
    try:
        from importlib.metadata import version
        lines.append(f"dbt-sail      {version('dbt-sail')}")
    except Exception:
        lines.append("dbt-sail      (not installed — this probe is raw SQL, not dbt)")
    lines.append(f"catalog       {catalog_cfg('***')}")
    lines.append(f"schema        {SCHEMA}")
    if conn is not None:
        try:
            lines.append(f"sail version  {conn.sql('select version()').collect()[0][0]}")
        except Exception as e:
            lines.append(f"sail version  (select version() failed: {type(e).__name__}: {e})")
    return lines


def probes():
    """(number, name, sql, why) in execution order.

    The merge statements are written in the shape dbt-spark's merge macro actually emits —
    DBT_INTERNAL_DEST / DBT_INTERNAL_SOURCE aliases, `update set *`, `insert *` — and not a
    hand-simplified version. A parser that accepts a tidied-up merge and rejects dbt's is a
    result we would rather find here than in a Fabric run.
    """
    tgt = f"{SCHEMA}.probe_tgt"
    src = f"{SCHEMA}.probe_src"
    src2 = f"{SCHEMA}.probe_src_insert_only"
    return [
        (1, "show databases", f"show databases in {CATALOG}",
         "is the catalog reachable at all"),
        (2, "create schema", f"create schema if not exists {SCHEMA}",
         "dbt creates its own schemas"),
        (3, "create table as select", f"create table {tgt} as select 1 as k, 10 as v "
                                      f"union all select 2, 20",
         "known-good by hand; anchors the run"),
        (4, "create merge source",
         f"create table {src} as select 2 as k, 99 as v union all select 4, 40",
         "merge sources, as real tables — dbt merges from a __dbt_tmp relation"),
        (5, "insert into", f"insert into {tgt} values (3, 30)",
         "the append path every insert-only fact would take"),
        (6, "MERGE INTO (update + insert)",
         f"merge into {tgt} as DBT_INTERNAL_DEST\n"
         f"    using {src} as DBT_INTERNAL_SOURCE\n"
         f"    on DBT_INTERNAL_SOURCE.k = DBT_INTERNAL_DEST.k\n"
         f"    when matched then update set *\n"
         f"    when not matched then insert *",
         "THE BLOCKING UNKNOWN — dbt-spark's merge strategy, exactly as it renders"),
        (7, "MERGE INTO (insert only)",
         f"merge into {tgt} as DBT_INTERNAL_DEST\n"
         f"    using {src2} as DBT_INTERNAL_SOURCE\n"
         f"    on DBT_INTERNAL_SOURCE.k = DBT_INTERNAL_DEST.k\n"
         f"    when not matched then insert *",
         "the insert-only shape (dim_calendar's skip_matched_step, and the wide facts, "
         "which only ever add rows)"),
        (8, "show table extended", f"show table extended in {SCHEMA} like '*'",
         "HOW dbt-spark DECIDES A MODEL EXISTS — empty here means silent full-refresh "
         "every run"),
        (9, "describe table extended", f"describe table extended {tgt}",
         "column and type discovery"),
    ]


def run(conn, sql):
    """Execute and materialise. Returns the rows; Spark Connect is lazy without collect()."""
    return conn.sql(sql).collect()


def oneline(e):
    return " ".join(str(e).split("\n")[0].split())


def main():
    from pyspark.sql import SparkSession
    from pysail.spark import SparkConnectServer

    print("\n".join(banner(None)), flush=True)
    print(flush=True)

    # A server that will not start is an environment problem, not a finding about Sail, so
    # this one is allowed to raise.
    server = SparkConnectServer()
    server.start()
    _, port = server.listening_address
    conn = SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
    print(f"sail listening on {port}", flush=True)
    print("\n".join(banner(conn)[-1:]), flush=True)
    print(flush=True)

    results = []

    # Everything below is two-part named (<schema>.<table>), which is what dbt-spark emits;
    # the default catalog resolves the first part.
    for n, name, sql, why in probes():
        print(f"[{n}] {name} — {why}", flush=True)
        for line in sql.split("\n"):
            print(f"    {line}", flush=True)
        try:
            rows = run(conn, sql)
        except Exception as e:
            print(f"    FAIL {type(e).__name__}: {oneline(e)}", flush=True)
            traceback.print_exc()
            results.append((n, name, f"FAIL — {type(e).__name__}: {oneline(e)}"))
            print(flush=True)
            continue

        status = f"PASS ({len(rows)} row(s))"
        # Probe 8 is the one where a successful empty result is the bad outcome.
        if n == 8 and not rows:
            status = "EMPTY — statement succeeded but listed NOTHING (dbt would full-refresh)"
            print(f"    ::warning::{status}", flush=True)
        for r in rows[:5]:
            print(f"    -> {r}", flush=True)
        if len(rows) > 5:
            print(f"    -> ... {len(rows) - 5} more", flush=True)
        print(f"    {status}\n", flush=True)
        results.append((n, name, status))

        # The insert-only source has to exist before probe 7 and after probe 6 reads its own
        # source, so it is created here rather than as a probe of its own.
        if n == 6:
            try:
                run(conn, f"create table {SCHEMA}.probe_src_insert_only as "
                          f"{INSERT_ONLY_SOURCE}")
            except Exception as e:
                print(f"    (could not create the insert-only source: {oneline(e)})",
                      flush=True)

    # ---- 10: did the writes actually land -------------------------------------------
    # "It did not raise" is not "it worked". A merge that silently applies nothing looks
    # identical to a merge that applied correctly, from the statement alone.
    print(f"[10] verify contents — a merge that returns is not a merge that applied",
          flush=True)
    try:
        rows = [tuple(r) for r in run(conn, f"select k, v from {SCHEMA}.probe_tgt order by k")]
        print(f"    expected {EXPECTED}", flush=True)
        print(f"    actual   {rows}", flush=True)
        if rows == EXPECTED:
            results.append((10, "verify contents", "PASS — every write applied"))
        else:
            results.append((10, "verify contents", f"MISMATCH — got {rows}"))
    except Exception as e:
        print(f"    FAIL {type(e).__name__}: {oneline(e)}", flush=True)
        results.append((10, "verify contents", f"FAIL — {type(e).__name__}: {oneline(e)}"))
    print(flush=True)

    # ---- 11: cleanup, itself a probe -------------------------------------------------
    if KEEP:
        print(f"[11] cleanup — SKIPPED (SAIL_SMOKE_KEEP=1); {SCHEMA} left in the lakehouse\n",
              flush=True)
        results.append((11, "cleanup", "SKIPPED (SAIL_SMOKE_KEEP=1)"))
    else:
        print("[11] cleanup — DROP is a capability too", flush=True)
        dropped = []
        for stmt in (f"drop table if exists {SCHEMA}.probe_tgt",
                     f"drop table if exists {SCHEMA}.probe_src",
                     f"drop table if exists {SCHEMA}.probe_src_insert_only",
                     f"drop schema if exists {SCHEMA}"):
            try:
                run(conn, stmt)
                dropped.append(f"ok: {stmt}")
            except Exception as e:
                dropped.append(f"FAIL: {stmt} — {type(e).__name__}: {oneline(e)}")
            print(f"    {dropped[-1]}", flush=True)
        bad = [d for d in dropped if d.startswith("FAIL")]
        results.append((11, "cleanup", "PASS" if not bad else f"{len(bad)} drop(s) failed"))
        print(flush=True)

    report(results)

    try:
        server.stop()
    except Exception:
        pass


def report(results):
    out = ["=" * 100, "Sail smoke — dbt-sail feasibility against the OneLake Iceberg REST "
                      "catalog", "-" * 100]
    for n, name, status in results:
        out.append(f"{n:>3}  {name:<34}{status}")
    out.append("=" * 100)

    verdict = read_verdict(results)
    out.append(verdict)
    out.append("=" * 100)
    print("\n".join(out), flush=True)

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write("## ⛵ Sail smoke (OneLake Iceberg REST)\n\n")
            f.write("| # | probe | result |\n|---|---|---|\n")
            for n, name, status in results:
                f.write(f"| {n} | {name} | {status} |\n")
            f.write(f"\n{verdict}\n\n")


def read_verdict(results):
    """Say what the result MEANS for a sixth engine, so the log answers the question asked."""
    by_n = {n: status for n, _, status in results}
    merge_ok = by_n.get(6, "").startswith("PASS")
    listing = by_n.get(8, "")
    verified = by_n.get(10, "").startswith("PASS")

    # INCONCLUSIVE BEATS A WRONG VERDICT. If probe 1 never reached the catalog, nothing below
    # is evidence about Sail at all -- the first run said "not viable yet, dbt-spark cannot
    # see existing relations" when the truth was that the catalog config was rejected with a
    # 400 before a single statement was planned. A probe that draws conclusions from its own
    # misconfiguration is worse than one that fails.
    if not by_n.get(1, "").startswith("PASS"):
        return ("VERDICT: INCONCLUSIVE -- the catalog was never reached, so nothing here says "
                "anything about Sail. Check the SAIL_CATALOG__LIST url in the banner above: "
                "the item resolves by GUID (<workspace-id>/<lakehouse-id>), and a name there "
                "comes back as `Failed to load config: 400 Bad Request`.")

    if not listing.startswith("PASS"):
        return ("VERDICT: not viable yet — `show table extended` did not list the schema, so "
                "dbt-spark cannot see existing relations and every run would silently "
                "full-refresh.")
    if merge_ok and verified:
        return ("VERDICT: viable — merge and relation listing both work and the writes "
                "verified. The spark model tree ports nearly as-is; next step is the full "
                "leg.")
    if merge_ok and not verified:
        return ("VERDICT: SUSPECT — merge returned without error but the table does not hold "
                "what it should. This is the worst outcome and the most worth reporting "
                "upstream; do not build on it.")
    return ("VERDICT: buildable but divergent — no MERGE, so the wide facts would become "
            "append-with-anti-join (duckrun already treats insert and do-nothing-merge as "
            "the same operation) and fct_summary would need insert_overwrite partitioned on "
            "date. A real design fork, not a detail.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # A smoke test reports; it does not gate. The workflow is continue-on-error too, but
        # exiting 0 keeps `| tee` and the artifact upload honest.
        print(f"\nsail_smoke could not run: {type(e).__name__}: {e}", file=sys.stderr)
        traceback.print_exc()
    sys.exit(0)
