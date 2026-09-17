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

import json
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

# TWO CREDENTIALS, NOT ONE -- the same split dbt2/profiles.yml spells out for the iceberg leg
# ("TWO SECRETS, NOT ONE": `azure` authorises the DATA files over abfss, `iceberg` authorises
# the REST CATALOG). The catalog's bearer_token above covers only the catalog. Without this,
# `show databases` and `create schema` both pass -- they are pure catalog calls -- and the
# first statement that touches storage dies with
#   Generic MicrosoftAzure error: ... GET http://169.254.169.254/metadata/identity/oauth2/token
#   ... 400 Bad Request: {"error":"invalid_request","error_description":"Identity not found"}
# because Sail walked its Azure credential chain down to the instance metadata endpoint. A
# hosted runner has no managed identity, and AZURE_CLIENT_ID being set for azure/login is what
# sends it there. Assignment, not setdefault: ours is the one that works.
os.environ["AZURE_STORAGE_TOKEN"] = TOKEN
os.environ.setdefault("SAIL_OPTIMIZER__ENABLE_JOIN_REORDER", "true")
os.environ.setdefault("SAIL_EXECUTION__COLLECT_STATISTICS", "true")


# The rows each probe should leave behind, tracked so probe 10 can tell "the merge ran" from
# "the merge returned". Start (1,10),(2,20); insert (3,30); merge updates 2 and adds 4; the
# insert-only merge must LEAVE 1 ALONE and add 5.
EXPECTED = [(1, 10), (2, 99), (3, 30), (4, 40), (5, 50)]

# EVERY CREATE MUST NAME IT. Sail's CREATE TABLE defaults to parquet and the OneLake
# Iceberg REST catalog refuses that outright -- "not supported: Iceberg REST catalog
# cannot create 'parquet' tables" (run 35188636093). This is what dbt-spark expresses as
# `file_format: iceberg`, so a real leg carries it in config; here it goes in the DDL.
FILE_FORMAT = "iceberg"

# THE MERGE TARGET NEEDS IT. Sail refuses a merge against an Iceberg table left on the
# default mode: "Iceberg MERGE with `write.merge.mode=copy-on-write` is not supported
# yet; set `write.merge.mode=merge-on-read`" (run 35188799850). Set as a property rather
# than worked around, because a real leg would carry it on every merged model 
# (dbt-spark spells that `tblproperties` in config) -- that is a finding about what the
# leg would cost, not a detail of this script.
MERGE_MODE = "'write.merge.mode'='merge-on-read'"

# Created BETWEEN probes 6 and 7 -- probe 6 has to read its own source before this table
# exists, so it cannot be a probe of its own. Hoisted to a constant anyway so tests_py can
# check the keys it introduces against EXPECTED.
INSERT_ONLY_SOURCE = "select 1 as k, 111 as v union all select 5, 50"


# ---- phase B: the shapes the MODELS need -------------------------------------------------
# Phase A proves the catalog accepts SQL. It does not prove this repo's gold layer could run
# on it: two-column literal tables exercise none of what the models do. The AEMO leg reads
# ragged quoted CSV off OneLake with an explicit all-STRING schema, captures provenance with
# input_file_name(), parses slash dates, explodes a generated date range, folds a 130-column
# record and merges on a multi-column key. Every one of those has broken an engine in this
# repo before -- Spark CASTs a slash date to NULL instead of erroring, Fabric Spark's catalog
# base32hex-decodes multipart names so `csv.`path`` is unusable, and its __dbt_tmp is a
# PERSISTENT view -- so each is probed rather than assumed.
LANDING_PATH = os.environ.get("LANDING_PATH", "")

# A DREGION-ish slice, not the real 130 columns. What differs by engine is the SHAPE -- all
# STRING on read, cast afterwards -- and the slash date, not the column count. Probe 19 covers
# the width separately.
CSV_SCHEMA = "`I` STRING, `REPORT` STRING, `SETTLEMENTDATE` STRING, `RRP` STRING"

# A schema WIDER than any AEMO row. The narrow one above is narrower than most of them, and
# the two cases fail differently: if the wide schema reads and the narrow one does not, only
# OVER-wide rows are rejected and declaring the record's full width (which aemo_columns.sql
# already knows) is enough. If both fail, `mode 'PERMISSIVE'` is simply not implemented and
# no schema saves it -- a PUBLIC_DAILY file holds many record types of different widths, so
# some row is always the wrong shape. That distinction is the difference between a config
# detail and a blocker, so it gets measured rather than guessed.
WIDE_SCHEMA = ", ".join(f"`c{i}` STRING" for i in range(200))

# AEMO ships yyyy/MM/dd. Spark's CAST returns NULL for it rather than erroring, which is why
# every model parses the format explicitly; a silent NULL reaches the gold layer.
SLASH_DATE = "2026/09/17 04:30:00"
DATE_FORMAT = "yyyy/MM/dd HH:mm:ss"

# Probes where an ERROR is a legitimate -- even preferable -- answer, so they are reported as
# NOTE and kept out of the phase B failure count. Only probe 21 so far: a bare CAST of a slash
# date returning NULL is the Spark trap, and refusing to parse is strictly better.
MAY_FAIL = {21}


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
        (3, "create table as select", f"create table {tgt} using {FILE_FORMAT} tblproperties ({MERGE_MODE}) "
         f"as select 1 as k, 10 as v "
                                      f"union all select 2, 20",
         "known-good by hand; anchors the run"),
        (4, "create merge source",
         f"create table {src} using {FILE_FORMAT} as "
         f"select 2 as k, 99 as v union all select 4, 40",
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


def ensure_insert_only_source(conn):
    """Probe 7's source table. Best-effort and loud: if it cannot be made, probe 7's
    failure is about this, not about the insert-only merge."""
    try:
        run(conn, f"create table {SCHEMA}.probe_src_insert_only using {FILE_FORMAT} as "
                  f"{INSERT_ONLY_SOURCE}")
    except Exception as e:
        print(f"    (probe 7's source could not be created: {oneline(e)})", flush=True)


def landing_files(limit=2):
    """Up to `limit` real AEMO CSV paths from the landing zone, newest name first.

    REAL files, not ones this script writes: the question is whether Sail can read what
    download_aemo.py actually lands -- ragged, quoted, many record types per file. Uses the
    OneLake DFS list API over urllib (stdlib; the probe env has no `requests` and should not
    need one) and returns [] on any failure, which the probes report as SKIP rather than
    FAIL. An empty landing zone is a fact about the landing zone, not about Sail.
    """
    if not LANDING_PATH:
        return []
    import urllib.error
    import urllib.parse
    import urllib.request

    # abfss://<ws>@onelake.dfs.fabric.microsoft.com/<item>/Files  ->  ws, "<item>/Files"
    try:
        rest = LANDING_PATH.split("://", 1)[1]
        ws, rest = rest.split("@", 1)
        _, path = rest.split("/", 1)
    except (IndexError, ValueError):
        print(f"    (could not parse LANDING_PATH: {LANDING_PATH})", flush=True)
        return []

    q = urllib.parse.urlencode({
        "resource": "filesystem", "recursive": "true",
        "directory": f"{path}/csv_raw", "maxResults": "200",
    })
    url = f"https://onelake.dfs.fabric.microsoft.com/{ws}?{q}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {TOKEN}"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            paths = json.load(r).get("paths", [])
    except (urllib.error.URLError, OSError, ValueError) as e:
        print(f"    (could not list the landing zone: {type(e).__name__}: {oneline(e)})",
              flush=True)
        return []

    names = sorted((e["name"] for e in paths
                    if not e.get("isDirectory") and e["name"].lower().endswith(".csv")),
                   reverse=True)
    if not names:
        print("    (no CSVs under Files/csv_raw -- has a real leg ever landed?)", flush=True)
    return [f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/{n}" for n in names[:limit]]


def model_shape_probes(files):
    """(number, name, sql, why) for what this repo's models actually do.

    An empty `files` leaves probes 12-14 with no SQL, which main() reports as SKIP.
    """
    view = "raw_probe"
    wide = f"{SCHEMA}.probe_wide"
    out = []

    if files:
        # The brace glob is load-bearing. spark_read_csv.sql names this run's files
        # explicitly as {a.CSV,b.CSV} rather than globbing the folder, so a run folds exactly
        # the files it decided to fold and a backlog converges instead of restarting.
        if len(files) == 1:
            path = files[0]
        else:
            folder = files[0].rsplit("/", 1)[0]
            path = folder + "/{" + ",".join(f.rsplit("/", 1)[1] for f in files) + "}"
        out += [
            (12, "csv view over abfss, AND READ IT",
             [f"create or replace temporary view {view} ({CSV_SCHEMA}) "
              f"using csv options (path '{path}', header 'true', mode 'PERMISSIVE')",
              f"select count(*) as n from {view}"],
             "the exact shape spark_read_csv.sql emits. The SELECT is the point: CREATE VIEW "
             "does not touch the file, so this probe reported PASS for three runs while "
             "reading was in fact broken (run 35206757444). A probe that cannot fail for its "
             "own stated reason measures nothing"),
            (13, "read it, with input_file_name()",
             f"select input_file_name() as _fname, `I`, `REPORT`, `SETTLEMENTDATE` "
             f"from {view} limit 5",
             "provenance -- the models' `file` column is parsed from this and exists "
             "nowhere else"),
            (14, "is that view TEMPORARY",
             f"show tables in {SCHEMA}",
             "Fabric Spark makes dbt's __dbt_tmp PERSISTENT, which is the entire reason the "
             "spark leg stages through a Delta table. A genuinely temporary view here means "
             "the sail leg is simpler than spark's"),
        ]
    else:
        for n, name in ((12, "csv temp view over abfss"),
                        (13, "read it, with input_file_name()"),
                        (14, "is that view TEMPORARY")):
            out.append((n, name, "", "SKIPPED: no landing files found"))
        # 22, 23 and 24 are appended below with empty SQL for the same reason; main()
        # reports an empty statement as SKIP.

    out += [
        (15, "slash date, explicit format",
         f"select to_timestamp('{SLASH_DATE}', '{DATE_FORMAT}') as parsed",
         "AEMO ships yyyy/MM/dd, so every model parses the format explicitly. ONE question "
         "per statement: this used to also select a bare CAST, and the bare cast is a hard "
         "parse error on Sail, so the whole probe died without answering this one"),
        (16, "sequence + explode",
         "select explode(sequence(to_date('2026-01-01'), to_date('2026-01-10'), "
         "interval 1 day)) as d",
         "dim_calendar is built this way on every engine"),
        (17, "window function",
         f"select k, row_number() over (partition by k % 2 order by v desc) as rn "
         f"from {SCHEMA}.probe_tgt",
         "used across the marts"),
        (18, "double -> decimal",
         "select cast(2.5 as double) as d, "
         "cast(cast(2.5 as double) as decimal(18,6)) as dec18_6, "
         "cast(cast(0.125 as double) as decimal(18,2)) as tie_break",
         "the money columns. Three engines round DOUBLE->DECIMAL three ways, which is why "
         "parity.py gives them a relative tolerance -- tie_break says which way Sail goes"),
        (19, "wide table (130 columns)",
         f"create table {wide} using {FILE_FORMAT} tblproperties ({MERGE_MODE}) as select "
         + ", ".join(f"cast({i} as double) as c{i}" for i in range(130)),
         "fct_price is AEMO's DREGION record -- all 130 columns, and fct_scada all 53"),
        (20, "merge on a three-column key",
         f"merge into {wide} as DBT_INTERNAL_DEST\n"
         f"    using (select "
         + ", ".join(f"cast({i if i < 3 else i + 1000} as double) as c{i}"
                     for i in range(130))
         + f") as DBT_INTERNAL_SOURCE\n"
         f"    on DBT_INTERNAL_SOURCE.c0 = DBT_INTERNAL_DEST.c0\n"
         f"       and DBT_INTERNAL_SOURCE.c1 = DBT_INTERNAL_DEST.c1\n"
         f"       and DBT_INTERNAL_SOURCE.c2 = DBT_INTERNAL_DEST.c2\n"
         f"    when not matched then insert *",
         "fct_summary merges on (date, time, DUID) and fct_price on four columns; a "
         "single-column key proves nothing about either. The source carries ALL 130 columns "
         "because `insert *` resolves them positionally by name -- a three-column source is "
         "'Cannot resolve source column c3 ... without schema evolution', which is a fact "
         "about the probe, not about Sail"),
        (22, "direct csv.`path` read",
         f"select count(*) as n from csv.`{files[0]}`" if files else "",
         "Fabric Spark CANNOT do this -- its catalog base32hex-decodes every part of a "
         "multipart name, so `csv.`path`` dies on the 'x'. If Sail can, the read is one "
         "statement instead of a view plus a stage table"),
        (23, "provenance WITHOUT input_file_name()",
         [f"create or replace temporary view probe_f{i} ({CSV_SCHEMA}) "
          f"using csv options (path '{f}', header 'true', mode 'PERMISSIVE')"
          for i, f in enumerate(files)]
         + [" union all ".join(
             f"select '{f.rsplit('/', 1)[1]}' as _fname, count(*) as n from probe_f{i}"
             for i, f in enumerate(files))]
         if files else "",
         "THE WORKAROUND for input_file_name() being unimplemented. spark_new_files already "
         "resolves this run's filenames at render time, so the name can be a LITERAL per "
         "file instead of a function -- one view per file, unioned. If this works the leg is "
         "not blocked, it just reads per file rather than one brace glob"),
        (24, "ragged CSV with a 200-column schema",
         [f"create or replace temporary view probe_wide_v ({WIDE_SCHEMA}) "
          f"using csv options (path '{files[0]}', header 'true', mode 'PERMISSIVE')",
          "select count(*) as n from probe_wide_v"] if files else "",
         "WIDER than any AEMO row. Reading here while the narrow schema fails means only "
         "OVER-wide rows are rejected, and declaring the record's full width is enough. "
         "Failing here too means PERMISSIVE is unimplemented and ragged files are "
         "unreadable at all -- the difference between a config detail and a blocker"),
        (21, "bare CAST of a slash date",
         f"select cast('{SLASH_DATE}' as timestamp) as bare_cast",
         "SEPARATE, and allowed to fail -- see MAY_FAIL. Spark returns NULL here rather than "
         "erroring, which is the trap that silently emptied a column on the spark leg. An "
         "ERROR is the BETTER outcome: it cannot reach the gold layer unnoticed"),
    ]
    return out


def interpret(n, rows):
    """Status for a phase B probe. Three of them mean something other than "it ran".

    Probe 14 SUCCEEDS either way -- the question is what it LISTED. Probe 15 succeeds even
    when the bare cast silently returns NULL, which is the exact Spark trap the models exist
    to dodge. Reporting "PASS" for those would be the same mistake probe 10 was added to stop.
    """
    if n == 14:
        listed = [str(r).lower() for r in rows]
        leaked = [r for r in listed if "raw_probe" in r]
        if leaked:
            return ("PERSISTENT - the temp view is listed in the schema, the same trap that "
                    "forces the spark leg to stage through a Delta table")
        return f"PASS - temporary, not listed ({len(rows)} table(s) in the schema)"

    if n == 15:
        if not rows or rows[0][0] is None:
            return "FAIL - the EXPLICIT format returned NULL; slash dates are unparseable"
        return f"PASS - parsed to {rows[0][0]}"

    if n == 21:
        # Reached only when the cast SUCCEEDED; the error path is handled as a NOTE.
        if rows and rows[0][0] is None:
            return ("SILENT NULL - Spark's trap exists here: a bare cast of a slash date "
                    "returns NULL instead of failing, so every model must parse the format")
        return f"PARSES - the bare cast works ({rows[0][0] if rows else '?'})"

    if n == 23:
        if not rows:
            return "FAIL - no rows"
        names = {str(r[0]) for r in rows}
        if any(not nm or nm == "None" for nm in names):
            return f"FAIL - a filename came back empty: {sorted(names)}"
        return (f"PASS - provenance without input_file_name(), {len(rows)} file(s): "
                f"{sorted(names)}")

    if n == 18 and rows:
        return f"PASS - tie_break={rows[0][2]} (0.12 is HALF_EVEN, 0.13 is HALF_UP)"

    return f"PASS ({len(rows)} row(s))"


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
        # BEFORE the statement, and outside probe 6's outcome entirely. This lived in
        # probe 6's success path, so a failed merge skipped it and probe 7 reported
        # "Table not found" instead of answering whether the insert-only shape works
        # (run 35188799850). The two merges are independent questions and have to be
        # able to fail independently.
        if n == 7:
            ensure_insert_only_source(conn)

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

    # ---- 12-20: the shapes the MODELS need -------------------------------------------
    # Phase A said the catalog takes SQL. This says whether it takes THIS repo's SQL.
    print("=" * 100, flush=True)
    print("phase B: the shapes the models actually need", flush=True)
    print("=" * 100 + "\n", flush=True)

    files = landing_files()
    if files:
        print(f"landing files: {len(files)}", flush=True)
        for f in files:
            print(f"  {f}", flush=True)
        print(flush=True)

    for n, name, sql, why in model_shape_probes(files):
        print(f"[{n}] {name} - {why}", flush=True)
        if not sql:
            print("    SKIP\n", flush=True)
            results.append((n, name, "SKIP - no landing files"))
            continue
        # A probe may be a LIST of statements. Only the last one's rows are reported; the
        # earlier ones are setup that has to happen on this same session (probe 23 needs one
        # view per file before it can union them).
        stmts = list(sql) if isinstance(sql, (list, tuple)) else [sql]
        for st in stmts:
            for line in st.split("\n"):
                print(f"    {line[:160]}", flush=True)
        try:
            for st in stmts[:-1]:
                run(conn, st)
            rows = run(conn, stmts[-1])
        except Exception as e:
            # For a MAY_FAIL probe the error IS the answer, so it is a NOTE and does not
            # count against phase B. Probe 21 refusing a slash date is better than Spark
            # quietly returning NULL for one.
            label = "NOTE" if n in MAY_FAIL else "FAIL"
            print(f"    {label} {type(e).__name__}: {oneline(e)}", flush=True)
            results.append((n, name, f"{label} - {type(e).__name__}: {oneline(e)}"))
            print(flush=True)
            continue

        for r in rows[:5]:
            print(f"    -> {str(r)[:200]}", flush=True)
        if len(rows) > 5:
            print(f"    -> ... {len(rows) - 5} more", flush=True)

        status = interpret(n, rows)
        print(f"    {status}\n", flush=True)
        results.append((n, name, status))

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
                     f"drop table if exists {SCHEMA}.probe_wide",
                     "drop view if exists raw_probe",
                     "drop view if exists probe_f0",
                     "drop view if exists probe_f1",
                     "drop view if exists probe_wide_v",
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

    # Close the client before the server: an open Spark Connect channel keeps the server's
    # actors busy, and stop() then waits on them.
    for what, closer in (("session", conn.stop), ("server", server.stop)):
        try:
            closer()
        except Exception as e:
            print(f"(could not stop the {what}: {type(e).__name__}: {oneline(e)})", flush=True)


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

    # Same rule one step later. With no table created, probes 6-10 can only report that it is
    # missing, and reading THAT as "merge is unsupported" or "listing is broken" is the same
    # confident wrong answer. A run where CREATE TABLE failed measures nothing below it.
    created = by_n.get(3, "")
    if not created.startswith("PASS"):
        hint = ""
        if "Identity not found" in created or "169.254.169.254" in created:
            hint = (" The error is Sail reaching the instance metadata endpoint for a STORAGE "
                    "token, which means the data-file credential never arrived -- the "
                    "catalog's bearer_token covers the catalog only. Set AZURE_STORAGE_TOKEN.")
        return (f"VERDICT: INCONCLUSIVE -- CREATE TABLE failed, so nothing was written and "
                f"probes 6-10 only report a missing table.{hint}")

    if not listing.startswith("PASS"):
        return ("VERDICT: not viable yet — `show table extended` did not list the schema, so "
                "dbt-spark cannot see existing relations and every run would silently "
                "full-refresh.")
    # Phase B is reported as its own line: phase A being green says the catalog works,
    # which is not the same claim as "this repo's models could run on it".
    shapes = [status for n, _, status in results if n >= 12]
    bad = [s for s in shapes if s.startswith("FAIL") or s.startswith("PERSISTENT")]
    skipped = [s for s in shapes if s.startswith("SKIP")]
    if bad:
        shape_note = (f" PHASE B: {len(bad)} of {len(shapes)} model shapes did NOT work -- "
                      f"see 12-20 above; those are the leg's real cost.")
    elif skipped:
        shape_note = (f" PHASE B: the rest passed, but {len(skipped)} CSV probe(s) were "
                      f"SKIPPED for want of landing files, so reading AEMO CSV is UNPROVEN.")
    else:
        shape_note = " PHASE B: every model shape works too, CSV read included."

    if merge_ok and verified:
        return ("VERDICT: viable — merge and relation listing both work and the writes "
                "verified. The spark model tree ports nearly as-is; next step is the full "
                "leg." + shape_note)
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

    # os._exit, NOT sys.exit. THE PROBES FINISH AND THE PROCESS DOES NOT EXIT: the embedded
    # Sail server runs a tokio runtime on non-daemon threads, and normal interpreter shutdown
    # joins them, so the job printed its entire summary and then sat idle until it was killed
    # (observed on run 35187028483: the report, then nothing, then the server shutting down
    # only once the run was cancelled). Every result is printed and flushed by this point, so
    # skipping interpreter cleanup costs nothing and is the only thing that reliably ends the
    # process. Flush explicitly first -- os._exit does not, and this goes through `| tee` into
    # the artifact.
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(0)
