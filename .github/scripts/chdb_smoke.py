"""Probe what chDB can do against OneLake on the Entra token this repo already mints.

NOT AN ENGINE. This is a smoke test for a candidate sixth engine, and its output is meant to
be read by a human — or pasted into an upstream issue. It never fails the workflow: a red
result IS the deliverable.

WHY IT EXISTS. chDB is ClickHouse in-process — the same embedded shape as the duckrun,
iceberg and ducklake legs, on a completely different core. The blocker for every candidate
here has been OneLake auth: OneLake takes an Entra bearer token and refuses account keys and
SAS outright. chDB has exactly one route that takes such a token, and it is not the obvious
one:

  * `DataLakeCatalog(...) SETTINGS catalog_type='onelake', onelake_bearer_token='<token>'`
    WORKS. The same string signs the catalog REST calls and the blob reads — chdb-core's
    RestCatalog.cpp builds the `Authorization` header from it, and
    Storages/ObjectStorage/Azure/Configuration.cpp wraps it in an AzureBlobStorage::
    StaticCredential. It is the same https://storage.azure.com/ audience pipeline.yml's
    "Mint OneLake token" step already produces for the iceberg and ducklake legs.
  * `azureBlobStorage()` / `icebergAzure()` / `deltaLakeAzure()` DO NOT. Their whole
    credential surface is a connection string, account_name+account_key, a SAS, or
    extra_credentials(client_id, tenant_id) -> WorkloadIdentityCredential. There is no slot
    for a bearer token. So `Tables/` is reachable through the catalog and `Files/` — the
    landing zone every model in this repo reads — is not.

Both of those are claims read off chdb-core's source. This job MEASURES them against the real
lakehouse, because "we read the source" is not the same as "we ran it", and the entry that
lands in docs/candidate-engines.md should be a result.

THE THIRD QUESTION, and the one that decides whether chDB could be a LEG rather than just a
reader: can it write. ClickHouse's support matrix lists OneLake as one of only three catalogs
that are NOT read-only — with Unity and SeaweedFS — CREATE TABLE and INSERT both Beta, while
Glue, Iceberg REST, BigLake, Lakekeeper and Nessie are read-only. Probes 7 and 8 measure it.

AN EARLIER VERSION OF THIS FILE ANSWERED THAT QUESTION WRONG, and how is worth keeping.
It read `DatabaseDataLake::createTable`, saw an empty body, and concluded a create through
the catalog registers nothing. That hook is not the create path: a datalake database holds no
table metadata of its own, so creation goes through the catalog instead --
IcebergMetadata::createInitial writes a v1.metadata.json into the lake and then calls
catalog->createTable(namespace, table, ...). The probe then tested a statement that could
never reach it (ENGINE = Memory, added to dodge an unrelated MergeTree error) with the write
path switched off (no allow_insert_into_iceberg), got the empty hook, and published
"VERDICT: READER ONLY" off the back of it. Reading source and then testing the wrong
statement is worse than not testing, and searching the docs first would have caught it.

WHAT THE PROBE MAY WRITE, AND WHERE. Probes 7 and 8 create and populate ONE table in the
`chdb_smoke` namespace, run-scoped by GITHUB_RUN_ID. Nothing else is ever written.

AND IT NEVER DROPS. DatabaseDataLake::dropTable calls table->drop(), which unregisters the
real table -- and every other table in this catalog is a leg's gold layer. There is no DROP
anywhere below and no DROP DATABASE either, so the guard needs no exceptions and cannot be
typo'd into deleting a mart. The cost is that each run leaves a small table behind in
`chdb_smoke`; drop that namespace by hand if it ever accumulates.
tests_py/test_chdb_smoke.py pins both halves.

Env (the workflow supplies all of these):
    WAREHOUSE_PATH      "{workspace_id}/{lakehouse_id}" — the same value provision.py's
                        iceberg branch emits, and what the OneLake catalog calls `warehouse`
    ONELAKE_TOKEN       the https://storage.azure.com/ token, the same one the iceberg leg uses
    LANDING_PATH        abfss:// URL of the landing lakehouse's Files section (optional; the
                        phase B CSV probes SKIP without it)
    CHDB_SMOKE_SCHEMA   namespace for probe 7's create attempt (default chdb_smoke —
                        deliberately outside the <engine>_landing / <engine>_mart namespace
                        the five engines share)
    CHDB_SMOKE_TABLE    pin the read probes to one catalog table instead of letting the
                        script pick

Usage:
    python .github/scripts/chdb_smoke.py
"""

import json
import os
import sys
import traceback

WAREHOUSE = os.environ["WAREHOUSE_PATH"]
TOKEN = os.environ["ONELAKE_TOKEN"]
SCHEMA = os.environ.get("CHDB_SMOKE_SCHEMA", "chdb_smoke")
PIN_TABLE = os.environ.get("CHDB_SMOKE_TABLE", "")
LANDING_PATH = os.environ.get("LANDING_PATH", "")

# The attached catalog's name INSIDE chDB. Not `onelake`, which is what dbt1/profiles.yml
# calls the iceberg leg's DuckDB attachment — a probe sharing a name with a leg's live
# attachment is the kind of thing that reads as shared state when it is not.
DB = "onelake_probe"

# Fabric's Iceberg table API, and it covers MORE THAN THE ICEBERG LEG. The first run listed
# 21 tables across BOTH `duckrun_*` and `iceberg_*` (run 35433920589): Fabric exposes the
# Delta tables duckrun writes through this endpoint too, so the read probes below ran against
# a Delta mart and counted 109M rows. Worth stating because the obvious assumption -- an
# Iceberg endpoint shows Iceberg tables -- is wrong here and would have had this probe
# reporting on one leg when it can see two.
CATALOG_URL = "https://onelake.table.fabric.microsoft.com/iceberg"

# THE GATE. The catalog database engine is BETA upstream and refuses to be created without
# it; `allow_database_iceberg` is the alias, `allow_experimental_database_iceberg` the
# original name. Session-scoped SET, because the setting has to be live when CREATE DATABASE
# runs and a `SETTINGS` clause there configures the DATABASE, not the query.
GATE = "allow_database_iceberg"

# THE BLOB ENDPOINT, not the DFS one. OneLake serves the same bytes on both hosts; chDB's
# OneLake catalog reads through .blob by default (onelake_use_blob_endpoint), and the url()
# probes below follow it so a failure there is not confused with a host mismatch.
BLOB_HOST = "onelake.blob.fabric.microsoft.com"
DFS_HOST = "onelake.dfs.fabric.microsoft.com"

# Azure Blob's REST API REQUIRES this header when the request is authorised with a bearer
# token rather than a shared key. Without it the GET comes back 400 and reads like a bad
# token. Not needed by the catalog path, which goes through the Azure SDK.
MS_VERSION = "2023-11-03"

# AEMO ships yyyy/MM/dd. ClickHouse's parseDateTime takes a MySQL-style format string; the
# bare toDateTime is probed separately (probe 14) because returning NULL instead of erroring
# is the trap the models exist to dodge, and an ERROR there is the better outcome.
SLASH_DATE = "2026/09/17 04:30:00"
DATE_FORMAT = "%Y/%m/%d %H:%i:%S"

# A PUBLIC_DAILY holds many record types -- DUNIT is 53 columns, DREGION 130 -- so the
# declared structure has to be at least as wide as the widest record, and every narrower row
# needs input_format_csv_allow_variable_number_of_columns. Same pair the sail probe found,
# said in ClickHouse.
CSV_WIDTH = 131
CSV_STRUCTURE = ", ".join(f"c{i} String" for i in range(CSV_WIDTH))
RAGGED = "input_format_csv_allow_variable_number_of_columns=1"

# Probes where an ERROR is a legitimate -- even expected -- answer, so they are reported as
# NOTE and kept out of the failure count.
#   9   THE CONTROL. azureBlobStorage() has no bearer-token argument, so this is expected to
#       be refused. Its failure is what turns "Files/ is unreachable" from a reading of
#       Configuration.cpp into a measurement.
#   14  a bare toDateTime of a slash date returning NULL is the Spark trap; refusing to parse
#       is strictly better.
MAY_FAIL = {9, 14}

# WRITE GATE. One setting turns on the whole Iceberg write path -- INSERT and the initial
# CREATE alike (IcebergMetadata.cpp checks it in createInitial as well as the write paths).
# Without it probes 7 and 8 are refused, and the refusal looks like "OneLake is read-only"
# rather than "the probe left the gate shut", which is the mistake this file already made.
WRITE_GATE = "allow_insert_into_iceberg"

# Probes 7-8's table. The namespace is one no leg uses, and the name is RUN-SCOPED: nothing
# here ever drops anything, so a fixed name would be created once and every later run would
# measure "it already exists" instead of "the create worked".
PROBE_TABLE = f"{SCHEMA}.probe_{os.environ.get('GITHUB_RUN_ID', 'local')}"


def quoted(name):
    """A catalog table name as chDB needs it written.

    The OneLake catalog reports `<namespace>.<table>` as ONE name, and ClickHouse has no
    second namespace level to put it in, so the whole thing goes inside one pair of
    backticks: onelake_probe.`iceberg_mart.fct_price`. Written unquoted it resolves as a
    database called `iceberg_mart` that does not exist.
    """
    return "`" + name.replace("`", "``") + "`"


def attach_sql():
    """The CREATE DATABASE, and the banner's redacted twin.

    FOUR SETTINGS, AND THE DOCUMENTED STATEMENT HAS SEVEN. oauth_server_uri, auth_scope and
    onelake_tenant_id all belong to the CLIENT-CREDENTIALS path: that one exchanges an id and
    secret for a token against a tenant's endpoint. The bearer path does no exchange at all --
    OneLakeCatalog stores the token straight into the auth header and Configuration.cpp wraps
    it in a StaticCredential -- so the tenant is stored and never read.

    This carried onelake_tenant_id anyway, on the grounds that the documented CREATE DATABASE
    carries it. That documented statement is the client-secret example, and copying settings
    across from it is how a dead argument comes to look required. Dropped after a hand-run
    confirmed the attach works without it.
    """
    def build(token):
        return (
            f"CREATE DATABASE {DB}\n"
            f"ENGINE = DataLakeCatalog('{CATALOG_URL}')\n"
            f"SETTINGS catalog_type = 'onelake',\n"
            f"         warehouse = '{WAREHOUSE}',\n"
            f"         onelake_bearer_token = '{token}'"
        )
    return build(TOKEN), build("***")


def phase_a_probes(table, cols):
    """(number, name, sql, why) for the catalog contract, 3-8.

    Probes 1 and 2 are run by main() before this: the table these read has to be DISCOVERED
    from the catalog listing, and a probe list that hardcoded one would report FAIL for a
    workspace whose marts are simply named differently.

    `cols` is DESCRIBE's answer as [(name, type), ...]. The aggregate probes pick a date-like
    and a string column out of it rather than assuming settlementdate/duid, so this measures
    chDB rather than measuring whether the probe guessed the schema.
    """
    t = f"{DB}.{quoted(table)}"
    tcol = first_col(cols, ("Date", "DateTime"))
    scol = first_col(cols, ("String",))

    if tcol:
        agg = (f"SELECT min({tcol}) AS lo, max({tcol}) AS hi"
               + (f", uniqExact({scol}) AS keys" if scol else "")
               + f" FROM {t}")
        why_agg = (f"a real aggregate over real data, not a row count -- min/max of {tcol}"
                   + (f" and the distinct count of {scol}" if scol else ""))
        pred = (f"SELECT count() AS n FROM {t} "
                f"WHERE {tcol} >= (SELECT max({tcol}) FROM {t})")
        why_pred = (f"a predicate over {tcol}. Reading a mart through the catalog should not "
                    f"mean a full scan for every filtered query")
    else:
        agg = pred = ""
        why_agg = why_pred = (f"SKIPPED: {table} has no Date/DateTime column to aggregate "
                              f"over, so this would measure the probe's guess and not chDB")

    return [
        (3, "describe through the catalog", f"DESCRIBE {t}",
         "column and type discovery -- what an adapter would introspect"),
        (4, "count() -- the first STORAGE read", f"SELECT count() AS n FROM {t}",
         "THE ONE THAT ANSWERS THE ORIGINAL QUESTION. Probes 1-3 are catalog REST calls; this "
         "is the first statement that fetches a data file, so it is where one bearer token "
         "signing BOTH the catalog and the blob is proved or disproved"),
        (5, "aggregate over real data", agg, why_agg),
        (6, "filtered read", pred, why_pred),
        (7, "CREATE TABLE through the catalog",
         f"CREATE TABLE {DB}.{quoted(PROBE_TABLE)} (k Int64, v Int64) ENGINE = Iceberg",
         "CAN A LEG MATERIALISE A MODEL AT ALL. ClickHouse's support matrix lists OneLake as "
         "one of three catalogs that are NOT read-only (with Unity and SeaweedFS): CREATE "
         "TABLE and INSERT are both Beta, and IcebergMetadata::createInitial writes a "
         "v1.metadata.json into the lake and then registers it with catalog->createTable. "
         "main() re-lists the catalog afterwards rather than believing the statement. "
         "TWO THINGS THIS GOT WRONG BEFORE, and together they produced a whole wrong verdict "
         "(run 35434183971 reported READER ONLY): no allow_insert_into_iceberg, which gates "
         "the entire write path including create; and ENGINE = Memory, added to dodge a "
         "MergeTree error, which routed the statement away from the Iceberg writer so the "
         "empty IDatabase::createTable hook ran and registered nothing. That hook is not the "
         "create path -- a datalake database does not hold its own table metadata -- and "
         "reading source then testing the wrong statement is worse than not testing"),
        (8, "INSERT INTO",
         f"INSERT INTO {DB}.{quoted(PROBE_TABLE)} VALUES (1, 10), (2, 20)",
         "the append every insert-only fact in this repo would take. INTO THE PROBE'S OWN "
         "TABLE, in a namespace no leg uses -- the legs' marts are not a test fixture. "
         "main() reads the rows back afterwards, because a write that returns is not a write "
         "that landed"),
    ]


def first_col(cols, kinds):
    """First column whose type starts with one of `kinds`. Nullable(...) counts."""
    for name, typ in cols:
        bare = typ.replace("Nullable(", "").replace("LowCardinality(", "")
        if bare.startswith(tuple(kinds)):
            return name
    return None


def blob_url(name):
    """A OneLake DFS listing entry -> the .blob URL url() can GET.

    The listing's `name` is already <item>/<section>/... relative to the workspace
    filesystem, so the workspace goes in front and nothing else is rewritten.
    """
    ws = LANDING_PATH.split("://", 1)[1].split("@", 1)[0]
    return f"https://{BLOB_HOST}/{ws}/{name}"


def headers_sql(token):
    return (f"headers('Authorization'='Bearer {token}', "
            f"'x-ms-version'='{MS_VERSION}')")


def model_shape_probes(files):
    """(number, name, sql, why) for what this repo's models actually do, 9-16.

    An empty `files` leaves the CSV probes with no SQL, which main() reports as SKIP: an
    empty landing zone is a fact about the landing zone, not a finding about chDB.
    """
    out = []
    u = blob_url(files[0]) if files else ""
    h = headers_sql(TOKEN)

    if files:
        # OneLake's blob endpoint puts the WORKSPACE where a storage account puts a container
        # and the item where it puts the blob path, so the split is after the host and then
        # once more. Shaped correctly on purpose: a malformed url would fail for its own
        # reason and the control would stop being attributable to the missing token slot.
        ws, blob = u.split(f"{BLOB_HOST}/", 1)[1].split("/", 1)
        out.append(
            (9, "azureBlobStorage() with the token in every slot it has",
             f"SELECT count() AS n FROM azureBlobStorage("
             f"'https://{BLOB_HOST}', '{ws}', '{blob}', "
             f"'{TOKEN}', '{TOKEN}', 'LineAsString')",
             "THE CONTROL, and expected to FAIL -- see MAY_FAIL. The function's whole "
             "credential surface is connection string / account_name+account_key / SAS / "
             "extra_credentials(client_id, tenant_id), so the token can only be smuggled "
             "into the account slots. Its refusal is what makes 'Files/ is unreachable' a "
             "measurement instead of a reading of Configuration.cpp"))
        out.append(
            (10, "url() + Authorization header -- the escape hatch",
             f"SELECT count() AS n FROM url('{u}', LineAsString, {h})",
             "the ONLY way a bearer token reaches Files/. LineAsString on purpose: this "
             "probe asks whether the BYTES arrive, so a CSV parsing failure cannot be "
             "mistaken for an auth failure -- and with NO explicit structure, because "
             "LineAsString's is fixed at one column and naming it is the documented way to "
             "confuse the argument list. x-ms-version is required -- Azure Blob's REST API "
             "rejects an OAuth request without it, and a 400 there reads like a bad token"))
        out.append(
            (11, "ragged AEMO CSV through url()",
             f"SELECT count() AS n FROM url('{u}', CSV, '{CSV_STRUCTURE}', {h}) "
             f"SETTINGS {RAGGED}",
             "can chDB read what download_aemo.py actually lands. A PUBLIC_DAILY holds many "
             "record types in one file -- DUNIT 53 columns, DREGION 130 -- so it takes a "
             "structure padded past the widest record AND a setting allowing every narrower "
             "row. Either alone fails, in opposite directions"))
        out.append(
            (12, "provenance -- _file over url()",
             f"SELECT _file AS f, count() AS n FROM url('{u}', CSV, '{CSV_STRUCTURE}', {h}) "
             f"GROUP BY 1 SETTINGS {RAGGED}",
             "the models' `file` column is parsed from the source filename and exists nowhere "
             "else. Every other engine here has a way to name it -- DuckDB's filename, "
             "Fabric's filepath(), Spark's input_file_name() -- which is why "
             "macros/parse_filename.sql has a dialect branch at all"))
    else:
        for n, name in ((9, "azureBlobStorage() with the token in every slot it has"),
                        (10, "url() + Authorization header -- the escape hatch"),
                        (11, "ragged AEMO CSV through url()"),
                        (12, "provenance -- _file over url()")):
            out.append((n, name, "", "SKIPPED: no landing files found"))

    out += [
        (13, "slash date, explicit format",
         f"SELECT parseDateTime('{SLASH_DATE}', '{DATE_FORMAT}') AS parsed",
         "AEMO ships yyyy/MM/dd, so every model parses the format explicitly. ONE question "
         "per statement, so a hard parse error on the bare cast cannot take this one with it"),
        (14, "bare toDateTime of a slash date",
         f"SELECT toDateTime('{SLASH_DATE}') AS bare_cast",
         "SEPARATE, and allowed to fail -- see MAY_FAIL. Spark returns NULL here rather than "
         "erroring, which is the trap that silently emptied a column on the spark leg. An "
         "ERROR is the BETTER outcome: it cannot reach the gold layer unnoticed"),
        (15, "double -> decimal",
         "SELECT toDecimal64(toFloat64(2.5), 6) AS dec18_6, "
         "toDecimal64(toFloat64(0.125), 2) AS tie_break",
         "the money columns. Engines round DOUBLE->DECIMAL differently, which is why "
         "parity.py gives them a relative tolerance -- tie_break says which way chDB goes "
         "(0.12 is HALF_EVEN, 0.13 is HALF_UP)"),
        (16, "date range explode",
         "SELECT arrayJoin(arrayMap(x -> toDate('2026-01-01') + x, range(10))) AS d",
         "dim_calendar is built from a generated date range on every engine. ClickHouse has "
         "no sequence()/explode(); this is the spelling a chdb leg would carry"),
        (17, "window function",
         "SELECT k, row_number() OVER (PARTITION BY k % 2 ORDER BY v DESC) AS rn "
         "FROM (SELECT 1 AS k, 10 AS v UNION ALL SELECT 2, 20 UNION ALL SELECT 3, 30)",
         "used across the marts"),
        (18, "brace glob over this run's files",
         (f"SELECT _file AS f, count() AS n FROM url('{brace_glob(files)}', CSV, "
          f"'{CSV_STRUCTURE}', {h}) GROUP BY 1 ORDER BY 1 SETTINGS {RAGGED}")
         if len(files) >= 2 else "",
         "THE ONE THAT DECIDES WHAT A LEG'S READ LOOKS LIKE. url() expands *, {a,b}, {N..M} "
         "and ** in the path, and {a,b} is client-side -- which is exactly the shape "
         "spark_read_csv.sql already emits, naming this run's files explicitly so a backlog "
         "converges instead of restarting. If this works the leg's read is ONE statement over "
         "the files it chose; if it does not, it is one url per file and the leg carries its "
         "own enumeration. GROUP BY _file because a glob that reads only the first file would "
         "otherwise look identical to one that read both"),
    ]
    return out


def brace_glob(files):
    """<folder>/{a.CSV,b.CSV} -- the same construction spark_read_csv.sql renders.

    A FOLDER GLOB WOULD BE A DIFFERENT QUESTION and an easier one: `*` against a remote
    listing is not what the models do, because a run folds exactly the files it decided to
    fold. So the alternation is built from the real names, not from a wildcard.
    """
    urls = [blob_url(f) for f in files]
    folder = urls[0].rsplit("/", 1)[0]
    return folder + "/{" + ",".join(u.rsplit("/", 1)[1] for u in urls) + "}"


def landing_files(limit=2):
    """Up to `limit` real AEMO CSV paths from the landing zone, newest name first.

    REAL files, not ones this script writes: the question is whether chDB can read what
    download_aemo.py actually lands -- ragged, quoted, many record types per file. Uses the
    OneLake DFS list API over urllib (stdlib; the probe env has no `requests` and should not
    need one) and returns [] on any failure, which the probes report as SKIP rather than FAIL.

    TWO, because probe 18 needs two. This was 1, on the stated grounds that "url() takes ONE
    url and there is no brace-glob form to exercise here" -- which was never measured and is
    wrong: url() expands *, {a,b}, {N..M} and ** in the path, and {a,b} is exactly the shape
    spark_read_csv.sql already emits. A probe that asserts a limit it never tested is the
    thing this file exists to not do, and the verdict repeated the claim for a whole run.
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
    url = f"https://{DFS_HOST}/{ws}?{q}"
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
    return names[:limit]


def interpret(n, rows):
    """Status for a probe whose meaning is not "it ran".

    Probes 7 and 8 are the reason this exists. Neither is answered by its own statement: a
    create through a catalog can return success and register nothing, and an insert can
    return without landing a row. `rows` for 7 is the catalog listing taken AFTER the create;
    for 8 it is a read-back of the table.
    """
    if n == 7:
        listed = {str(r[0]) for r in rows}
        if PROBE_TABLE in listed:
            return (f"PASS -- {PROBE_TABLE} is registered in the catalog, so a chdb leg could "
                    f"materialise a model. Left in place: nothing here drops anything")
        return ("SILENT NO-OP -- the statement succeeded and the table is NOT in the catalog. "
                "The failure mode to distrust, and the one that produced a wrong verdict "
                "once: check allow_insert_into_iceberg is on and that the ENGINE reached the "
                "Iceberg writer rather than a local storage engine")

    if n == 8:
        got = sorted(tuple(r) for r in rows)
        if got == [(1, 10), (2, 20)]:
            return "PASS -- both rows read back; the insert landed, not merely returned"
        if not rows:
            return ("SILENT NO-OP -- the INSERT returned and the table is EMPTY. The worst "
                    "outcome here and the most worth reporting upstream; do not build on it")
        return f"MISMATCH -- expected [(1, 10), (2, 20)], got {got}"

    if n == 13:
        if not rows or rows[0][0] is None:
            return "FAIL - the EXPLICIT format returned NULL; slash dates are unparseable"
        return f"PASS - parsed to {rows[0][0]}"

    if n == 14:
        # Reached only when the cast SUCCEEDED; the error path is handled as a NOTE.
        if rows and rows[0][0] is None:
            return ("SILENT NULL - Spark's trap exists here too: a bare cast of a slash date "
                    "returns NULL instead of failing, so every model must parse the format")
        return f"PARSES - the bare cast works ({rows[0][0] if rows else '?'})"

    if n == 15 and rows:
        return f"PASS - tie_break={rows[0][1]} (0.12 is HALF_EVEN, 0.13 is HALF_UP)"

    if n == 12:
        if not rows:
            return "FAIL - no rows"
        names = {str(r[0]) for r in rows}
        if any(not nm or nm == "None" for nm in names):
            return f"FAIL - _file came back empty: {sorted(names)}"
        return f"PASS - _file resolves: {sorted(names)}"

    if n == 18:
        # A GLOB THAT READ ONE FILE IS NOT A GLOB, and from the row count alone it looks
        # exactly like one that read both -- which is why the statement groups by _file.
        if len(rows) < 2:
            got = sorted(str(r[0]) for r in rows)
            return (f"PARTIAL - the statement ran but only {len(rows)} file(s) came back "
                    f"({got}); the alternation did not expand")
        return (f"PASS - ONE statement read {len(rows)} files: "
                f"{sorted(str(r[0]) for r in rows)}")

    return f"PASS ({len(rows)} row(s))"


def run(sess, sql):
    """Execute and materialise. Returns rows as tuples; DDL returns [].

    JSONCompactEachRow, NOT JSONCompact, AND THE DIFFERENCE IS NOT COSMETIC. JSONCompact
    wraps everything in ONE document with meta/data/rows/statistics, and probe 10 died on
    `json.loads` with "Extra data: line 24 column 1" (run 35433920589) -- a second document
    trailing the first -- while probes 11 and 12 parsed fine off the same url. A result
    reader that fails for some queries and not others turns a working probe into a FAIL and
    then into a wrong verdict, which is what happened. One JSON array per line cannot have
    extra data after it.
    """
    out = str(sess.query(sql, "JSONCompactEachRow")).strip()
    if not out:
        return []
    return [tuple(json.loads(line)) for line in out.splitlines() if line.strip()]


def oneline(e):
    return " ".join(str(e).split("\n")[0].split())


def banner():
    """Everything an upstream issue needs to reproduce, with the token redacted."""
    try:
        import chdb
        lines = [f"chdb          {getattr(chdb, '__version__', '?')}"]
    except ImportError:
        lines = ["chdb          (not importable)"]
    lines.append(f"catalog       {CATALOG_URL}")
    lines.append(f"warehouse     {WAREHOUSE}")
    lines.append(f"landing       {LANDING_PATH or '(unset -- CSV probes will SKIP)'}")
    lines.append(f"probe schema  {SCHEMA}")
    return lines


def pick_table(rows):
    """The catalog table the read probes use.

    CHDB_SMOKE_TABLE wins. Otherwise prefer a mart fact -- that is the gold layer, the thing
    a sixth engine would have to agree with -- and fall back to whatever the catalog listed
    first so a differently-named workspace still gets measured.
    """
    names = [str(r[0]) for r in rows]
    if PIN_TABLE:
        return PIN_TABLE
    for want in ("_mart.fct_", "_mart.dim_", "_mart.", "_landing."):
        for name in names:
            if want in name:
                return name
    return names[0] if names else ""


def main():
    from chdb import session

    print("\n".join(banner()), flush=True)
    print(flush=True)

    # In-memory and ephemeral: no path, so nothing survives the process and there is nothing
    # to clean up. Deliberate -- the alternative is a persistent session holding a catalog
    # attachment whose DROP path deletes real data.
    sess = session.Session()

    results = []
    attach, redacted = attach_sql()

    # ---- 1: does chDB take the token at all ------------------------------------------
    print(f"[1] attach the OneLake catalog - THE QUESTION THIS JOB EXISTS FOR", flush=True)
    for line in (f"SET {GATE}=1\nSET {WRITE_GATE}=1\n" + redacted).split("\n"):
        print(f"    {line}", flush=True)
    try:
        run(sess, f"SET {GATE}=1")
        # Probes 7 and 8 are refused without it, and the refusal reads as "OneLake is
        # read-only" rather than "the gate was shut" -- which is the mistake this file made.
        run(sess, f"SET {WRITE_GATE}=1")
        run(sess, attach)
        results.append((1, "attach the OneLake catalog", "PASS"))
        print("    PASS\n", flush=True)
    except Exception as e:
        print(f"    FAIL {type(e).__name__}: {oneline(e)}", flush=True)
        traceback.print_exc()
        results.append((1, "attach the OneLake catalog",
                        f"FAIL - {type(e).__name__}: {oneline(e)}"))
        print(flush=True)

    # ---- 2: what does the catalog list -----------------------------------------------
    listing = []
    print(f"[2] list the catalog - does it see the legs' tables", flush=True)
    print(f"    SHOW TABLES FROM {DB}", flush=True)
    try:
        listing = run(sess, f"SHOW TABLES FROM {DB}")
        for r in listing[:10]:
            print(f"    -> {r}", flush=True)
        if len(listing) > 10:
            print(f"    -> ... {len(listing) - 10} more", flush=True)
        status = f"PASS ({len(listing)} table(s))"
        if not listing:
            status = ("EMPTY - the catalog answered and listed NOTHING; has ANY leg "
                      "ever run in this workspace?")
            print(f"    ::warning::{status}", flush=True)
        results.append((2, "list the catalog", status))
        print(f"    {status}\n", flush=True)
    except Exception as e:
        print(f"    FAIL {type(e).__name__}: {oneline(e)}", flush=True)
        results.append((2, "list the catalog", f"FAIL - {type(e).__name__}: {oneline(e)}"))
        print(flush=True)

    table = pick_table(listing)
    cols = []
    if table:
        print(f"reading through: {table}\n", flush=True)
        try:
            # DESCRIBE's first two columns are name and type. Taken before the probe list is
            # built because probes 5 and 6 pick their columns out of it.
            cols = [(str(r[0]), str(r[1]))
                    for r in run(sess, f"DESCRIBE {DB}.{quoted(table)}")]
        except Exception as e:
            print(f"(DESCRIBE failed up front, so probes 5-6 will skip: {oneline(e)})\n",
                  flush=True)

    for n, name, sql, why in phase_a_probes(table or "(none)", cols):
        print(f"[{n}] {name} - {why}", flush=True)
        if not sql or not table:
            reason = "no table to read" if not table else "no SQL"
            print(f"    SKIP\n", flush=True)
            results.append((n, name, f"SKIP - {reason}"))
            continue
        for line in sql.split("\n"):
            print(f"    {line}", flush=True)
        try:
            rows = run(sess, sql)
        except Exception as e:
            print(f"    FAIL {type(e).__name__}: {oneline(e)}", flush=True)
            traceback.print_exc()
            results.append((n, name, f"FAIL - {type(e).__name__}: {oneline(e)}"))
            print(flush=True)
            continue

        # NEITHER 7 NOR 8 IS ANSWERED BY ITS OWN STATEMENT. A create can return success and
        # register nothing; an insert can return without landing a row. So the catalog is
        # re-listed after 7 and the table read back after 8, and interpret() reads THAT.
        if n in (7, 8):
            after = (f"SHOW TABLES FROM {DB}" if n == 7 else
                     f"SELECT k, v FROM {DB}.{quoted(PROBE_TABLE)} ORDER BY k")
            try:
                rows = run(sess, after)
            except Exception as e:
                rows = []
                print(f"    (could not verify: {oneline(e)})", flush=True)
            if n == 8:
                for r in rows[:5]:
                    print(f"    -> {str(r)[:200]}", flush=True)
        else:
            for r in rows[:5]:
                print(f"    -> {str(r)[:200]}", flush=True)
            if len(rows) > 5:
                print(f"    -> ... {len(rows) - 5} more", flush=True)

        status = interpret(n, rows)
        if status.startswith("SILENT NO-OP") or status.startswith("MISMATCH"):
            print(f"    ::warning::{status}", flush=True)
        print(f"    {status}\n", flush=True)
        results.append((n, name, status))

    # ---- 9-17: the shapes the MODELS need --------------------------------------------
    # Phase A says the catalog works. This says whether this repo's gold layer could be
    # BUILT on it, which is a different claim -- and the Files/ probes are where it is
    # decided, because every model here starts by reading a CSV out of the landing zone.
    print("=" * 100, flush=True)
    print("phase B: the shapes the models actually need", flush=True)
    print("=" * 100 + "\n", flush=True)

    files = landing_files()
    for f in files:
        print(f"landing file: {f}", flush=True)
    if files:
        print(flush=True)

    for n, name, sql, why in model_shape_probes(files):
        print(f"[{n}] {name} - {why}", flush=True)
        if not sql:
            # Probe 18 needs TWO files, so "no landing files" would be the wrong reason when
            # the zone holds exactly one -- and a wrong skip reason sends the next person
            # looking at the landing zone instead of at the probe.
            reason = ("needs two landing files, found "
                      f"{len(files)}") if n == 18 else "no landing files"
            print("    SKIP\n", flush=True)
            results.append((n, name, f"SKIP - {reason}"))
            continue
        for line in sql.split("\n"):
            print(f"    {redact(line)[:200]}", flush=True)
        try:
            rows = run(sess, sql)
        except Exception as e:
            # For a MAY_FAIL probe the error IS the answer, so it is a NOTE and does not
            # count against phase B.
            label = "NOTE" if n in MAY_FAIL else "FAIL"
            print(f"    {label} {type(e).__name__}: {redact(oneline(e))}", flush=True)
            results.append((n, name, f"{label} - {type(e).__name__}: {redact(oneline(e))}"))
            print(flush=True)
            continue

        for r in rows[:5]:
            print(f"    -> {str(r)[:200]}", flush=True)
        if len(rows) > 5:
            print(f"    -> ... {len(rows) - 5} more", flush=True)

        status = interpret(n, rows)
        print(f"    {status}\n", flush=True)
        results.append((n, name, status))

    report(results)


def redact(text):
    """The token appears INSIDE phase B's SQL -- in url()'s headers() and in probe 9's
    account slots -- so it is in the statements this script prints and in the errors those
    statements raise. ::add-mask:: covers the workflow's own log, and this covers the
    artifact, which is the thing that gets attached to an upstream issue."""
    return text.replace(TOKEN, "***") if TOKEN else text


def report(results):
    out = ["=" * 100,
           "chDB smoke - OneLake access on an Entra bearer token",
           "-" * 100]
    for n, name, status in results:
        out.append(f"{n:>3}  {name:<44}{status}")
    out.append("=" * 100)

    verdict = read_verdict(results)
    out.append(verdict)
    out.append("=" * 100)
    print("\n".join(out), flush=True)

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write("## 🏠 chDB smoke (OneLake catalog, bearer token)\n\n")
            f.write("| # | probe | result |\n|---|---|---|\n")
            for n, name, status in results:
                f.write(f"| {n} | {name} | {status} |\n")
            f.write(f"\n{verdict}\n\n")


def read_verdict(results):
    """Say what the result MEANS for a sixth engine.

    ORDERED SO A MISCONFIGURATION NEVER READS AS A FINDING. If the catalog was never
    attached, nothing below it is evidence about chDB -- and a verdict drawn from the
    probe's own setup failure is worse than no verdict. The sail probe learned that across
    three runs; this one starts with it.
    """
    by_n = {n: status for n, _, status in results}

    if not by_n.get(1, "").startswith("PASS"):
        return ("VERDICT: INCONCLUSIVE -- the catalog was never attached, so nothing here "
                "says anything about chDB. Two things break it: `warehouse` must be "
                "<workspace-id>/<lakehouse-id> and not a name, and the token must be the "
                "https://storage.azure.com/ audience -- an api.fabric.microsoft.com token "
                "authenticates and then reads nothing.")

    if by_n.get(2, "").startswith("EMPTY"):
        return ("VERDICT: INCONCLUSIVE -- the token works and the catalog answered, but it "
                "listed no tables. The OneLake catalog is the ICEBERG table API: it shows "
                "Iceberg tables and Delta tables carrying Iceberg metadata, so a workspace "
                "where no leg has ever run looks exactly like this. Run one "
                "first, then re-dispatch.")

    reads = by_n.get(4, "")
    if not reads.startswith("PASS"):
        return (f"VERDICT: CATALOG YES, STORAGE NO -- the catalog was attached and listed "
                f"tables, but the first read of a data file failed ({reads}). That splits the "
                f"bearer token's two jobs: it signed the REST calls and did not sign the blob "
                f"GETs. Worth reporting upstream -- chdb-core wraps the same string in a "
                f"StaticCredential for both.")

    created = by_n.get(7, "")
    # PROBE 11 IS THE INGEST SIGNAL, NOT PROBE 10. This keyed off 10 alone and announced
    # "INGEST: BLOCKED -- Files/ is unreachable" on a run where 11 and 12 had both read
    # 334k rows off the very same url (run 35433920589); 10 had died in the probe's own JSON
    # parsing. A headline that survives its own evidence is the bug, so the ragged read --
    # the thing a leg would actually do -- decides, and 10 only corroborates.
    csv_ok = by_n.get(11, "").startswith("PASS")
    reach_ok = by_n.get(10, "").startswith("PASS")
    skipped = by_n.get(11, "").startswith("SKIP")

    if skipped:
        ingest = (" INGEST: UNPROVEN -- the Files/ probes were skipped for want of landing "
                  "files, so whether chDB can read the AEMO CSVs at all is unmeasured.")
    elif csv_ok:
        # WHAT THE READ COSTS IS PROBE 18's ANSWER, NOT AN ASSUMPTION. This said "it takes ONE
        # url with no glob and no listing" for a whole run without ever having tried a glob --
        # and url() expands {a,b} client-side, which is the shape spark_read_csv.sql already
        # emits. Asserting an untested limit is the thing this file exists to not do.
        glob = by_n.get(18, "")
        if glob.startswith("PASS"):
            shape = ("and a brace glob of this run's files reads them in ONE statement, which "
                     "is the same shape spark_read_csv.sql emits -- so the leg's read ports "
                     "rather than being rebuilt")
        elif glob.startswith("PARTIAL") or glob.startswith("FAIL"):
            shape = ("but the brace glob did not expand, so it is one url per file and a leg "
                     "would carry its own enumeration -- the DFS list API this script already "
                     "uses -- where the other engines pass a pattern")
        else:
            shape = ("and whether a brace glob of this run's files reads in one statement is "
                     "UNMEASURED (probe 18 needs two landing files)")
        ingest = f" INGEST: url()+Authorization reads the real ragged AEMO CSV, {shape}."
        if not reach_ok:
            ingest += (" (Probe 10 did not pass, which given 11 and 12 did is about probe 10 "
                       "and not about Files/.)")
    elif reach_ok:
        ingest = (" INGEST: PARTIAL -- url()+Authorization reaches Files/ one file at a time, "
                  "but the ragged AEMO CSV did not parse. A leg would need that read working "
                  "before anything downstream matters.")
    else:
        ingest = (" INGEST: BLOCKED -- Files/ is unreachable. azureBlobStorage() has no "
                  "bearer-token argument and url()+Authorization did not work either, so "
                  "there is no path to the landing zone every model reads.")

    inserted = by_n.get(8, "")

    # A SILENT WRITE IS THE WORST OUTCOME, so it is checked before the happy path. A create
    # or an insert that returns without landing anything looks identical to one that worked,
    # from the statement alone -- which is why 7 re-lists and 8 reads back.
    if created.startswith("SILENT NO-OP") or inserted.startswith("SILENT NO-OP") \
            or inserted.startswith("MISMATCH"):
        return ("VERDICT: SUSPECT -- a write returned without error and the catalog does not "
                "show it. Do not build on this, and do not read it as 'OneLake is read-only' "
                "either: that conclusion was drawn once already from a probe that had left "
                "allow_insert_into_iceberg off and named a local storage engine. Check both "
                "before believing the engine." + ingest)

    if created.startswith("PASS") and inserted.startswith("PASS"):
        return ("VERDICT: READ AND WRITE -- chDB reaches OneLake on the Entra bearer token "
                "this repo already mints, lists the legs' tables, reads them, and CREATES AND "
                "POPULATES a table of its own through the catalog. That is the whole adapter "
                "contract a sixth engine needs on the write side, so what decides a chdb leg "
                "now is ingest and the model shapes below, not access." + ingest)

    if created.startswith("PASS"):
        return (f"VERDICT: CREATE YES, INSERT NO -- the table registered but the append did "
                f"not ({inserted or 'no result'}). Every fact model in this repo is an "
                f"incremental append, so a leg is blocked on exactly this." + ingest)

    return (f"VERDICT: READS WORK, WRITE DID NOT -- the catalog and the storage read are both "
            f"green, but the create failed ({created or 'no result'}). If the error names a "
            f"storage engine rather than the catalog, it is the statement and not chDB."
            + ingest)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # A smoke test reports; it does not gate. The workflow is continue-on-error too, but
        # exiting 0 keeps `| tee` and the artifact upload honest.
        print(f"\nchdb_smoke could not run: {type(e).__name__}: {redact(str(e))}",
              file=sys.stderr)
        traceback.print_exc()

    sys.stdout.flush()
    sys.stderr.flush()
    sys.exit(0)
