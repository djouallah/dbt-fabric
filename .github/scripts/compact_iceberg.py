"""Compact the OneLake Iceberg catalog's data files (post-load maintenance).

Every model in this project is an insert-only incremental merge — that is deliberate
(OneLake accepts one add-snapshot per commit), but it means each dbt run appends another
small data file per table and nothing ever folds them back together. This runs
iceberg_rewrite_data_files() over each table, consolidating files below the target size.

iceberg_rewrite_data_files landed in duckdb/iceberg#1035 and is not in a stable
duckdb release yet, so requirements/iceberg.txt installs the latest PRE-release duckdb
(`--pre duckdb`, unpinned) rather than a hand-bumped build. The iceberg extension binary is
keyed to the duckdb build, so whatever pip resolves brings its own matching extension.
has_rewrite_function() checks for the function rather than assuming it, so a resolution
that happens to lack it degrades to "nothing to compact" instead of failing the job.

Ported from djouallah/analytics-as-code scripts/compact_iceberg.py, which runs this against
an R2-backed catalog. Two things differ here:
  - Credentials. That catalog vends storage credentials (CREATE SECRET TYPE ICEBERG).
    OneLake is attached with access_delegation_mode 'none', so the client brings its own
    Azure token — the same azure secret + attach options dbt/profiles.yml uses.
  - No S3 uploader tuning. The reference sets s3_uploader_max_parts_per_file for R2's
    "non-trailing parts must be equal length" rule; OneLake writes go through the azure
    extension over abfss:// and never touch the S3 uploader.

At most one iceberg_metadata() call per table, in prime(), and only as a FALLBACK: the rewrite
is tried cold first (see compact()). Do not add more — it enumerates every manifest, which on
a fragmented table is the whole problem we're here to fix.

Known limitations of the upstream function:
  - manifest-level column statistics are not populated for rewritten files
  - V3 tables and partition spec evolution are unsupported
  - there is no snapshot expiry, so the pre-compaction data files stay in OneLake until
    something expires them. Reads get faster immediately; storage does not shrink.

Never fails the pipeline: every table is best-effort and errors are printed, not raised, so
a bad run just means the tables stay fragmented until the next one.

Usage:
    python .github/scripts/compact_iceberg.py
"""

import os
import sys
import time

import duckdb

ENDPOINT = os.environ["ONELAKE_ENDPOINT"]
TOKEN = os.environ["ONELAKE_TOKEN"]
WAREHOUSE = os.environ["WAREHOUSE_PATH"]      # "{workspace_id}/{lakehouse_id}"

# Files smaller than this get folded together; the rest are left alone.
TARGET_FILE_SIZE = "64MiB"
# Don't bother rewriting a table that only has a handful of files. Also what keeps
# already-tidy tables (dim_calendar, anything compacted last run) cheap.
#
# Overridable because a dbt run adds exactly one data file per table, so in steady state the
# tables sit well under this and compaction is a no-op — correct, but it means the rewrite
# path goes untested for weeks. Dropping this to 2 on a manual run forces a real rewrite and
# proves the catalog still accepts the commit. Leave the default alone.
MIN_INPUT_FILES = int(os.environ.get("COMPACT_MIN_INPUT_FILES", "5"))
# Stop starting new tables past this much wall clock, so the job reports what it did instead
# of being killed by the runner's timeout mid-rewrite. Keep it well under the workflow's
# timeout-minutes. Whatever gets skipped is picked up next run — compaction is incremental
# by nature.
BUDGET_MINUTES = float(os.environ.get("COMPACT_BUDGET_MINUTES", "70"))

# DuckDB's azure extension reads/writes OneLake over a curl transport (its default transport
# fails the OneLake TLS handshake). dbt sets this via on-run-start in dbt_project.yml; this
# script is not a dbt run, so it sets it itself. Same default, same env var.
AZURE_TRANSPORT = os.environ.get("AZURE_TRANSPORT_OPTION_TYPE", "default")

# The schemas are resolved the SAME WAY macros/generate_schema_name.sql resolves them, and
# must stay in step with it: this script is not a dbt run, so nothing else would catch a
# drift. All five engines share one lakehouse, so the iceberg tables live under
# `iceberg_landing` / `iceberg_mart`; compacting a bare `landing`/`mart` would either find
# nothing (and report a tidy catalog, wrongly) or reach into another engine's schema.
_DBT_SCHEMA = os.environ.get("DBT_SCHEMA", "mart")
_PREFIX = "iceberg" if _DBT_SCHEMA == "mart" else f"{_DBT_SCHEMA}_iceberg"
LANDING, MART = f"{_PREFIX}_landing", f"{_PREFIX}_mart"

# Hand-ordered, not discovered — we know the models, and listing them costs a metadata scan
# per table for nothing. A new model just gets added here.
#
# Ordered by expected fragmentation and by what we'd least regret dropping if the budget runs
# out: the dashboard-facing tables first (they take an append every run and are what Power BI
# reads), then the staging log, then the small dimensions, then the big historical facts last
# — those are the slowest to rewrite and the least sensitive to small-file overhead.
#
# This is the canonical EIGHT. `fct_summary_daily` used to be listed here and is NOT a model
# in this repo — it existed only in the iceberg source repo, which is exactly the one-engine
# drift the merge exists to stop. Listing it only bought a "no version-hint" read failure
# every run.
TABLES = [
    f"{LANDING}.fct_price_today",
    f"{LANDING}.fct_scada_today",
    f"{MART}.fct_summary",
    f"{LANDING}.stg_csv_archive_log",
    f"{MART}.dim_calendar",
    f"{MART}.dim_duid",
    f"{LANDING}.fct_price",
    f"{LANDING}.fct_scada",
]


def connect():
    con = duckdb.connect(":memory:")
    # Plain install first. The 1.6.0/2.0.0 dev line self-identifies as v2.0.0-alpha*, and
    # nightly-extensions.duckdb.org has no iceberg build under that version — asking
    # core_nightly first just buys a 404 and a scary log line. The core extension for these
    # builds does carry iceberg_rewrite_data_files.
    try:
        con.install_extension("iceberg")
    except Exception as e:
        print(f"  (core install failed, trying core_nightly: {e})", flush=True)
        con.execute("FORCE INSTALL iceberg FROM core_nightly")
    con.load_extension("iceberg")

    con.execute(f"SET GLOBAL azure_transport_option_type = '{AZURE_TRANSPORT}'")
    con.execute("SET GLOBAL temp_directory = '/tmp/duckdb_spill'")

    # Mirrors dbt/profiles.yml. OneLake is attached with access_delegation_mode 'none' — the
    # catalog does not vend storage credentials, so the azure secret below is what authorises
    # the actual data-file reads and writes.
    con.execute(
        f"CREATE SECRET onelake_storage "
        f"(TYPE azure, PROVIDER access_token, ACCESS_TOKEN '{TOKEN}')"
    )
    con.execute(
        f"ATTACH '{WAREHOUSE}' AS onelake "
        f"(TYPE iceberg, ENDPOINT '{ENDPOINT}', TOKEN '{TOKEN}', "
        f"ACCESS_DELEGATION_MODE 'none')"
    )
    return con


def oneline(e):
    """Collapse a duckdb error to its first line — they carry a SQL echo and a caret ruler."""
    return " ".join(str(e).split("\n")[0].split())


def catalog_tables(con):
    """What the catalog actually holds, as {"schema.table"}.

    One cheap REST list call, not a metadata scan. Worth it: the first run reported
    mart.fct_summary_daily as a "Failed to read iceberg table / no version-hint" error, which
    is what you get when the name doesn't resolve as a catalog table and falls back to being
    read as a path. Listing up front says plainly whether a table is missing or misnamed
    instead of dressing it up as a read failure.
    """
    try:
        rows = con.execute(
            "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'onelake'"
        ).fetchall()
        return {f"{s}.{t}" for s, t in rows}
    except Exception as e:
        print(f"  (could not list catalog tables: {oneline(e)})", flush=True)
        return None


def has_rewrite_function(con):
    return (
        con.execute(
            "SELECT count(*) FROM duckdb_functions() "
            "WHERE function_name = 'iceberg_rewrite_data_files'"
        ).fetchone()[0]
        > 0
    )


def prime(con, fq):
    """Make the table's storage credentials available to the rewrite, and count its files.

    iceberg_rewrite_data_files doesn't fetch credentials itself — called cold it can die with
    403 "No credentials are provided" (duckdb/iceberg#1349). The reference
    implementation tried the cheaper options (LIMIT 0, LIMIT 1) against a real catalog and
    both still 403'd: the 403 is on the manifest avro, and iceberg_metadata() is what reads
    those.

    It is expensive — it enumerates every manifest — so it is the one and only metadata call
    here. Don't add more, and don't "optimise" this one away. Since we're paying for it, keep
    the row count: it is the only independent read on how fragmented the table actually is,
    and without it a rewrite that does nothing is indistinguishable from a tidy table.
    """
    return con.execute(f"SELECT count(*) FROM iceberg_metadata('{fq}')").fetchone()[0]


def looks_like_credentials(e):
    """The duckdb/iceberg#1349 signature: an auth failure on the data-file store."""
    s = str(e)
    return "403" in s or "credential" in s.lower() or "Authentication" in s


def rewrite(con, fq):
    return con.execute(
        f"SELECT rewritten_data_files, added_data_files, rewritten_bytes "
        f"FROM iceberg_rewrite_data_files('{fq}', "
        f"target_file_size_bytes => '{TARGET_FILE_SIZE}', "
        f"min_input_files => {MIN_INPUT_FILES})"
    ).fetchone()


def compact(con, table, say):
    """Compact one table. Returns (table, status) for the report.

    COLD FIRST. The rewrite is called with no prior read of the table; prime() runs only if
    that call fails with a credentials-shaped error, and the report says which path was
    taken. That is the measurement: duckdb/iceberg#1349 is open upstream (PR #1362 unmerged)
    but the maintainer could not reproduce it after the 2026-09-16 credential-handling
    commits, and on OneLake the catalog never vended credentials in the first place
    (ACCESS_DELEGATION_MODE 'none', our own azure secret) -- so whether the priming scan is
    still needed HERE is only knowable from a cold call. "(cold)" in every line means the
    workaround can go; "(primed after ...)" means it is still load-bearing.
    """
    fq = f"onelake.{table}"
    files = None
    how = "cold"

    say("rewriting (cold)")
    try:
        row = rewrite(con, fq)
    except Exception as cold_err:
        if not looks_like_credentials(cold_err):
            return (table, f"ERROR: {type(cold_err).__name__}: {oneline(cold_err)}")
        say(f"cold rewrite failed on credentials ({oneline(cold_err)}) -- priming and retrying")
        try:
            files = prime(con, fq)
        except Exception as e:
            # Keep it to one line — the full multi-line duckdb error is already on stdout above.
            return (table, f"ERROR priming: {type(e).__name__}: {oneline(e)}")
        say(f"{files} data files")
        how = f"primed after: {oneline(cold_err)[:80]}"
        try:
            row = rewrite(con, fq)
        except Exception as e:
            return (table, f"ERROR after priming: {type(e).__name__}: {oneline(e)}")

    # Report what the function actually returned. "No row at all" and "a row of zeros" are
    # different failure modes and both look like a tidy table if you collapse them into one
    # "skipped" — which is exactly how the first run hid that nothing was happening.
    # The data-file count only exists when prime() ran: it is iceberg_metadata()'s row count,
    # and the cold path is precisely the one that does not pay for that scan.
    count = f", {files} data files" if files is not None else ""
    if row is None:
        return (table, f"NO ROW returned ({how}{count})")

    rewritten, added, rewritten_bytes = row
    if not rewritten:
        # Two different innocent reasons, worth telling apart when the count is known. Under
        # the file threshold means we declined to look; at or over it means we looked and
        # every file was already at or above the target size, so folding them would buy
        # nothing. Neither is a problem — observed on fct_scada, whose files are individually
        # larger than the target.
        if files is None:
            note = f"under the {MIN_INPUT_FILES}-file threshold or nothing below {TARGET_FILE_SIZE}"
        elif files < MIN_INPUT_FILES:
            note = f"under the {MIN_INPUT_FILES}-file threshold"
        else:
            note = f"nothing below the {TARGET_FILE_SIZE} target"
        return (table, f"0 rewritten ({how}{count}) — {note}")

    mb = (rewritten_bytes or 0) / 1048576.0
    return (table, f"OK ({rewritten} -> {added} files, {mb:.1f} MB, {how}{count})")


def report(lines, duckdb_version):
    out = ["=" * 100, f"Iceberg compaction (duckdb {duckdb_version})", "-" * 100]
    for table, status in lines:
        out.append(f"{table:<32}{status}")
    out.append("=" * 100)
    print("\n".join(out))

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(f"## 🧹 Iceberg compaction (duckdb {duckdb_version})\n\n")
            f.write("| table | result |\n|---|---|\n")
            for table, status in lines:
                f.write(f"| `{table}` | {status} |\n")
            f.write("\n")


def main():
    version = duckdb.__version__
    print(f"duckdb {version} — compacting {len(TABLES)} table(s) at {TARGET_FILE_SIZE}, "
          f"min_input_files={MIN_INPUT_FILES}, budget={BUDGET_MINUTES:g}min, in order:")
    for t in TABLES:
        print(f"  - onelake.{t}")
    print(flush=True)

    con = connect()
    if not has_rewrite_function(con):
        print(
            f"iceberg_rewrite_data_files() not available in duckdb {version} — the pinned "
            "build or its iceberg extension has moved. Nothing compacted."
        )
        return

    present = catalog_tables(con)
    if present is not None:
        print(f"catalog holds {len(present)} table(s): {', '.join(sorted(present))}")
        missing = [t for t in TABLES if t not in present]
        if missing:
            print(f"::warning::not in the catalog, will be skipped: {', '.join(missing)}")
        print(flush=True)

    started = time.monotonic()
    total = len(TABLES)
    lines = []
    for i, table in enumerate(TABLES, 1):
        if present is not None and table not in present:
            lines.append((table, "NOT IN CATALOG — never built, or renamed"))
            continue

        elapsed = (time.monotonic() - started) / 60.0
        if elapsed >= BUDGET_MINUTES:
            # Never drop tables silently — say which ones and why.
            for skipped in TABLES[i - 1:]:
                lines.append((skipped,
                              f"not attempted ({BUDGET_MINUTES:g}min budget spent)"))
            print(f"time budget spent after {elapsed:.1f}min — not attempting: "
                  f"{', '.join(TABLES[i - 1:])}", flush=True)
            break

        prefix = f"[{i}/{total}] onelake.{table}"

        def say(phase, _prefix=prefix):
            # Which step it's on, so a slow table can't be mistaken for a hang.
            print(f"{_prefix} ... {phase}", flush=True)

        print(f"{prefix} ... ({elapsed:.1f}min elapsed)", flush=True)
        _, status = compact(con, table, say)
        took = (time.monotonic() - started) / 60.0 - elapsed
        print(f"{prefix}: {status}  [{took:.1f}min]\n", flush=True)
        lines.append((table, status))

    report(lines, version)
    con.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Maintenance must never fail the pipeline.
        print(f"compact_iceberg failed (non-fatal): {e}", file=sys.stderr)
