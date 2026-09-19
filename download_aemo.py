"""Land the AEMO source files, for every engine.

ONE downloader. The four source repos each carried a ~380-line copy of this as a dbt PYTHON
MODEL (`stg_csv_archive_log.py`), which does not work for half the engines: dbt-fabric's
python models are PySpark-via-Livy only, so the dwh repo kept a fifth copy outside
model-paths driven by a stub `dbt` object, and the spark leg has no usable python-model
runtime here either. Landing is not a modelling step — it is a prerequisite — so it is a
plain script, and each engine gets a thin `stg_csv_archive_log.sql` view over the parquet
log this writes.

ONE LANDING ZONE, ONE PATH, PLAIN CSV. The DuckDB-family repos used to gzip into `csv/`
while dwh landed plain into `csv_raw/`, because Fabric Warehouse OPENROWSET cannot read
gzip CSV at all (`DATA_COMPRESSION` is only valid under CSV PARSER 1.0, and 1.0 cannot
parse the ragged/quoted AEMO rows; the only working combination is plain CSV + PARSER 2.0).
Everything lands plain, in `csv_raw/`, once.

THIS SCRIPT WRITES TO `LANDING_PATH`, NEVER TO `FILES_PATH`, and the difference is the
whole point. `FILES_PATH` is per-engine — how that engine's dbt READS the landing zone,
which for the dwh leg is a shortcut rather than the zone itself. `LANDING_PATH` is the same
directory on all five legs. They were one variable once, and provision.py re-pointed it for
two engines, so those legs quietly downloaded their own private copy of the CSVs: the
parity comparison was then grading engines on different inputs, which makes it worthless.

Idempotent: `csv_raw_archive_log.parquet` is the watermark, so a re-run fetches only files
it has not already landed.

    export LANDING_PATH=./landing
    python download_aemo.py
    dbt build --target duckrun --profiles-dir .

Every correctness fix that existed in exactly one of the four repos is merged in here:
  * sql_retry() around the network listing queries     (was dwh only)
  * plain-CSV landing + daily_download_limit           (was dwh only)
  * parameterised INSERTs                              (was iceberg only)
  * GITHUB_TOKEN secret + try/except on the backfill   (was iceberg only)
"""

import io
import os
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import duckrun

# FILES_PATH is the fallback so the local recipe (`export FILES_PATH=./landing`) and a
# laptop run still work with one variable; in Fabric, provision.py always sets both.
LANDING_PATH = (
    os.environ.get("LANDING_PATH")
    or os.environ.get("FILES_PATH")
    or "/tmp/landing"
).rstrip("/")
DOWNLOAD_LIMIT = int(os.environ.get("download_limit", "2"))
# Daily files are backfilled from a GitHub mirror (raw download_url), not nemweb, so a high
# limit is safe there. Intraday scada/price hit nemweb directly, which throttles bursts with
# HTTP 403 — keep those on the smaller download_limit.
DAILY_DOWNLOAD_LIMIT = int(os.environ.get("daily_download_limit", str(DOWNLOAD_LIMIT)))

# duckrun is the transport for every engine, not just the duckrun target: it resolves OneLake
# auth and gives a plain DuckDB connection, and it works against a local directory too.
dr = duckrun.connect(LANDING_PATH, read_only=False)
con = dr.con
con.sql("INSTALL httpfs; LOAD httpfs; INSTALL json; LOAD json;")
try:
    con.sql(
        "SET GLOBAL azure_transport_option_type='"
        + os.environ.get("AZURE_TRANSPORT_OPTION_TYPE", "default")
        + "'"
    )
except Exception:
    pass


def push_new(local_folder, rel):
    dr.copy(local_folder, rel, overwrite=False)


def push_replace(local_folder, rel):
    """Overwrite whatever is already at `rel`. obstore delete-then-copy rather than a plain
    overwrite so a file that vanished upstream does not linger in the landing zone."""
    import obstore
    from dbt.adapters.duckrun import objectstore, secret

    base = f"{LANDING_PATH}/{rel}" if rel else LANDING_PATH
    store = objectstore.build_store(base, secret.refreshed(dr.storage_options))
    for n in os.listdir(local_folder):
        try:
            obstore.delete(store, n)
        except Exception:
            pass
    dr.copy(local_folder, rel, overwrite=True)


def download_aemo(session, files_path, download_limit, daily_download_limit):
    csv_log_path = files_path + "/csv_raw_archive_log.parquet"
    batch_size, max_workers = 7, 8

    # --- Load the existing log, or start an empty one -------------------------------
    log_exists = session.sql(f"SELECT count(*) FROM glob('{csv_log_path}')").fetchone()[0]
    if log_exists > 0:
        session.sql(
            f"""
            CREATE OR REPLACE TEMP TABLE _csv_archive_log AS
            SELECT source_type, source_filename, archive_path, archived_at,
                   row_count, source_url, etag, csv_filename
            FROM read_parquet('{csv_log_path}') WHERE csv_filename IS NOT NULL
            """
        )
    else:
        session.sql(
            """
            CREATE OR REPLACE TEMP TABLE _csv_archive_log (
                source_type VARCHAR, source_filename VARCHAR, archive_path VARCHAR,
                archived_at TIMESTAMPTZ, row_count BIGINT, source_url VARCHAR,
                etag VARCHAR, csv_filename VARCHAR)
            """
        )

    def download_and_extract(url, temp_dir):
        """Download a ZIP and extract its CSVs UNCOMPRESSED. Thread-safe."""
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (dbt-aemo)"})
        for attempt in range(3):
            try:
                zip_bytes = urllib.request.urlopen(req, timeout=60).read()
                break
            except urllib.error.HTTPError:
                if attempt < 2:
                    time.sleep(2**attempt)
                    continue
                raise
        z = zipfile.ZipFile(io.BytesIO(zip_bytes))
        out = []
        for name in z.namelist():
            if name.upper().endswith(".CSV"):
                safe = name.replace("/", "_")
                p = os.path.join(temp_dir, safe)
                # Uncompressed: Fabric OPENROWSET cannot read gzip CSV. See the module docstring.
                with open(p, "wb") as f:
                    f.write(z.read(name))
                out.append((name, safe, p))
        return out

    def save_log():
        with tempfile.TemporaryDirectory() as ltmp:
            lp = os.path.join(ltmp, "csv_raw_archive_log.parquet").replace("\\", "/")
            session.sql(f"COPY _csv_archive_log TO '{lp}' (FORMAT PARQUET)")
            push_replace(ltmp, "")

    def log_insert(source_type, source_filename, archive_path, now, url, csv_base):
        """Parameterised: odd characters in a filename or URL cannot break the SQL."""
        session.execute(
            "INSERT INTO _csv_archive_log VALUES "
            "(?, ?, ?, CAST(? AS TIMESTAMPTZ), NULL, ?, NULL, ?)",
            [source_type, source_filename, archive_path, now, url, csv_base],
        )

    def process(rows, source_type, subfolder):
        files = [(r[0], r[1]) for r in rows]
        for i in range(0, len(files), batch_size):
            batch = files[i : i + batch_size]
            with tempfile.TemporaryDirectory() as tmp:
                extracted = []
                with ThreadPoolExecutor(max_workers=max_workers) as ex:
                    futs = {ex.submit(download_and_extract, u, tmp): (u, fn) for u, fn in batch}
                    for fut in as_completed(futs):
                        u, fn = futs[fut]
                        try:
                            for csv_name, safe, path in fut.result():
                                extracted.append((fn, safe, path, u))
                        except Exception as e:
                            print(f"  WARN skip {fn}: {e}")
                if extracted:
                    push_new(tmp, f"csv_raw/{subfolder}")
                now = datetime.now(timezone.utc).isoformat()
                for fn, csv_name, path, u in extracted:
                    base = csv_name.removesuffix(".CSV").removesuffix(".csv")
                    log_insert(source_type, fn, f"/{subfolder}/{csv_name}", now, u, base)
            save_log()

    def sql_retry(q, attempts=4, base=5):
        """For the network listing queries. nemweb throws a transient 403/5xx when it
        rate-limits a burst; back off (5s, 10s, 20s) rather than failing the whole run."""
        for a in range(attempts):
            try:
                return session.sql(q)
            except Exception as e:
                if a < attempts - 1:
                    w = base * (2**a)
                    print(f"  WARN net {type(e).__name__}: {e}; retry {a + 1}/{attempts - 1} in {w}s")
                    time.sleep(w)
                else:
                    raise

    def new_files(table, source_type):
        # ORDER BY filename DESC -- NEWEST FIRST, so a limit smaller than the backlog lands the
        # recent end of the archive and the gap fills backwards over runs. The intraday candidate
        # tables below are already built newest-first (ORDER BY full_url DESC LIMIT 500); the
        # daily one is not ordered at all -- it is the nemweb listing with the GitHub-mirror
        # backfill (years ascending) appended -- so without this the daily backlog landed in
        # whatever order DuckDB happened to return. Same direction the fact models fold in.
        limit = daily_download_limit if source_type == "daily" else download_limit
        return session.sql(
            f"""SELECT full_url, filename FROM {table}
                WHERE '{source_type}::' || filename NOT IN (
                    SELECT source_type || '::' || source_filename FROM _csv_archive_log)
                ORDER BY filename DESC
                LIMIT {limit}"""
        ).fetchall()

    # --- DAILY (the price and scada records share one file) --------------------------
    sql_retry(
        """CREATE OR REPLACE TEMP TABLE daily_files_web AS
        WITH h AS (SELECT content AS html FROM read_text('https://nemweb.com.au/Reports/Current/Daily_Reports/')),
             l AS (SELECT unnest(string_split(html, '<br>')) AS line FROM h)
        SELECT 'https://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url,
               split_part(regexp_extract(line, 'HREF="[^"]+/([^"]+\\.zip)"', 1), '.', 1) AS filename
        FROM l WHERE line LIKE '%PUBLIC_DAILY%.zip%'"""
    )
    aemo_new = session.sql(
        """SELECT count(*) FROM daily_files_web
           WHERE 'daily::' || filename NOT IN (
               SELECT source_type || '::' || source_filename FROM _csv_archive_log)"""
    ).fetchone()[0]

    if aemo_new < daily_download_limit:
        # nemweb only keeps the current window, so history comes from a GitHub mirror.
        # An authenticated GitHub API call gets 5000 req/h against 60 anonymous, and CI
        # runners share IPs — without a token the listing calls hit the anonymous quota.
        github_token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
        if github_token:
            # INLINED, not a bind parameter: DuckDB's CREATE SECRET does not accept
            # prepared-statement parameters and dies with "Unrecognized expression type
            # PARAMETER". It only bites when this backfill branch runs, so an earlier leg
            # that had enough new nemweb files sails past it -- which is exactly how it
            # reached CI green on four engines and killed the fifth.
            session.execute(
                "CREATE OR REPLACE SECRET github_api (TYPE HTTP, BEARER_TOKEN '"
                + github_token.replace("'", "''")
                + "', SCOPE 'https://api.github.com')"
            )
        years = range(2018, datetime.now(timezone.utc).year + 1)
        reads = " UNION ALL ".join(
            "SELECT content AS j FROM read_text("
            f"'https://api.github.com/repos/djouallah/aemo_data/contents/data/archive/{y}')"
            for y in years
        )
        # Opportunistic: if the listing API is rate-limited or down, carry on with whatever
        # nemweb is currently serving rather than failing the run.
        try:
            sql_retry(
                f"""INSERT INTO daily_files_web
                WITH api AS ({reads}),
                     p AS (SELECT unnest(from_json(j, '["json"]')) AS fi FROM api)
                SELECT json_extract_string(fi, '$.download_url') AS full_url,
                       split_part(json_extract_string(fi, '$.name'), '.', 1) AS filename
                FROM p WHERE json_extract_string(fi, '$.name') LIKE 'PUBLIC_DAILY%.zip'
                  AND split_part(json_extract_string(fi, '$.name'), '.', 1)
                      NOT IN (SELECT filename FROM daily_files_web)"""
            )
        except Exception as e:
            print(f"  WARN: GitHub backfill listing unavailable, continuing with nemweb only: {e}")

    daily = new_files("daily_files_web", "daily")
    if daily:
        process(daily, "daily", "daily")

    # --- INTRADAY SCADA --------------------------------------------------------------
    sql_retry(
        """CREATE OR REPLACE TEMP TABLE intraday_scada_web AS
        WITH h AS (SELECT content AS html FROM read_text('http://nemweb.com.au/Reports/Current/Dispatch_SCADA/')),
             l AS (SELECT unnest(string_split(html, '<br>')) AS line FROM h)
        SELECT 'http://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url,
               split_part(regexp_extract(line, 'HREF="[^"]+/([^"]+\\.zip)"', 1), '.', 1) AS filename
        FROM l WHERE line LIKE '%PUBLIC_DISPATCHSCADA%' ORDER BY full_url DESC LIMIT 500"""
    )
    scada = new_files("intraday_scada_web", "scada_today")
    if scada:
        process(scada, "scada_today", "scada_today")

    # --- INTRADAY PRICE --------------------------------------------------------------
    sql_retry(
        """CREATE OR REPLACE TEMP TABLE intraday_price_web AS
        WITH h AS (SELECT content AS html FROM read_text('http://nemweb.com.au/Reports/Current/DispatchIS_Reports/')),
             l AS (SELECT unnest(string_split(html, '<br>')) AS line FROM h)
        SELECT 'http://nemweb.com.au' || regexp_extract(line, 'HREF="([^"]+)"', 1) AS full_url,
               split_part(regexp_extract(line, 'HREF="[^"]+/([^"]+\\.zip)"', 1), '.', 1) AS filename
        FROM l WHERE line LIKE '%PUBLIC_DISPATCHIS_%.zip%' ORDER BY full_url DESC LIMIT 500"""
    )
    price = new_files("intraday_price_web", "price_today")
    if price:
        process(price, "price_today", "price_today")

    # --- DUID REFERENCE DATA (refreshed at most once a day) --------------------------
    duid_sources = [
        ("duid_data", "duid_data",
         "https://raw.githubusercontent.com/djouallah/aemo_data/refs/heads/main/duid_data.csv",
         "duid_data.csv"),
        ("duid_facilities", "facilities",
         "https://data.wa.aemo.com.au/datafiles/post-facilities/facilities.csv",
         "facilities.csv"),
        ("duid_wa_energy", "WA_ENERGY",
         "https://raw.githubusercontent.com/djouallah/aemo_data/refs/heads/main/WA_ENERGY.csv",
         "WA_ENERGY.csv"),
        ("duid_geo_data", "geo_data",
         "https://raw.githubusercontent.com/djouallah/aemo_data/refs/heads/main/geo_data.csv",
         "geo_data.csv"),
    ]
    last = session.sql(
        "SELECT max(archived_at) FROM _csv_archive_log WHERE source_type LIKE 'duid_%'"
    ).fetchone()[0]
    fresh = last is not None and (datetime.now(last.tzinfo) - last).total_seconds() < 86400
    if fresh:
        print(f"  DUID data fresh ({last}), skipping")
    else:
        with tempfile.TemporaryDirectory() as dtmp:
            for st, sf, url, fn in duid_sources:
                hdr = ", header=true" if sf == "WA_ENERGY" else ""
                lp = os.path.join(dtmp, fn).replace("\\", "/")
                session.sql(
                    f"""COPY (SELECT * FROM read_csv_auto('{url}',
                              null_padding=true, ignore_errors=true{hdr}))
                        TO '{lp}' (FORMAT CSV, HEADER)"""
                )
            push_replace(dtmp, "csv_raw/duid")
        session.sql("DELETE FROM _csv_archive_log WHERE source_type LIKE 'duid_%'")
        now = datetime.now(timezone.utc).isoformat()
        for st, sf, url, fn in duid_sources:
            log_insert(st, sf, f"/duid/{fn}", now, url, fn.rsplit(".", 1)[0])

    save_log()
    summary = session.sql(
        """SELECT source_type, count(*) AS files FROM _csv_archive_log
           GROUP BY source_type ORDER BY source_type"""
    )
    return summary, {"daily": len(daily), "scada_today": len(scada), "price_today": len(price)}


if __name__ == "__main__":
    print(f"Landing to: {LANDING_PATH}")
    summary, landed = download_aemo(con, LANDING_PATH, DOWNLOAD_LIMIT, DAILY_DOWNLOAD_LIMIT)
    summary.show()
    print("Landed this run: " + ", ".join(f"{k}={v}" for k, v in landed.items()))
    # The engine trees live in two dbt projects, so the build is run from inside one of
    # them: dbt1/ is dbt-core 1.x (duckrun, ducklake, dwh, spark) and dbt2/ is dbt OSS 2
    # (iceberg). See dbt2/dbt_project.yml for why they cannot share a root.
    print("Done. Now run one of:")
    print("  cd dbt1 && dbt build --target <duckrun|ducklake|dwh|spark> --profiles-dir .")
    print("  cd dbt2 && dbt build --target iceberg --profiles-dir .")
