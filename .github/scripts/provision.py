#!/usr/bin/env python3
"""Create the Fabric items one engine needs, and print the env vars its profile reads.

    python .github/scripts/provision.py <engine> >> "$GITHUB_ENV"

Idempotent: every item is create-if-missing, keep-if-present. Diagnostics go to stderr so
stdout is nothing but KEY=value lines.

FOUR ITEMS, ALL INSIDE ONE WORKSPACE FOLDER. The engines used to get one data item each,
which put up to fourteen items at the root of a workspace that already has ~175 — and every
lakehouse drags an auto-created SQLEndpoint shadow item along with it. They share one
lakehouse now and are separated by SCHEMA instead (see macros/generate_schema_name.sql):

    <FOLDER>/
      dbt_landing        Lakehouse  the CSVs download_aemo.py lands. Written ONCE, read by all five.
      dbt                Lakehouse  duckrun_* iceberg_* ducklake_* spark_* schemas (Tables/), and
                                    the duckrun_remote/ round-trip files of the Fabric-side runs
      dbt_dwh            Warehouse  dwh_landing, dwh_mart
      dbt_ducklake_meta  SQL DB     DuckLake catalog metadata only, no data

The Warehouse and the SQL DB cannot collapse into the lakehouse: a Warehouse cannot hold
Delta tables another engine wrote, and DuckLake's catalog must be a SQL database. Those two
are an engine-forced floor, not a layout choice.

THE LANDING PATH AND THE READ PATH ARE DIFFERENT VARIABLES, deliberately:

    LANDING_PATH  where download_aemo.py writes. IDENTICAL on all five legs, always
                  dbt_landing/Files. This is the whole basis of parity.py — if the engines
                  read different bytes, comparing their output means nothing.
    FILES_PATH    how THIS engine's dbt reads that zone. Same as LANDING_PATH for four of
                  them; the dwh leg reads through a shortcut because a Warehouse has no
                  Files section of its own.

They used to be one variable, and provision.py re-emitted it for ducklake and dwh — so
those two legs' downloaders landed their own private copies of the AEMO CSVs and the parity
comparison was quietly comparing different inputs.

EVERY ITEM THIS TOUCHES IS WRITTEN DOWN UNDER ITS GUID into the run-record fragment named by
`RUN_RECORD` (see record.py) -- plus, for dwh and spark, WHICH item the leg's compute bills
against (`legs.<engine>.compute`): the Warehouse for dwh, the shared lakehouse for spark's
Livy session. That is what measure_cu.py joins the Capacity Metrics model on. `RUN_RECORD`
unset is a no-op, so running this by hand (or from the demo notebook) still works.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time

import requests

import record

API = "https://api.fabric.microsoft.com/v1"
WS = os.environ["FABRIC_WORKSPACE_ID"]
FOLDER = os.environ.get("FOLDER", "dbt")

LANDING_LAKEHOUSE = "dbt_landing"
# The one lakehouse every Delta/Iceberg-writing engine shares. Schemas keep them apart.
DATA_LAKEHOUSE = "dbt"
DWH_WAREHOUSE = "dbt_dwh"
DUCKLAKE_SQL_DB = "dbt_ducklake_meta"

LAKEHOUSE_ENGINES = {"duckrun", "iceberg", "ducklake", "spark"}
ENGINES = LAKEHOUSE_ENGINES | {"dwh"}


def log(msg: str) -> None:
    print(msg, file=sys.stderr)


def emit(key: str, value: str) -> None:
    print(f"{key}={value}")


FABRIC_RESOURCE = "https://api.fabric.microsoft.com"


def token(resource: str = FABRIC_RESOURCE) -> str:
    """A bearer token for `resource`. duckrun mints the Fabric control-plane one from the GitHub
    OIDC assertion with no login step; any OTHER audience (the SQL DB token for ducklake) goes
    through `az`, because duckrun's public helpers only mint storage and Fabric tokens -- this
    used to hand a Fabric-API token to every caller regardless of the resource asked for."""
    if resource == FABRIC_RESOURCE:
        try:
            import duckrun.auth

            return duckrun.auth.get_fabric_token()
        except Exception:
            pass
    # Inside a Fabric notebook (the demo notebook runs this script there) notebookutils mints
    # any audience, and `az` does not exist.
    try:
        import notebookutils  # type: ignore

        return notebookutils.credentials.getToken(resource)
    except ImportError:
        pass
    return subprocess.run(
        ["az", "account", "get-access-token", "--resource", resource,
         "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()


def req(method: str, path: str, **kw):
    hdr = {"Authorization": f"Bearer {token()}", "Content-Type": "application/json"}
    for attempt in range(5):
        r = requests.request(method, f"{API}/{path}", headers=hdr, timeout=120, **kw)
        if r.status_code in (429, 500, 502, 503, 504) and attempt < 4:
            time.sleep(5 * 2**attempt)
            continue
        return r
    return r


def ensure_folder(name: str) -> str | None:
    """Create-if-missing, top level. Returns None if folders are unavailable."""
    r = req("GET", f"workspaces/{WS}/folders")
    if r.status_code == 200:
        for f in r.json().get("value", []):
            # Top-level only: a nested folder of the same name is a different folder.
            if f.get("displayName") == name and not f.get("parentFolderId"):
                log(f"  = folder {name} exists ({f['id']})")
                return f["id"]
    r = req("POST", f"workspaces/{WS}/folders", json={"displayName": name})
    if r.status_code in (200, 201):
        fid = r.json().get("id")
        log(f"  + created folder {name} ({fid})")
        return fid
    # Not fatal: items at the root still work, they are just untidy.
    log(f"  ! could not create folder {name}: {r.status_code} {r.text[:200]}")
    return None


def find(kind: str, name: str) -> str | None:
    r = req("GET", f"workspaces/{WS}/{kind}")
    if r.status_code != 200:
        return None
    for it in r.json().get("value", []):
        if it.get("displayName") == name:
            return it["id"]
    return None


def move_to_folder(item_id: str, folder_id: str) -> None:
    """Belt and braces: folderId on create is not honoured by every item type."""
    r = req("GET", f"workspaces/{WS}/items/{item_id}")
    if r.status_code == 200 and r.json().get("folderId") == folder_id:
        return
    r = req("POST", f"workspaces/{WS}/items/{item_id}/move",
            json={"targetFolderId": folder_id})
    if r.status_code not in (200, 201, 202):
        log(f"  ! could not move {item_id} into folder: {r.status_code} {r.text[:200]}")


def ensure(kind: str, name: str, payload: dict | None = None,
           folder_id: str | None = None) -> str:
    """Create if missing, keep if present, and land it in the folder either way."""
    existing = find(kind, name)
    if existing:
        log(f"  = {kind[:-1]} {name} exists ({existing})")
        if folder_id:
            move_to_folder(existing, folder_id)
        return existing
    body = {"displayName": name}
    if payload:
        body.update(payload)
    if folder_id:
        body["folderId"] = folder_id
    r = req("POST", f"workspaces/{WS}/{kind}", json=body)
    if r.status_code in (200, 201, 202):
        # Fabric answers 409 ItemDisplayNameNotAvailableYet for a while after a delete, and
        # the create itself can be async, so poll for the item rather than trusting the body.
        for _ in range(40):
            got = find(kind, name)
            if got:
                log(f"  + created {kind[:-1]} {name} ({got})")
                if folder_id:
                    move_to_folder(got, folder_id)
                return got
            time.sleep(15)
    raise SystemExit(f"could not create {kind[:-1]} {name}: {r.status_code} {r.text[:300]}")


def abfss(item_id: str, section: str) -> str:
    return f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{item_id}/{section}"


def ensure_landing_shortcut(host_lakehouse_id: str, landing_id: str) -> None:
    """Shortcut Files/landing in the shared lakehouse -> the landing lakehouse.

    This exists for the dwh leg: a Fabric WAREHOUSE has no Files section and cannot host a
    shortcut, so its OPENROWSET reads the landing zone through one hosted elsewhere. It used
    to live in a lakehouse created for nothing else (`dbt_dwh_src`); the shared data
    lakehouse hosts it now, which is one item fewer for the same mechanism.

    OneLake accounts a transaction against the REQUESTED path, so reading through a
    shortcut books the cost to the item hosting the shortcut.
    """
    r = req("GET", f"workspaces/{WS}/items/{host_lakehouse_id}/shortcuts")
    if r.status_code == 200:
        for s in r.json().get("value", []):
            if s.get("name") == "landing":
                return
    req("POST", f"workspaces/{WS}/items/{host_lakehouse_id}/shortcuts", json={
        "path": "Files",
        "name": "landing",
        "target": {"oneLake": {"workspaceId": WS, "itemId": landing_id, "path": "Files"}},
    })


def warehouse_connection(wh_id: str) -> str:
    """The SQL analytics endpoint lands last; poll for it."""
    for _ in range(60):
        r = req("GET", f"workspaces/{WS}/warehouses/{wh_id}")
        if r.status_code == 200:
            cs = r.json().get("properties", {}).get("connectionString")
            if cs:
                return cs
        time.sleep(5)
    raise SystemExit("warehouse connection string never appeared")


def workspace_name() -> str:
    r = req("GET", f"workspaces/{WS}")
    return r.json().get("displayName", "") if r.status_code == 200 else ""


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in ENGINES:
        print(f"usage: provision.py [{' | '.join(sorted(ENGINES))}]", file=sys.stderr)
        return 2
    engine = sys.argv[1]

    log(f"provisioning {engine} in workspace {WS}")
    folder_id = ensure_folder(FOLDER)
    record.item(folder_id, "folder", "Folder", FOLDER)

    landing_id = ensure("lakehouses", LANDING_LAKEHOUSE,
                        {"creationPayload": {"enableSchemas": True}}, folder_id)
    record.item(landing_id, "landing", "Lakehouse", LANDING_LAKEHOUSE)
    landing_path = abfss(landing_id, "Files")

    # Every engine's download_aemo.py writes HERE, and only here.
    emit("LANDING_PATH", landing_path)

    # The shared data lakehouse is provisioned on every leg, not just the four that write
    # to it: the dwh leg needs it to host the landing shortcut.
    data_id = ensure("lakehouses", DATA_LAKEHOUSE,
                     {"creationPayload": {"enableSchemas": True}}, folder_id)
    record.item(data_id, "data", "Lakehouse", DATA_LAKEHOUSE)
    # remote_dbt.py needs the item (not a path) for run_python's result round-trip: the
    # workspace holds many lakehouses, so duckrun cannot infer one.
    emit("DATA_LAKEHOUSE_ID", data_id)

    if engine in LAKEHOUSE_ENGINES:
        # Reads the landing zone directly — no shortcut, no second copy.
        emit("FILES_PATH", landing_path)

    if engine == "duckrun":
        emit("ONELAKE_TABLES_PATH", abfss(data_id, "Tables"))

    elif engine == "iceberg":
        # The Iceberg REST catalog addresses the item as <workspace>/<item>, not abfss.
        emit("WAREHOUSE_PATH", f"{WS}/{data_id}")
        emit("ONELAKE_ENDPOINT", "https://onelake.table.fabric.microsoft.com/iceberg")

    elif engine == "ducklake":
        # The catalog metadata lives in a Fabric SQL DB. Created through the REST API, not
        # `fab create`, because only the API accepts a collation — and DuckLake needs a
        # UTF-8 one, which CANNOT be changed after creation.
        db_id = find("sqlDatabases", DUCKLAKE_SQL_DB)
        if not db_id:
            # `creationMode: New` IS REQUIRED. SQLDatabaseCreationPayload is a polymorphic
            # type (New | Restore | RestoreDeletedDatabase) and creationMode is its
            # discriminator, so a payload carrying only `collation` cannot be resolved and
            # the whole request comes back 400 InvalidInput -- with no hint that the
            # discriminator is what is missing. `collation` and `backupRetentionDays` are
            # accepted only under this mode.
            #
            # The collation must be a UTF-8 one for DuckLake and CANNOT be changed after
            # creation, which is why this goes through the REST API rather than `fab create`.
            body = {
                "displayName": DUCKLAKE_SQL_DB,
                "creationPayload": {
                    "creationMode": "New",
                    "collation": "Latin1_General_100_BIN2_UTF8",
                },
            }
            if folder_id:
                body["folderId"] = folder_id
            r = req("POST", f"workspaces/{WS}/sqlDatabases", json=body)
            log(f"  + created SQL DB {DUCKLAKE_SQL_DB}: {r.status_code}")
            if r.status_code not in (200, 201, 202):
                # Fail here with the body rather than polling for something that is not coming.
                raise SystemExit(
                    f"could not create SQL DB {DUCKLAKE_SQL_DB}: {r.status_code} {r.text[:500]}"
                )
        server = database = ""
        for _ in range(40):  # provisioning is async and the connection details land last
            r = req("GET", f"workspaces/{WS}/sqlDatabases")
            if r.status_code == 200:
                for it in r.json().get("value", []):
                    if it.get("displayName") == DUCKLAKE_SQL_DB:
                        db_id = it["id"]
                        props = it.get("properties", {})
                        server = props.get("serverFqdn", "")
                        database = props.get("databaseName", "")
            if server and database:
                break
            time.sleep(15)
        if not (server and database):
            raise SystemExit("ducklake SQL DB connection details never appeared")
        if folder_id and db_id:
            move_to_folder(db_id, folder_id)
        record.item(db_id, "catalog", "SQLDatabase", DUCKLAKE_SQL_DB, engine="ducklake")
        emit("DUCKLAKE_CATALOG_DSN", f"Server={server};Database={database};Encrypt=yes")
        # DuckLake's parquet goes under the shared lakehouse's TABLES, as in the original
        # ducklake repo: DuckLake lays it out as <data_path>/<schema>/<table>/ and the
        # delta_export() on-run-end hook writes each table's _delta_log IN PLACE, so
        # Tables/ducklake_mart/<table> becomes a real lakehouse table Direct Lake can read.
        # (Under Files/ the export produced nothing Fabric could see.) The ducklake_* schema
        # folders keep it apart from duckrun_*, iceberg_* and spark_*. Its own key, NOT an
        # override of FILES_PATH: overriding that is what used to send this leg's downloader
        # off to a private copy of the CSVs.
        emit("DUCKLAKE_DATA_PATH", abfss(data_id, "Tables"))
        # For a LOCAL run of the ducklake target only. On CI the leg runs inside Fabric, where
        # remote_dbt.py's setup hook mints this from notebookutils; a runner-side copy would
        # land unmasked in $GITHUB_ENV and never be read.
        if not os.environ.get("GITHUB_ACTIONS"):
            try:
                emit("DBT_ENV_SECRET_SQL_TOKEN", token("https://database.windows.net/"))
            except (FileNotFoundError, subprocess.CalledProcessError):
                log("  ! no `az` to mint the SQL DB token; export DBT_ENV_SECRET_SQL_TOKEN "
                    "yourself for a local ducklake run")

    elif engine == "dwh":
        wh_id = ensure("warehouses", DWH_WAREHOUSE, None, folder_id)
        emit("FABRIC_DWH_SERVER", warehouse_connection(wh_id))
        emit("FABRIC_DWH_NAME", DWH_WAREHOUSE)
        # The item, not just the name: layout.py reads the warehouse's Tables by GUID.
        emit("FABRIC_DWH_ID", wh_id)
        emit("FABRIC_AUTH", "CLI")
        # A Warehouse bills its own compute (`Warehouse Query`) against its own item, and the
        # item outlives every run -- so the leg's window, not the GUID alone, is what
        # attributes it. See history/README.md.
        record.item(wh_id, "warehouse", "Warehouse", DWH_WAREHOUSE, engine="dwh")
        record.leg("dwh", compute=[wh_id])
        # A Warehouse has no Files section, so it reads the landing zone through a shortcut
        # hosted in the shared data lakehouse. Note this is the READ path only — the
        # downloader still writes to LANDING_PATH above.
        ensure_landing_shortcut(data_id, landing_id)
        emit("FILES_PATH", f"{abfss(data_id, 'Files')}/landing")

    elif engine == "spark":
        emit("FABRIC_LAKEHOUSE_ID", data_id)
        emit("FABRIC_LAKEHOUSE_NAME", DATA_LAKEHOUSE)
        # Resolved from the GUID, never hardcoded: dbt-fabricspark needs the workspace NAME
        # to build relations against a schema-enabled lakehouse.
        emit("FABRIC_WORKSPACE_NAME", workspace_name())
        # A Livy session bills `High Concurrency Session Livy Run` against the lakehouse it
        # was opened on -- the SHARED one. Only the spark leg opens Livy sessions there, so the
        # (item, operation) pair is still unambiguous; the window does the rest.
        record.leg("spark", compute=[data_id])

    log(f"provisioned {engine}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
