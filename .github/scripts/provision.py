#!/usr/bin/env python3
"""Create the Fabric items one engine needs, and print the env vars its profile reads.

    python .github/scripts/provision.py <engine> >> "$GITHUB_ENV"

Idempotent: every item is create-if-missing, keep-if-present. Diagnostics go to stderr so
stdout is nothing but KEY=value lines.

WHY EACH ENGINE GETS ITS OWN ITEM: the five engines write the same models to five different
stores, and the whole point is to compare them, so they must not share a destination.

The landing lakehouse is the exception — one `dbt_landing`, shared, holding the CSVs
download_aemo.py lands. Every engine reads the SAME bytes; that is what makes the parity
comparison meaningful. A Fabric WAREHOUSE has no Files section and cannot host a shortcut,
so the dwh leg gets a small extra lakehouse that holds nothing but a shortcut to it.
"""
from __future__ import annotations

import os
import sys
import time

import requests

API = "https://api.fabric.microsoft.com/v1"
WS = os.environ["FABRIC_WORKSPACE_ID"]
FOLDER = os.environ.get("FOLDER", "aemo")

LANDING_LAKEHOUSE = "dbt_landing"

# One item per engine. Prefix, not suffix, so no name is a prefix of another.
ITEMS = {
    "duckrun":  ("lakehouses", "dbt_duckrun"),
    "iceberg":  ("lakehouses", "dbt_iceberg"),
    "ducklake": ("lakehouses", "dbt_ducklake"),
    "spark":    ("lakehouses", "dbt_spark"),
    "dwh":      ("warehouses", "dbt_dwh"),
}
# The Warehouse cannot host a shortcut to the landing zone, so dwh reads through this.
DWH_SRC_LAKEHOUSE = "dbt_dwh_src"
DUCKLAKE_SQL_DB = "dbt_ducklake_meta"


def log(msg: str) -> None:
    print(msg, file=sys.stderr)


def emit(key: str, value: str) -> None:
    print(f"{key}={value}")


def token(resource: str = "https://api.fabric.microsoft.com") -> str:
    try:
        import duckrun.auth

        return duckrun.auth.get_fabric_token()
    except Exception:
        import subprocess

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


def find(kind: str, name: str) -> str | None:
    r = req("GET", f"workspaces/{WS}/{kind}")
    if r.status_code != 200:
        return None
    for it in r.json().get("value", []):
        if it.get("displayName") == name:
            return it["id"]
    return None


def ensure(kind: str, name: str, payload: dict | None = None) -> str:
    """Create if missing, keep if present."""
    existing = find(kind, name)
    if existing:
        log(f"  = {kind[:-1]} {name} exists ({existing})")
        return existing
    body = {"displayName": name}
    if payload:
        body.update(payload)
    r = req("POST", f"workspaces/{WS}/{kind}", json=body)
    if r.status_code in (200, 201, 202):
        # Fabric answers 409 ItemDisplayNameNotAvailableYet for a while after a delete, and
        # the create itself can be async, so poll for the item rather than trusting the body.
        for _ in range(40):
            got = find(kind, name)
            if got:
                log(f"  + created {kind[:-1]} {name} ({got})")
                return got
            time.sleep(15)
    raise SystemExit(f"could not create {kind[:-1]} {name}: {r.status_code} {r.text[:300]}")


def abfss(item_id: str, section: str) -> str:
    return f"abfss://{WS}@onelake.dfs.fabric.microsoft.com/{item_id}/{section}"


def ensure_landing_shortcut(host_lakehouse_id: str, landing_id: str) -> None:
    """Shortcut Files/landing in the engine's own lakehouse -> the shared landing zone.

    OneLake accounts a transaction against the REQUESTED path, so reading through a
    shortcut books the cost to the item hosting the shortcut — which is how each engine's
    read cost stays attributable to that engine rather than to the landing lakehouse.
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
    if len(sys.argv) != 2 or sys.argv[1] not in ITEMS:
        print(f"usage: provision.py [{' | '.join(ITEMS)}]", file=sys.stderr)
        return 2
    engine = sys.argv[1]
    kind, name = ITEMS[engine]

    log(f"provisioning {engine} in workspace {WS}")
    landing_id = ensure("lakehouses", LANDING_LAKEHOUSE,
                        {"creationPayload": {"enableSchemas": True}})
    files_path = abfss(landing_id, "Files")

    if kind == "lakehouses":
        item_id = ensure(kind, name, {"creationPayload": {"enableSchemas": True}})
        ensure_landing_shortcut(item_id, landing_id)
    else:
        item_id = ensure(kind, name)

    # Every engine reads the SAME landed bytes.
    emit("FILES_PATH", files_path)

    if engine == "duckrun":
        emit("ONELAKE_TABLES_PATH", abfss(item_id, "Tables"))

    elif engine == "iceberg":
        # The Iceberg REST catalog addresses the item as <workspace>/<item>, not abfss.
        emit("WAREHOUSE_PATH", f"{WS}/{item_id}")
        emit("ONELAKE_ENDPOINT", "https://onelake.table.fabric.microsoft.com/iceberg")

    elif engine == "ducklake":
        # The catalog metadata lives in a Fabric SQL DB. Created through the REST API, not
        # `fab create`, because only the API accepts a collation — and DuckLake needs a
        # UTF-8 one, which CANNOT be changed after creation.
        db_id = find("sqlDatabases", DUCKLAKE_SQL_DB)
        if not db_id:
            r = req("POST", f"workspaces/{WS}/sqlDatabases", json={
                "displayName": DUCKLAKE_SQL_DB,
                "creationPayload": {"collation": "Latin1_General_100_BIN2_UTF8"},
            })
            log(f"  + created SQL DB {DUCKLAKE_SQL_DB}: {r.status_code}")
        server = database = ""
        for _ in range(40):  # provisioning is async and the connection details land last
            r = req("GET", f"workspaces/{WS}/sqlDatabases")
            if r.status_code == 200:
                for it in r.json().get("value", []):
                    if it.get("displayName") == DUCKLAKE_SQL_DB:
                        p = it.get("properties", {})
                        server, database = p.get("serverFqdn", ""), p.get("databaseName", "")
            if server and database:
                break
            time.sleep(15)
        if not (server and database):
            raise SystemExit("ducklake SQL DB connection details never appeared")
        emit("DUCKLAKE_CATALOG_DSN", f"Server={server};Database={database};Encrypt=yes")
        # DuckLake writes its parquet under the engine's own lakehouse.
        emit("FILES_PATH", abfss(item_id, "Files"))
        emit("DBT_ENV_SECRET_SQL_TOKEN", token("https://database.windows.net/"))

    elif engine == "dwh":
        emit("FABRIC_DWH_SERVER", warehouse_connection(item_id))
        emit("FABRIC_DWH_NAME", name)
        emit("FABRIC_AUTH", "CLI")
        # A Warehouse has no Files section, so it reads the landing zone through a
        # shortcut hosted in a lakehouse that exists only for that purpose.
        src_id = ensure("lakehouses", DWH_SRC_LAKEHOUSE,
                        {"creationPayload": {"enableSchemas": True}})
        ensure_landing_shortcut(src_id, landing_id)
        emit("FILES_PATH", f"{abfss(src_id, 'Files')}/landing")

    elif engine == "spark":
        emit("FABRIC_LAKEHOUSE_ID", item_id)
        emit("FABRIC_LAKEHOUSE_NAME", name)
        # Resolved from the GUID, never hardcoded: dbt-fabricspark needs the workspace NAME
        # to build relations against a schema-enabled lakehouse.
        emit("FABRIC_WORKSPACE_NAME", workspace_name())

    log(f"provisioned {engine}: {name} ({item_id})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
