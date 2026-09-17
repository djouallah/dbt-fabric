#!/usr/bin/env python3
"""Run one DuckDB-family engine's dbt build ON FABRIC COMPUTE, from the GitHub runner.

    python .github/scripts/remote_dbt.py <duckrun | ducklake | iceberg>

WHY. DuckDB folds the AEMO archive in memory. On the 7 GB hosted runner the duckrun leg was
shut down mid-fct_scada twice in one day ("The runner has received a shutdown signal") once a
couple of cancelled runs had left a backlog; ducklake and iceberg are the same fold. The
original delta repo never ran dbt on the runner: its run_dbt.py used duckrun's RemoteRunner,
"data-local to OneLake -- a GitHub runner pulls every byte across the internet twice".

HOW. duckrun.workspace(...).run_python(...) ships this repo to a throwaway Fabric Python
notebook of FABRIC_CORES vCores, pip-installs requirements/<engine>.txt there, mints the tokens
the profile needs from notebookutils (the kernel-side `setup` hook below), runs
.github/scripts/run_in_fabric.py as a subprocess with the config env forwarded, streams the log
back as `[remote]` lines, and deletes the notebook. This script then lifts the parity
fingerprint out of that log so history/parity/<engine>.json lands where the upload step finds
it for the local legs too.

TOKENS NEVER TRAVEL. ONELAKE_TOKEN (and DBT_ENV_SECRET_SQL_TOKEN for ducklake) are minted
INSIDE Fabric by notebookutils -- the same calls the original ducklake notebook made -- and
never appear in the notebook definition. Only non-secret config is forwarded (FORWARD below).

Why run_python rather than RemoteRunner: RemoteRunner assumes a duckrun-type target with a
root_path to locate the workspace and refuses to forward anything named like a token, so it
cannot carry the dbt-duckdb profiles (iceberg, ducklake). run_python is what RemoteRunner is
built on, and it takes a pip list, an env dict and the setup hook.

Needs FABRIC_WORKSPACE_ID and DATA_LAKEHOUSE_ID (provision.py emits the latter) plus the OIDC
identity (AZURE_CLIENT_ID / AZURE_TENANT_ID, `id-token: write`) with item CRUD on the workspace.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ENGINES = ("duckrun", "ducklake", "iceberg")
REPO = Path(__file__).resolve().parents[2]
CORES = int(os.environ.get("FABRIC_CORES", "8"))

# Config the profile and macros read at run time, and NOTHING token-shaped. LANDING_PATH and
# the download limits stay behind: only download_aemo.py reads them, and it never runs here.
# Also not forwarded, on purpose: AZURE_TRANSPORT_OPTION_TYPE /
# CURL_CA_INFO (inside Fabric DuckDB's default OneLake transport is the one that works, and the
# on-run-start hook renders to nothing when the variable is unset) and DUCKDB_TEMP_DIR
# (run_in_fabric.py points it at the notebook's 135 GiB work disk, not the 19 GiB /tmp overlay).
FORWARD = (
    "FILES_PATH", "ONELAKE_TABLES_PATH",
    "WAREHOUSE_PATH", "ONELAKE_ENDPOINT",
    "DUCKLAKE_CATALOG_DSN", "DUCKLAKE_DATA_PATH",
    "DBT_SCHEMA", "DBT_THREADS",
    "process_limit",
)

# exec'd in the notebook KERNEL before the script starts, where notebookutils is guaranteed.
# The subprocess inherits os.environ, so these become the env_var() values the profile reads.
SETUP = {
    "duckrun": "",  # the duckrun adapter self-acquires through notebookutils
    "iceberg": (
        "import os, notebookutils\n"
        "os.environ['ONELAKE_TOKEN'] = notebookutils.credentials.getToken('storage')\n"
    ),
    "ducklake": (
        "import os, notebookutils\n"
        "os.environ['ONELAKE_TOKEN'] = notebookutils.credentials.getToken('storage')\n"
        # The DuckLake catalog is a Fabric SQL DB: a database.windows.net audience, forwarded to
        # the catalog connection through the profile's meta_access_token.
        "os.environ['DBT_ENV_SECRET_SQL_TOKEN'] = "
        "notebookutils.credentials.getToken('https://database.windows.net/')\n"
    ),
}


def pip_args(engine: str) -> list[str]:
    """requirements/<engine>.txt as pip arguments -- the SAME pins the runner installs, so the
    notebook builds with exactly the versions the gating job parsed with. Flags like `--pre`
    stay flags."""
    args: list[str] = []
    for line in (REPO / "requirements" / f"{engine}.txt").read_text(encoding="utf-8").splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            args.extend(line.split())
    return args


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1] not in ENGINES:
        print(f"usage: remote_dbt.py [{' | '.join(ENGINES)}]", file=sys.stderr)
        return 2
    engine = sys.argv[1]

    import duckrun  # installed by every DuckDB leg's requirements

    ws = duckrun.workspace(os.environ["FABRIC_WORKSPACE_ID"])
    env = {k: os.environ[k] for k in FORWARD if os.environ.get(k)}
    print(f"[remote_dbt] {engine}: {CORES} vCores, forwarding {', '.join(sorted(env))}", flush=True)

    result = ws.run_python(
        str(REPO),
        entry=".github/scripts/run_in_fabric.py",
        args=[engine],
        cores=CORES,
        lakehouse=os.environ["DATA_LAKEHOUSE_ID"],
        pip=pip_args(engine),
        env=env,
        setup=SETUP[engine] or None,
        # Named so the run's capacity is attributable in the Fabric capacity metrics.
        name=f"dbt-{engine}-{os.environ.get('GITHUB_RUN_ID', 'local')}",
    )

    # The fingerprint JSON is in the streamed log (run_in_fabric.py runs the run-operation
    # last). Same capture as the local legs' `| parity.py capture`, so the artifact is identical.
    capture = subprocess.run(
        [sys.executable, str(REPO / ".github/scripts/parity.py"), "capture", "history/parity"],
        input=result.log, text=True,
    )
    if result.success and capture.returncode != 0:
        print("[remote_dbt] build succeeded but no fingerprint was captured", file=sys.stderr)
        return 1
    print(f"[remote_dbt] {engine}: success={result.success} returncode={result.returncode} "
          f"notebook={result.item_id}", flush=True)
    return 0 if result.success else 1


if __name__ == "__main__":
    raise SystemExit(main())
