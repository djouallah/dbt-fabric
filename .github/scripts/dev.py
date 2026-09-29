#!/usr/bin/env python3
"""Build one engine from VS Code, against the DEV workspace.

    az login
    .venv-dwh/Scripts/python .github/scripts/dev.py dwh build            # Windows
    .venv-spark/bin/python   .github/scripts/dev.py spark build -s fct_summary

Everything after the engine is passed to dbt. Run it with the ENGINE'S OWN venv's python:
dbt-fabric and dbt-fabricspark cannot share an environment, and dbt runs here as
`sys.executable -m dbt.cli.main`.

THE WORKSPACE IS THE ONE IN `.env`, which is gitignored: copy `.env.example` and set
FABRIC_WORKSPACE_ID to your DEV workspace. Never the production one; production is built
only by run_pipeline, in Fabric.

The DEV workspace is installed like production (deploy.yml, environment `dev`), and
run_pipeline has run there once: its `ingest` landed the files and its `run` wrote the
landing shortcut. This script creates nothing. It looks the items up, sets the same env vars
the `run` notebook sets, with the settings of the deploy_config Variable Library, and runs
dbt as your `az login` identity (FABRIC_AUTH=CLI). tests_py/test_fabric_items.py pins the
env vars to the notebook's.
"""
from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
ENGINES = ("dwh", "spark")
# The package each engine's adapter installs, to catch a run from the wrong venv.
ADAPTER = {"dwh": "dbt.adapters.fabric", "spark": "dbt.adapters.fabricspark"}
VARIABLES = REPO / "fabric_items" / "deploy_config.VariableLibrary" / "variables.json"


def dotenv(path: Path) -> dict[str, str]:
    """KEY=value lines; blank lines and # comments skipped."""
    pairs = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            pairs[key.strip()] = value.strip().strip('"').strip("'")
    return pairs


def settings() -> dict[str, str]:
    """The deploy_config Variable Library's values: what the `run` notebook reads in Fabric."""
    return {v["name"]: v["value"]
            for v in json.loads(VARIABLES.read_text(encoding="utf-8"))["variables"]}


def main() -> int:
    if len(sys.argv) < 3 or sys.argv[1] not in ENGINES:
        print(f"usage: dev.py <{' | '.join(ENGINES)}> <dbt command> [dbt args...]", file=sys.stderr)
        return 2
    engine, args = sys.argv[1], sys.argv[2:]
    if importlib.util.find_spec(ADAPTER[engine]) is None:
        print(f"{ADAPTER[engine]} is not installed in {sys.executable}: run this with the "
              f"{engine} venv (pip install -r requirements/{engine}.txt)", file=sys.stderr)
        return 2
    if not (REPO / ".env").exists():
        print("no .env: copy .env.example and set FABRIC_WORKSPACE_ID to your DEV workspace",
              file=sys.stderr)
        return 2
    os.environ.update(dotenv(REPO / ".env"))

    sys.path.insert(0, str(Path(__file__).parent))
    import provision  # reads FABRIC_WORKSPACE_ID at import

    def item(kind: str, name: str) -> str:
        found = provision.find(kind, name)
        if not found:
            raise SystemExit(f"{name} is not in workspace {provision.WS}: deploy to it first "
                             "(deploy.yml, environment dev)")
        return found

    vl = settings()
    env = dict(os.environ)
    env["process_limit"] = os.environ.get("process_limit", vl["process_limit"])
    env["DBT_SCHEMA"] = vl["dbt_schema"]
    env["SPARK_RESOURCE_PROFILE"] = vl["spark_resource_profile"]
    env["FABRIC_WORKSPACE_ID"] = provision.WS
    # dbt-fabricspark 1.13.x runs OPTIMIZE after every Delta build, which rewrites the layout
    # the run just produced.
    env["DBT_FABRICSPARK_SKIP_OPTIMIZE"] = "true"
    env["PYTHONUNBUFFERED"] = "1"

    landing = item("lakehouses", provision.LANDING_LAKEHOUSE)
    lakehouse = item("lakehouses", provision.DATA_LAKEHOUSE)
    env["FABRIC_WORKSPACE_NAME"] = provision.workspace_name()
    if engine == "dwh":
        env["FABRIC_DWH_NAME"] = provision.DWH_WAREHOUSE
        env["FABRIC_DWH_SERVER"] = provision.warehouse_connection(
            item("warehouses", provision.DWH_WAREHOUSE))
        # The shortcut the `run` notebook writes on its first dwh leg in this workspace.
        env["FILES_PATH"] = f"{provision.abfss(lakehouse, 'Files')}/landing"
    else:
        env["FABRIC_LAKEHOUSE_ID"] = lakehouse
        env["FABRIC_LAKEHOUSE_NAME"] = provision.DATA_LAKEHOUSE
        env["FILES_PATH"] = provision.abfss(landing, "Files")
    # Both adapters take the token from the Azure CLI: your `az login`.
    env["FABRIC_AUTH"] = "CLI"

    print(f"{engine} -> {env['FABRIC_WORKSPACE_NAME']} ({provision.WS})", file=sys.stderr)
    return subprocess.run([sys.executable, "-m", "dbt.cli.main", *args,
                           "--target", engine, "--profiles-dir", "."],
                          cwd=REPO, env=env).returncode


if __name__ == "__main__":
    raise SystemExit(main())
