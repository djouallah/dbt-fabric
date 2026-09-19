#!/usr/bin/env python3
"""Prove the +enabled gating is correct, offline and without credentials.

WHY THIS EXISTS: the default failure mode of the engine-tree layout is a run that builds
NOTHING and exits 0. If a target name stops matching a folder name, every +enabled goes
false, `dbt build` reports "Nothing to do", and the job goes green having done nothing.
Nothing else in the repo catches that.

ONE PROJECT, dbt1/, FIVE ENGINES: duckrun, iceberg, ducklake, dwh, spark. It was briefly two
-- iceberg spent a day in a dbt2/ on dbt OSS 2, whose catalogs.yml cannot sit in a dbt 1.x
project root -- and ENGINES below is what is left of that: the engine -> project mapping,
kept because tests_py/_layout.py's PROJECT_OF holds the same dict and the two must agree.
run_dbt.py holds a THIRD copy, but of the DuckDB SUBSET only (duckrun, iceberg, ducklake):
dwh and spark never go through it, and tests_py/test_local_runner.py asserts that subset and
pipeline.yml's dwh/spark build step partition these five between them -- an engine in neither
reaches no build step at all, which is silent and green.

Runs `dbt parse` once per engine with dummy env vars and asserts, for each:
  * the ENABLED model set is exactly the canonical eight
  * every OTHER engine tree is in manifest['disabled'] -- not merely ABSENT, because a tree
    that failed to parse at all would also look empty
  * every model fqn is [aemo_electricity, aemo, <engine>, <layer>, <name>]
  * the schema is <engine>_landing / <engine>_mart -- the engine prefix is what keeps five
    engines from overwriting each other inside one shared lakehouse
  * the enabled singular-test count matches, and no test belongs to another engine
  * the generic tests (declared in models/aemo/_*.yml) run on EVERY engine -- this is the
    one that regressed elsewhere: a gate on the project key silently disabled them,
    because a generic test takes the fqn of the YML FILE, not of the model it patches

ONE ENGINE PER INVOCATION, and CI runs it as a five-way matrix. The adapters still cannot
share an environment: dbt-fabric and dbt-fabricspark shadow each other under the
dbt.adapters namespace ("has no attribute 'Plugin'"). Running per engine is also more
faithful: each one is validated against exactly the dependencies it will build with.

Usage:  python .github/scripts/check_gating.py <engine>
        python .github/scripts/check_gating.py            # every engine this shell can reach

The no-argument form checks every engine whose adapter is importable. One dbt major covers
all five now, so a single environment with dbt-duckdb reaches iceberg AND ducklake -- where
before iceberg needed a second venv and a DBT2_BIN pointing at it.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
PROJECT = "aemo_electricity"
DATASET = "aemo"

# engine -> the dbt project directory it lives in. The KEY is the dbt target name, the
# models/aemo/<engine>/ folder name and the schema prefix, all at once.
ENGINES = {
    "duckrun": "dbt1",
    "iceberg": "dbt1",
    "ducklake": "dbt1",
    "dwh": "dbt1",
    "spark": "dbt1",
}

MODELS = {
    "stg_csv_archive_log",
    "dim_calendar",
    "dim_duid",
    "fct_price",
    "fct_price_today",
    "fct_scada",
    "fct_scada_today",
    "fct_summary",
}

# Singular tests live at <project>/tests/aemo/<engine>/ and are gated per engine.
SINGULAR = 12
# Generic tests come from <project>/models/aemo/_*.yml and must run on EVERY engine.
GENERIC = 30

# Enough to satisfy every profile's env_var() calls; none of it is contacted.
DUMMY_ENV = {
    "FILES_PATH": "/tmp/landing",
    "ONELAKE_TABLES_PATH": "/tmp/warehouse",
    "WAREHOUSE_PATH": "ws/item",
    "ONELAKE_ENDPOINT": "https://onelake.table.fabric.microsoft.com/iceberg",
    "ONELAKE_TOKEN": "dummy",
    "DUCKLAKE_CATALOG_DSN": "Server=localhost;Database=dummy",
    "FABRIC_DWH_SERVER": "dummy.datawarehouse.fabric.microsoft.com",
    "FABRIC_DWH_NAME": "dummy",
    "FABRIC_WORKSPACE_ID": "00000000-0000-0000-0000-000000000000",
    "FABRIC_LAKEHOUSE_ID": "11111111-1111-1111-1111-111111111111",
    "FABRIC_LAKEHOUSE_NAME": "dummy",
    "DBT_SCHEMA": "mart",
}


def dbt_bin() -> str:
    """The dbt executable. DBT_BIN is an escape hatch for a laptop that keeps dbt-fabric and
    dbt-fabricspark in separate venvs, since those two still cannot share one."""
    return os.environ.get("DBT_BIN", "dbt")


def parse(target: str, target_path: Path) -> dict:
    project = ENGINES[target]
    env = {**os.environ, **DUMMY_ENV}
    # --no-partial-parse is belt and braces: every parse gets a fresh temporary
    # --target-path, so there is no prior manifest to reuse anyway.
    cmd = [dbt_bin(), "parse", "--no-partial-parse", "--target", target,
           "--profiles-dir", ".", "--target-path", str(target_path)]
    r = subprocess.run(cmd, cwd=REPO / project, env=env, capture_output=True, text=True)
    if r.returncode != 0:
        print(r.stdout[-4000:])
        print(r.stderr[-2000:], file=sys.stderr)
        raise SystemExit(f"dbt parse failed for target {target} in {project}/")
    return json.loads((target_path / "manifest.json").read_text(encoding="utf-8"))


def check(target: str, manifest: dict) -> list[str]:
    errs: list[str] = []
    nodes = manifest["nodes"]
    disabled = manifest.get("disabled", {})
    project = ENGINES[target]
    siblings = [e for e, p in ENGINES.items() if p == project and e != target]

    models = {k: v for k, v in nodes.items() if v["resource_type"] == "model"}
    names = {v["name"] for v in models.values()}
    if names != MODELS:
        errs.append(f"enabled models {sorted(names)} != canonical {sorted(MODELS)}")

    for uid, node in models.items():
        fqn = node["fqn"]
        if len(fqn) != 5 or fqn[0] != PROJECT or fqn[1] != DATASET or fqn[2] != target:
            errs.append(f"{uid}: fqn {fqn} is not [{PROJECT}, {DATASET}, {target}, <layer>, <name>]")

        # THE SCHEMA PREFIX IS LOAD-BEARING. All five engines write into one shared Fabric
        # lakehouse, so the engine name in the schema is the only thing keeping them apart.
        # Two engines resolving to the same schema would overwrite each other's gold layer
        # inside one item -- with every test still green, because each run would see a
        # perfectly consistent table. Nothing downstream catches that; this does, offline.
        # It is also why moving this engine between dbt majors needed no data migration.
        schema = node.get("schema") or node.get("config", {}).get("schema") or ""
        if schema not in (f"{target}_landing", f"{target}_mart"):
            errs.append(
                f"{uid}: schema {schema!r} is not {target}_landing or {target}_mart -- "
                f"the engine prefix from generate_schema_name() is missing or wrong"
            )

    # Sibling trees in the SAME project must be present-and-disabled, not simply missing.
    # Four siblings per engine, now that all five trees share one project.
    disabled_engines = {
        n["fqn"][2]
        for entries in disabled.values()
        for n in entries
        if n.get("resource_type") == "model" and len(n.get("fqn", [])) >= 3
    }
    for other in siblings:
        if other not in disabled_engines:
            errs.append(
                f"engine tree '{other}' is not in manifest['disabled'] -- it may have failed "
                f"to parse rather than being gated off"
            )

    tests = [v for v in nodes.values() if v["resource_type"] == "test"]
    singular = [t for t in tests if t.get("test_metadata") is None]
    generic = [t for t in tests if t.get("test_metadata") is not None]

    for t in singular:
        fqn = t["fqn"]
        if len(fqn) < 3 or fqn[2] != target:
            errs.append(f"singular test {t['name']} has fqn {fqn}, not under engine '{target}'")
    if len(singular) != SINGULAR:
        errs.append(f"{len(singular)} singular tests enabled, expected {SINGULAR}")
    if len(generic) != GENERIC:
        errs.append(
            f"{len(generic)} generic tests enabled, expected {GENERIC}. Generic tests take the "
            f"fqn of their YML FILE, so a gate on the project key silently disables them all."
        )
    return errs


def dbt_installed(target: str) -> bool:
    """Is this engine's adapter importable in this environment?"""
    import importlib.util

    # iceberg and ducklake share dbt-duckdb -- both are type: duckdb, which is also why
    # dbt1/macros/iceberg_adapter_overrides.sql has to branch on target.name.
    mod = {"duckrun": "dbt.adapters.duckrun", "iceberg": "dbt.adapters.duckdb",
           "ducklake": "dbt.adapters.duckdb",
           "dwh": "dbt.adapters.fabric", "spark": "dbt.adapters.fabricspark"}[target]
    try:
        return importlib.util.find_spec(mod) is not None
    except (ImportError, ValueError):
        return False


def main() -> int:
    if len(sys.argv) > 1:
        if sys.argv[1] not in ENGINES:
            print(f"unknown engine {sys.argv[1]!r}; expected one of {', '.join(ENGINES)}",
                  file=sys.stderr)
            return 2
        targets = [sys.argv[1]]
    else:
        targets = [e for e in ENGINES if dbt_installed(e)]
        skipped = [e for e in ENGINES if e not in targets]
        if skipped:
            print(f"skipping (dbt or adapter not installed): {', '.join(skipped)}")
        if not targets:
            print("no dbt installation found for any engine", file=sys.stderr)
            return 2

    failed = False
    for target in targets:
        with tempfile.TemporaryDirectory() as td:
            manifest = parse(target, Path(td))
        errs = check(target, manifest)
        if errs:
            failed = True
            print(f"FAIL {target} ({ENGINES[target]}/)")
            for e in errs:
                print(f"       {e}")
        else:
            print(f"ok   {target:<9} ({ENGINES[target]}/)  {len(MODELS)} models, "
                  f"{SINGULAR} singular + {GENERIC} generic tests")
    if failed:
        print("\ngating is WRONG -- do not spend capacity on this build", file=sys.stderr)
        return 1
    print(f"\ngating ok for {', '.join(targets)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
