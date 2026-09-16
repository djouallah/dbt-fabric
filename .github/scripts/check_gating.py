#!/usr/bin/env python3
"""Prove the +enabled gating is correct, offline and without credentials.

WHY THIS EXISTS: the default failure mode of the five-tree layout is a run that builds
NOTHING and exits 0. If a target name stops matching a folder name, every +enabled goes
false, `dbt build` reports "Nothing to do", and the job goes green having done nothing.
Nothing else in the repo catches that.

Runs `dbt parse` once per target with dummy env vars and asserts, for each:
  * the ENABLED model set is exactly the canonical eight
  * the other four engine trees are in manifest['disabled'] -- not merely ABSENT, because
    a tree that failed to parse at all would also look empty
  * every model fqn is [aemo_electricity, aemo, <engine>, <layer>, <name>]
  * the enabled singular-test count matches, and no test belongs to another engine
  * the generic tests (declared in models/aemo/_*.yml) run on EVERY engine -- this is the
    one that regressed elsewhere: a gate on the project key silently disabled them,
    because a generic test takes the fqn of the YML FILE, not of the model it patches

Usage:  python .github/scripts/check_gating.py
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
ENGINES = ["duckrun", "iceberg", "ducklake", "dwh", "spark"]

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

# Singular tests live at tests/aemo/<engine>/ and are gated per engine.
SINGULAR = 12
# Generic tests come from models/aemo/_*.yml and must run on EVERY engine.
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


def parse(target: str, target_path: Path) -> dict:
    env = {**os.environ, **DUMMY_ENV}
    r = subprocess.run(
        ["dbt", "parse", "--target", target, "--profiles-dir", ".",
         "--no-partial-parse", "--target-path", str(target_path)],
        cwd=REPO, env=env, capture_output=True, text=True,
    )
    if r.returncode != 0:
        print(r.stdout[-4000:])
        print(r.stderr[-2000:], file=sys.stderr)
        raise SystemExit(f"dbt parse failed for target {target}")
    return json.loads((target_path / "manifest.json").read_text(encoding="utf-8"))


def check(target: str, manifest: dict) -> list[str]:
    errs: list[str] = []
    nodes = manifest["nodes"]
    disabled = manifest.get("disabled", {})

    models = {k: v for k, v in nodes.items() if v["resource_type"] == "model"}
    names = {v["name"] for v in models.values()}
    if names != MODELS:
        errs.append(f"enabled models {sorted(names)} != canonical {sorted(MODELS)}")

    for uid, node in models.items():
        fqn = node["fqn"]
        if len(fqn) != 5 or fqn[0] != PROJECT or fqn[1] != DATASET or fqn[2] != target:
            errs.append(f"{uid}: fqn {fqn} is not [{PROJECT}, {DATASET}, {target}, <layer>, <name>]")

    # The other four trees must be present-and-disabled, not simply missing.
    disabled_engines = {
        n["fqn"][2]
        for entries in disabled.values()
        for n in entries
        if n.get("resource_type") == "model" and len(n.get("fqn", [])) >= 3
    }
    for other in ENGINES:
        if other == target:
            continue
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


def main() -> int:
    failed = False
    for target in ENGINES:
        with tempfile.TemporaryDirectory() as td:
            manifest = parse(target, Path(td))
        errs = check(target, manifest)
        if errs:
            failed = True
            print(f"FAIL {target}")
            for e in errs:
                print(f"       {e}")
        else:
            print(f"ok   {target}  {len(MODELS)} models, {SINGULAR} singular + {GENERIC} generic tests")
    if failed:
        print("\ngating is WRONG -- do not spend capacity on this build", file=sys.stderr)
        return 1
    print("\ngating ok for all five engines")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
