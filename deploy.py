#!/usr/bin/env python3
"""Deploy the in-Fabric demo: the repo into the shared lakehouse, the scheduling items, and a
per-engine Direct Lake semantic model.

    python deploy.py --engine iceberg            # repo copy + semantic model aemo_iceberg
    python deploy.py --engine iceberg --full     # + notebook, variable library, pipeline, schedule
    python deploy.py --engine dwh --no-model     # repo copy only

WHAT IT DOES, IN ORDER
  1. .github/scripts/provision.py <engine>: every Fabric item exists (idempotent).
  2. The git-tracked repo goes to Files/dbt/ of the shared `dbt` lakehouse, stale files removed.
     That copy is what fabric_items/run.Notebook runs: the SAME provision / land / build scripts
     CI runs, on the engine the deploy_config Variable Library names.
  3. --full: fabric_items/ (variable library, notebook, pipeline) and a 12-hourly schedule.
  4. The semantic model, as aemo_<engine>, bound to <engine>_mart.

DIRECT LAKE HAS NO SCHEMA PARAMETER. A partition's schemaName is a literal in the model
definition, and duckrun's deploy() rewrites only the OneLake GUIDs -- so the committed model.bim
carries placeholders (`mart`, and Desktop's GUIDs) and rebind_schema() writes the engine's schema
in at deploy time, the way the GUIDs are. Every engine writes <engine>_mart
(macros/generate_schema_name.sql); DBT_SCHEMA prefixes it here exactly as it does there.

ORDER MATTERS: a Direct Lake model is REFRAMED on deploy, which needs <schema>.fct_summary to
exist. Build the engine once (CI, or the scheduled notebook) before deploying its model.

dwh binds the same bim to the Warehouse item as Direct Lake on OneLake (mode="direct_lake": a
warehouse's tables are Delta in OneLake). The four lakehouse engines share one lakehouse and are
separated by schema, so their models differ in that one string only.

One backend, duckrun, for all five engines. There used to be a `fab` CLI backend for dwh and
ducklake because duckrun could not create a Warehouse or a SQL DB; provision.py creates both
through the REST API now, so it is gone. Not run by any workflow: deploying items and spending
capacity is a deliberate act.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent
SCRIPTS = REPO / ".github" / "scripts"
BIM = REPO / "semantic_model" / "aemo_electricity.SemanticModel" / "model.bim"
ENGINES = ("duckrun", "iceberg", "ducklake", "dwh", "spark")

FOLDER = os.environ.get("FOLDER", "dbt")


def log(m: str) -> None:
    print(m, flush=True)


def mart_schema(engine: str) -> str:
    """The schema this engine's gold layer lives in -- the rule of generate_schema_name():
    DBT_SCHEMA unset or 'mart' -> <engine>_mart, anything else -> <DBT_SCHEMA>_<engine>_mart."""
    prefix = os.environ.get("DBT_SCHEMA", "mart")
    return f"{engine}_mart" if prefix == "mart" else f"{prefix}_{engine}_mart"


def rebind_schema(bim_text: str, schema: str) -> str:
    """Point every Direct Lake partition (and its table's lineage tag) at `schema`.

    Walks the JSON rather than replacing text, so a reformatted bim cannot leave a partition
    silently on the `mart` placeholder -- a model bound to a schema no engine writes. The OneLake
    GUIDs are left alone: duckrun's deploy(lakehouse=/warehouse=) rewrites those."""
    model = json.loads(bim_text)
    for table in model["model"].get("tables", []):
        for part in table.get("partitions", []):
            src = part.get("source", {})
            if "schemaName" in src:
                src["schemaName"] = schema
        tag = table.get("sourceLineageTag", "")
        if tag.startswith("[") and "].[" in tag:
            table["sourceLineageTag"] = f"[{schema}].[" + tag.split("].[", 1)[1]
    return json.dumps(model, indent=2) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--engine", required=True, choices=ENGINES)
    ap.add_argument("--full", action="store_true",
                    help="also deploy the notebook, variable library, pipeline and schedule")
    ap.add_argument("--no-model", action="store_true", help="skip the semantic model")
    a = ap.parse_args()
    engine = a.engine

    ws_id = os.environ.get("FABRIC_WORKSPACE_ID", "")
    if not ws_id:
        raise SystemExit("FABRIC_WORKSPACE_ID is not set")

    # provision.py reads FABRIC_WORKSPACE_ID at import, hence after the check. It is the one
    # place the item names live.
    sys.path.insert(0, str(SCRIPTS))
    import provision  # noqa: E402
    import duckrun  # noqa: E402
    from duckrun.workspace import RemoteRunError  # noqa: E402

    log(f"provisioning {engine}")
    subprocess.run([sys.executable, str(SCRIPTS / "provision.py"), engine],
                   check=True, stdout=subprocess.DEVNULL, cwd=REPO)

    ws = duckrun.workspace(ws_id)
    lh = provision.DATA_LAKEHOUSE
    # Exactly `git ls-files` (no target/, logs/, local secrets), into Files/dbt/, with stale
    # remote files under that prefix removed. Anchored at REPO, not the cwd.
    duckrun.connect(f"{ws.display_name}/{lh}.Lakehouse").copy(
        str(REPO), "dbt", git_only=True, sync=True, overwrite=True)
    log(f"copied the repo into {lh}/Files/dbt")

    if a.full:
        # _scan_item_folders validates EVERY item folder before deploying ANY of them and raises
        # on an unknown type, so one stray folder under fabric_items/ makes this ship nothing.
        # That is why the semantic model lives in its own directory. duckrun points the
        # pipeline's notebook activities at the folder's one notebook itself.
        ws.deploy(str(REPO / "fabric_items"), variables={"dbt_target": engine},
                  folder=FOLDER, overwrite=True)
        # Idempotent: updates the existing schedule rather than stacking duplicates.
        ws.schedule("run_pipeline", every="720m")
        log(f"deployed fabric_items; run_pipeline builds {engine} every 12 h")

    if not a.no_model:
        schema = mart_schema(engine)
        # `warehouse=` alone is duckrun's DirectQuery path and raises on a Direct Lake bim;
        # mode="direct_lake" is what binds a Warehouse item's OneLake root instead.
        source = ({"warehouse": provision.DWH_WAREHOUSE, "mode": "direct_lake"}
                  if engine == "dwh" else {"lakehouse": lh})
        with tempfile.TemporaryDirectory() as tmp:
            bim = Path(tmp) / "model.bim"  # the extension is how duckrun types the item
            bim.write_text(rebind_schema(BIM.read_text(encoding="utf-8-sig"), schema),
                           encoding="utf-8")
            try:
                # overwrite=True is an in-place updateDefinition: the item id, and with it every
                # report bound to the model, survives.
                ws.deploy(str(bim), name=f"aemo_{engine}", folder=FOLDER, overwrite=True,
                          **source)
            except RemoteRunError as e:
                raise SystemExit(
                    f"semantic model aemo_{engine} failed to deploy: {e}\n"
                    f"A Direct Lake model reframes on deploy, so {schema}.fct_summary must "
                    f"exist -- build {engine} once (CI, or the scheduled notebook) first."
                ) from e
        log(f"deployed semantic model aemo_{engine} over {schema}")

    log("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
