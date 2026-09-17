#!/usr/bin/env python3
"""Deploy the in-Fabric demo: the repo into the shared lakehouse, the scheduling items, and a
Direct Lake semantic model per engine. GitHub Actions is the real orchestrator; the notebook
and pipeline are the demo of in-Fabric scheduling.

Run by the `deploy` job of .github/workflows/pipeline.yml (dispatch input `deploy`: none |
no_model | full), AFTER the build legs, with the legs' OIDC identity: duckrun mints the
storage, Fabric and Power BI tokens from the GitHub assertion, so there is no login step.
On a laptop, after `az login`:

    DEPLOY_ENGINES=iceberg python deploy.py

Env-driven, no argv -- the same shape as the source repo's deploy.py:

    FABRIC_WORKSPACE_ID    required
    DEPLOY_ENGINES         comma list of engines to provision and bind a model for (default: all)
    DEPLOY_SEMANTIC_MODEL  "false" skips the models -- and the Direct Lake reframe, the slow part
    DBT_SCHEMA             the schema the engines were built with (default mart), see mart_schema()
    FOLDER                 the workspace folder every item lands in (default dbt)

WHAT IT DOES, IN ORDER
  1. .github/scripts/provision.py <engine>, per engine: every Fabric item exists (idempotent).
  2. The git-tracked repo goes to Files/dbt/ of the shared `dbt` lakehouse, stale files removed.
     That copy is what fabric_items/run.Notebook runs: the SAME provision / land / build scripts
     CI runs, on the engine variables.json names (dbt_target).
  3. fabric_items/ (variable library, notebook, pipeline). The values in variables.json ARE the
     notebook's configuration; edit the file to change the engine or the limits.
  4. Unless DEPLOY_SEMANTIC_MODEL=false: the semantic model, once per engine, as aemo_<engine>
     bound to <engine>_mart.
  5. The pipeline's 12-hourly schedule.

DIRECT LAKE HAS NO SCHEMA PARAMETER. A partition's schemaName is a literal in the model
definition, and duckrun's deploy() rewrites only the OneLake GUIDs -- so the committed model.bim
carries placeholders (`mart`, and Desktop's GUIDs) and rebind_schema() writes the engine's schema
in at deploy time, the way the GUIDs are. Every engine writes <engine>_mart
(macros/generate_schema_name.sql); DBT_SCHEMA prefixes it here exactly as it does there.

ORDER MATTERS: a Direct Lake model is REFRAMED on deploy, which needs <schema>.fct_summary to
exist -- which is why the deploy job runs after the build legs.

dwh binds the same bim to the Warehouse item as Direct Lake on OneLake (mode="direct_lake": a
warehouse's tables are Delta in OneLake). The four lakehouse engines share one lakehouse and are
separated by schema, so their models differ in that one string only.

One backend, duckrun, for all five engines. There used to be a `fab` CLI backend for dwh and
ducklake because duckrun could not create a Warehouse or a SQL DB; provision.py creates both
through the REST API now, so it is gone.
"""
from __future__ import annotations

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

WS = os.environ.get("FABRIC_WORKSPACE_ID", "")
FOLDER = os.environ.get("FOLDER", "dbt")
DEPLOY_ENGINES = [e.strip() for e in os.environ.get("DEPLOY_ENGINES", ",".join(ENGINES)).split(",")
                  if e.strip()]
DEPLOY_MODEL = os.environ.get("DEPLOY_SEMANTIC_MODEL", "true").lower() != "false"
SCHEDULE_EVERY = "720m"


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
    if not WS:
        raise SystemExit("FABRIC_WORKSPACE_ID is not set")
    unknown = sorted(set(DEPLOY_ENGINES) - set(ENGINES))
    if unknown:
        raise SystemExit(f"DEPLOY_ENGINES: unknown engine(s) {unknown}; choose from {ENGINES}")

    # provision.py reads FABRIC_WORKSPACE_ID at import, hence after the check. It is the one
    # place the item names live.
    sys.path.insert(0, str(SCRIPTS))
    import provision  # noqa: E402
    import duckrun  # noqa: E402
    from duckrun.workspace import RemoteRunError  # noqa: E402

    for engine in DEPLOY_ENGINES:
        log(f"provisioning {engine}")
        subprocess.run([sys.executable, str(SCRIPTS / "provision.py"), engine],
                       check=True, stdout=subprocess.DEVNULL, cwd=REPO)

    ws = duckrun.workspace(WS)
    lh = provision.DATA_LAKEHOUSE
    # Exactly `git ls-files` (no target/, logs/, local secrets), into Files/dbt/, with stale
    # remote files under that prefix removed. Anchored at REPO, not the cwd.
    duckrun.connect(f"{ws.display_name}/{lh}.Lakehouse").copy(
        str(REPO), "dbt", git_only=True, sync=True, overwrite=True)
    log(f"copied the repo into {lh}/Files/dbt")

    # _scan_item_folders validates EVERY item folder before deploying ANY of them and raises on
    # an unknown type, so one stray folder under fabric_items/ makes this ship nothing. That is
    # why the semantic model lives in its own directory. duckrun points the pipeline's notebook
    # activities at the folder's one notebook itself.
    ws.deploy(str(REPO / "fabric_items"), folder=FOLDER, overwrite=True)
    log("deployed fabric_items")

    if DEPLOY_MODEL:
        for engine in DEPLOY_ENGINES:
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
                    # overwrite=True is an in-place updateDefinition: the item id, and with it
                    # every report bound to the model, survives.
                    ws.deploy(str(bim), name=f"aemo_{engine}", folder=FOLDER, overwrite=True,
                              **source)
                except RemoteRunError as e:
                    raise SystemExit(
                        f"semantic model aemo_{engine} failed to deploy: {e}\n"
                        f"A Direct Lake model reframes on deploy, so {schema}.fct_summary must "
                        f"exist -- build {engine} once first."
                    ) from e
            log(f"deployed semantic model aemo_{engine} over {schema}")
    else:
        log("skipping the semantic models (DEPLOY_SEMANTIC_MODEL=false)")

    # Idempotent: updates the existing schedule rather than stacking duplicates.
    ws.schedule("run_pipeline", every=SCHEDULE_EVERY)
    log(f"run_pipeline scheduled every {SCHEDULE_EVERY}; done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
