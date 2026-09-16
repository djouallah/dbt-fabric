#!/usr/bin/env python3
"""Deploy the Fabric items: notebook, variable library, pipeline, semantic model.

    python deploy.py --engine duckrun            # duckrun backend
    python deploy.py --engine dwh --full         # fab backend, everything

TWO BACKENDS, ONE INTERFACE. duckrun can create lakehouses and deploy items in one call,
but it cannot provision a Fabric WAREHOUSE or a SQL DB — so the dwh and ducklake legs go
through the `fab` CLI instead. Everything the two share is in Deployer; each backend fills
in the parts only it can do.

Where the two source implementations overlapped, the dwh one is kept: it is strictly better
(remote-bim cache skip, stale-binding recovery, schedule reconciliation, and consuming the
thread-pool iterator so a failed copy re-raises instead of being silently swallowed).

--full is off by default. The real orchestration is GitHub Actions; the Fabric notebook and
pipeline are a demo of in-Fabric scheduling, and redeploying them on every run is churn.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

REPO = Path(__file__).resolve().parent
ITEMS_DIR = REPO / "fabric_items"
SEMANTIC_DIR = REPO / "semantic_model"
SEMANTIC_MODEL = "aemo_electricity.SemanticModel"

WS = os.environ.get("FABRIC_WORKSPACE_ID", "")
FOLDER = os.environ.get("FOLDER", "aemo")

# Which backend each engine uses, and which Fabric item its semantic model binds to.
BACKEND = {
    "duckrun": "duckrun",
    "iceberg": "duckrun",
    "spark": "duckrun",
    "ducklake": "fab",
    "dwh": "fab",
}
LAKEHOUSE = {
    "duckrun": "dbt_duckrun",
    "iceberg": "dbt_iceberg",
    "ducklake": "dbt_ducklake",
    "spark": "dbt_spark",
    "dwh": "dbt_dwh",
}

# A Direct-Lake model is bound to the ITEM ID, which changes whenever the item is dropped
# and recreated. Fabric reports that as an import failure naming an object id, not as
# anything legible. Match the EXACT strings so an unrelated or transient failure never
# triggers the destructive delete-and-redeploy below.
STALE_BINDING_SIGNS = ("FailedToImportDataset", "is not found, or you do not have permission")


def log(m: str) -> None:
    print(m, flush=True)


# --------------------------------------------------------------------------- fab backend
def fab(*args: str, check: bool = True) -> str:
    r = subprocess.run(["fab", *args], capture_output=True, text=True)
    if check and r.returncode != 0:
        raise SystemExit(f"fab {' '.join(args)} failed: {r.stderr[:500]}")
    return r.stdout.strip()


def fab_deploy(item_types: list[str]) -> None:
    """`fab deploy` needs a config file. Written to a temp path and removed in a finally.

    NOTE `item_types_in_scope` is PLURAL. The singular spelling is silently ignored and
    deploys everything.
    """
    cfg = REPO / "_fab_deploy_tmp.yml"
    cfg.write_text(
        "core:\n"
        f"  workspace: {WS}\n"
        '  repository_directory: "./fabric_items"\n'
        "  item_types_in_scope: [" + ", ".join(item_types) + "]\n",
        encoding="utf-8",
    )
    try:
        fab("deploy", "-c", str(cfg))
    finally:
        cfg.unlink(missing_ok=True)


# ----------------------------------------------------------------------------- semantic
def rebind_bim(item_id: str) -> Path:
    """Point the model's OneLake URL at this engine's item.

    The committed .bim carries placeholder GUIDs. Do NOT remove the onelake.dfs URL or the
    camelCase mode token while "tidying" it — the deploy path greps the raw bytes for them
    to decide the model is Direct Lake, so losing either silently turns it into something
    deploy cannot serve.
    """
    bim = SEMANTIC_DIR / SEMANTIC_MODEL / "model.bim"
    txt = bim.read_text(encoding="utf-8-sig")
    m = re.search(r"onelake\.dfs\.fabric\.microsoft\.com/([0-9a-f-]{36})/([0-9a-f-]{36})", txt)
    if not m:
        raise SystemExit("model.bim has no onelake.dfs URL — it is no longer a Direct Lake model")
    new = txt.replace(m.group(1), WS).replace(m.group(2), item_id)
    bim.write_text(new, encoding="utf-8")
    return bim


def revert(path: Path) -> None:
    subprocess.run(["git", "checkout", "--", str(path)], cwd=REPO, capture_output=True)


# ------------------------------------------------------------------------------ deployers
def deploy_duckrun(engine: str, full: bool, with_model: bool) -> None:
    import duckrun

    ws = duckrun.workspace(WS)
    lh = LAKEHOUSE[engine]
    ws.create_lakehouse(lh, folder=FOLDER)

    # git-tracked files only (no dbt_packages/, target/, local secrets); stale remote files
    # removed; edited files replaced.
    conn = duckrun.connect(f"{ws.display_name}/{lh}.Lakehouse")
    for src in ("models", "macros", "tests", "dbt_project.yml", "profiles.yml"):
        conn.copy(src, src, git_only=True, sync=True, overwrite=True)
    log(f"copied the dbt project into {lh}/Files")

    if full:
        # _scan_item_folders validates EVERY item folder before deploying ANY of them and
        # raises on an unknown type rather than skipping, so one stray folder under
        # fabric_items/ makes this ship nothing. That is also why the semantic model lives
        # in its own directory: ws.deploy() takes no exclude filter.
        ws.deploy("fabric_items", lakehouse=lh, folder=FOLDER, overwrite=True)
        log("deployed fabric_items")

    if with_model:
        lh_id = ws.lakehouse_id(lh)
        bim = rebind_bim(lh_id)
        try:
            ws.deploy(str(SEMANTIC_DIR), name=f"aemo_{engine}", lakehouse=lh,
                      folder=FOLDER, overwrite=True)
            log(f"deployed semantic model aemo_{engine}")
        finally:
            revert(bim)

    if full:
        # Idempotent: updates the existing schedule rather than stacking duplicates.
        ws.schedule("run_pipeline", every="720m")


def deploy_fab(engine: str, full: bool, with_model: bool) -> None:
    fab("config", "set", "folder_listing_enabled", "true")
    lh = LAKEHOUSE[engine]

    if full:
        files = [p for p in (REPO / "models").rglob("*") if p.is_file()]
        files += [p for p in (REPO / "macros").rglob("*.sql")]
        dirs = sorted({p.parent.relative_to(REPO).as_posix() for p in files})
        for d in dirs:
            fab("mkdir", f"{WS}/{lh}.Lakehouse/Files/dbt/{d}", check=False)

        def cp(p: Path) -> None:
            rel = p.relative_to(REPO).as_posix()
            fab("cp", str(p), f"{WS}/{lh}.Lakehouse/Files/dbt/{rel}", "-f")

        with ThreadPoolExecutor(max_workers=8) as ex:
            # list() so a failed cp re-raises here — a lazy map silently swallows it.
            list(ex.map(cp, files))
        log(f"copied {len(files)} project files into {lh}")

        fab_deploy(["Notebook", "VariableLibrary"])
        # The notebook must build the Delta tables before a Direct Lake model can bind, so
        # this blocks. `-i '{}'` is required — without it the command does nothing at all.
        fab("job", "run", f"{WS}/run.Notebook", "-i", "{}")

    if with_model:
        item_id = fab("get", f"{WS}/{lh}.Lakehouse", "-q", "id")
        bim = rebind_bim(item_id)
        try:
            for attempt in range(3):
                r = subprocess.run(["fab", "deploy", "-c", "_fab_deploy_tmp.yml"],
                                   cwd=REPO, capture_output=True, text=True)
                out = r.stdout + r.stderr
                if r.returncode == 0:
                    break
                if any(s in out for s in STALE_BINDING_SIGNS):
                    log("stale Direct Lake binding — deleting and redeploying the model")
                    fab("rm", f"{WS}/aemo_{engine}.SemanticModel", "-f", check=False)
                    time.sleep(10)
                elif attempt == 2:
                    raise SystemExit(f"semantic model deploy failed: {out[:600]}")
                time.sleep(45)
        finally:
            revert(bim)

    if full:
        fab_deploy(["DataPipeline"])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--engine", required=True, choices=sorted(BACKEND))
    ap.add_argument("--full", action="store_true",
                    help="also deploy the notebook, pipeline and schedule (off by default)")
    ap.add_argument("--no-model", action="store_true", help="skip the semantic model")
    a = ap.parse_args()

    if not WS:
        raise SystemExit("FABRIC_WORKSPACE_ID is not set")

    backend = BACKEND[a.engine]
    log(f"deploying {a.engine} via the {backend} backend (full={a.full})")
    (deploy_duckrun if backend == "duckrun" else deploy_fab)(
        a.engine, a.full, not a.no_model
    )
    log("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
