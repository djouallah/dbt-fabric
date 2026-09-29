#!/usr/bin/env python3
"""Install fabric-medallion-dbt/ into a workspace with Fabric Jumpstart, then check what landed.

    FABRIC_WORKSPACE_ID=<guid> python .github/scripts/install_jumpstart.py <branch or tag>

THE CALL A USER PASTES INTO A FABRIC NOTEBOOK (README.md), plus what a runner has to add:
the workspace, which a notebook knows by itself, and `unattended`, because there is no cell
to render into. install.yml runs it so an install is proven before anybody is told to try it.

`_install_from_github` is Jumpstart's own route for a project that is not in its catalog
yet. It runs the same install as `jumpstart.install(<id>)`; only the lookup differs.

`update_existing` because the test workspace already holds dbt_landing, dbt and dbt_dwh. A
lakehouse or a warehouse is published as a shell, so updating one leaves its data alone.

Needs `az login` (azure/login on CI) and FABRIC_JUMPSTART_TOKEN_CREDENTIAL=AzureCliCredential.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
# Named after the logical_id: that is where Jumpstart looks when no workspace_path is given.
ITEMS = REPO / "fabric-medallion-dbt"

INSTALL = {
    "logical_id": "fabric-medallion-dbt",
    "repo_url": "https://github.com/djouallah/fabric-medallion-dbt",
    "entry_point": "run_pipeline.DataPipeline",
    "items_in_scope": ["VariableLibrary", "Lakehouse", "Warehouse", "Notebook", "SemanticModel",
                       "DataPipeline"],
}


def expected() -> set[tuple[str, str]]:
    """(name, type) of every item folder in scope -- the folder name is `<name>.<Type>`."""
    return {tuple(p.name.rsplit(".", 1)) for p in ITEMS.iterdir()
            if p.is_dir() and p.name.rsplit(".", 1)[-1] in INSTALL["items_in_scope"]}


def deployed() -> set[tuple[str, str]]:
    import provision  # reads FABRIC_WORKSPACE_ID at import

    found, path = set(), f"workspaces/{provision.WS}/items"
    while path:
        r = provision.req("GET", path)
        r.raise_for_status()
        body = r.json()
        found |= {(i["displayName"], i["type"]) for i in body.get("value", [])}
        token = body.get("continuationToken")
        path = f"workspaces/{provision.WS}/items?continuationToken={token}" if token else None
    return found


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: install_jumpstart.py <branch or tag>", file=sys.stderr)
        return 2
    import fabric_jumpstart as jumpstart

    jumpstart._install_from_github(
        **INSTALL,
        repo_ref=argv[1],
        workspace_id=os.environ["FABRIC_WORKSPACE_ID"],
        unattended=True,
        update_existing=True,
    )

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    missing = sorted(expected() - deployed())
    if missing:
        print("installed, but not in the workspace: "
              + ", ".join(f"{n}.{t}" for n, t in missing), file=sys.stderr)
        return 1
    print(f"installed {len(expected())} items: "
          + ", ".join(f"{n}.{t}" for n, t in sorted(expected())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
