#!/usr/bin/env python3
"""Deploy from CI: publish fabric-medallion-dbt/ from THIS CHECKOUT, and ship the project
to OneLake for run_pipeline to read.

    FABRIC_WORKSPACE_ID=<guid> python .github/scripts/deploy.py

THE PRODUCTION INSTALL; install_jumpstart.py is the demo one. The demo needs the repo to be
public twice over: Jumpstart clones it from GitHub to install, and run.Notebook downloads it
from GitHub on every run. Nothing here or after it fetches from GitHub, so this works from a
private copy of the repo, and with no secret: the login is the OIDC one every other workflow
uses.

  * THE ITEMS are published with fabric-cicd, which is what Jumpstart installs with, from
    the checkout instead of a clone.
  * THE PROJECT goes to dbt_landing/Files/project/ as one zip, the shape GitHub's archive
    has, so run.Notebook unpacks either the same way.
  * `project_source` BECOMES `onelake` because fabric-cicd activates the Variable Library
    value set named after the environment it publishes as. Jumpstart names none, so a demo
    install keeps the default, `github`. tests_py/test_fabric_items.py pins the names.

A change reaches the workspace at the next deploy, not at the next run: the run is of the
commit that was deployed. Do not deploy while run_pipeline is running; each step of a run
fetches the project again.

Needs `az login` (azure/login on CI).
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import install_jumpstart

REPO = install_jumpstart.REPO
ITEMS = install_jumpstart.ITEMS

# The value set in deploy_config.VariableLibrary/valueSets/ that this deploy activates.
ENVIRONMENT = "production"
# Where run.Notebook looks, relative to the landing lakehouse. Its PROJECT_ZIP is the same.
PROJECT_ZIP = "Files/project/fabric-medallion-dbt.zip"


def publish() -> None:
    from azure.identity import AzureCliCredential
    from fabric_cicd import FabricWorkspace, publish_all_items

    with tempfile.TemporaryDirectory() as tmp:
        # The layout Jumpstart publishes from: parameter.yml at the root, the items in a
        # folder named after the logical_id, which becomes the workspace folder.
        shutil.copy(ITEMS / "parameter.yml", tmp)
        shutil.copytree(ITEMS, Path(tmp, ITEMS.name),
                        ignore=shutil.ignore_patterns("parameter.yml"))
        publish_all_items(FabricWorkspace(
            workspace_id=os.environ["FABRIC_WORKSPACE_ID"],
            repository_directory=tmp,
            item_type_in_scope=install_jumpstart.INSTALL["items_in_scope"],
            environment=ENVIRONMENT,
            token_credential=AzureCliCredential(),
        ))


def ship() -> None:
    """HEAD as one zip, into the landing lakehouse."""
    import provision  # reads FABRIC_WORKSPACE_ID at import

    sys.path.insert(0, str(REPO / "ingest"))
    from onelake import Store

    landing = provision.find("lakehouses", provision.LANDING_LAKEHOUSE)
    if not landing:
        raise SystemExit(f"{provision.LANDING_LAKEHOUSE} is not in the workspace after the publish")
    section, folder, name = PROJECT_ZIP.split("/")
    with tempfile.TemporaryDirectory() as tmp:
        # git archive writes the commit into the zip comment, which run.Notebook prints.
        subprocess.run(["git", "archive", "--format=zip", f"--prefix={ITEMS.name}/",
                        "-o", str(Path(tmp, name)), "HEAD"], cwd=REPO, check=True)
        Store(provision.abfss(landing, section)).push(tmp, folder, overwrite=True)
    commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, check=True,
                            capture_output=True, text=True).stdout.strip()
    print(f"shipped {commit} to {provision.LANDING_LAKEHOUSE}/{PROJECT_ZIP}")


def main() -> int:
    publish()
    missing = sorted(install_jumpstart.expected() - install_jumpstart.deployed())
    if missing:
        print("published, but not in the workspace: "
              + ", ".join(f"{n}.{t}" for n, t in missing), file=sys.stderr)
        return 1
    print(f"published {len(install_jumpstart.expected())} items as {ENVIRONMENT}")
    ship()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
