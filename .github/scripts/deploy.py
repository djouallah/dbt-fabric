#!/usr/bin/env python3
"""Deploy from CI: publish fabric-medallion-dbt/ from THIS CHECKOUT, and upload the project
to OneLake for the `run` notebook to read.

    FABRIC_WORKSPACE_ID=<guid> python .github/scripts/deploy.py

THE PRODUCTION INSTALL; install_jumpstart.py is the demo one. The demo needs the repo to be
public twice over: Jumpstart clones it from GitHub to install, and the `run` notebook
downloads it from GitHub on every run. Nothing here or after it fetches from GitHub, so this
works from a private copy of the repo, and with no secret: the login is OIDC.

  * THE ITEMS are published with fabric-cicd, which is what Jumpstart installs with, from
    the checkout instead of a clone.
  * THE PROJECT goes to dbt_landing/Files/project/ as a folder, file by file, so what is
    deployed can be opened and read in the lakehouse. Only what the `run` notebook uses
    (UPLOADED, below), not the repo. Its COMMIT file names the commit.
  * `project_source` BECOMES `onelake` because fabric-cicd activates the Variable Library
    value set named after the environment it publishes as. Jumpstart names none, so a demo
    install keeps the default, `github`. tests_py/test_fabric_items.py pins the names.

A change reaches the workspace at the next deploy, not at the next run. Do not deploy while
run_pipeline is running: the two engines would build from two commits.

Needs `az login` (azure/login on CI).
"""
from __future__ import annotations

import io
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

import install_jumpstart

REPO = install_jumpstart.REPO
ITEMS = install_jumpstart.ITEMS

# The value set in deploy_config.VariableLibrary/valueSets/ that this deploy activates.
ENVIRONMENT = "production"
# Where the project goes in the landing lakehouse. The `run` notebook's PROJECT is this
# folder, seen through its default lakehouse.
PROJECT = "Files/project"
# What is uploaded: the dbt project, and the two things the `run` notebook needs beside it.
UPLOADED = [
    "dbt_project.yml", "profiles.yml", "models", "macros", "tests",
    "requirements",                     # what the notebook pip-installs dbt from
    ".github/scripts/provision.py",     # gives the dbt profile the warehouse and lakehouse
]


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


def upload() -> None:
    """UPLOADED, as of HEAD, file by file, into the landing lakehouse.

    HEAD AND NOT THE WORKING TREE, so a deploy from a laptop and one from CI upload the same
    files. THE FOLDER IS DELETED FIRST: a model removed from the repo must not survive in
    OneLake, where dbt would still build it."""
    import provision  # reads FABRIC_WORKSPACE_ID at import
    from azure.identity import AzureCliCredential
    from azure.storage.filedatalake import DataLakeServiceClient

    landing = provision.find("lakehouses", provision.LANDING_LAKEHOUSE)
    if not landing:
        raise SystemExit(f"{provision.LANDING_LAKEHOUSE} is not in the workspace after the publish")
    commit = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO, check=True,
                            capture_output=True, text=True).stdout.strip()
    archive = zipfile.ZipFile(io.BytesIO(subprocess.run(
        ["git", "archive", "--format=zip", "HEAD", "--", *UPLOADED], cwd=REPO, check=True,
        capture_output=True).stdout))

    onelake = DataLakeServiceClient("https://onelake.dfs.fabric.microsoft.com",
                                    credential=AzureCliCredential())
    workspace = onelake.get_file_system_client(provision.WS)
    folder = workspace.get_directory_client(f"{landing}/{PROJECT}")
    if folder.exists():
        folder.delete_directory()
    files = {n: archive.read(n) for n in archive.namelist() if not n.endswith("/")}
    files["COMMIT"] = f"{commit}\n".encode()
    for name, data in files.items():
        workspace.get_file_client(f"{landing}/{PROJECT}/{name}").upload_data(data, overwrite=True)
    print(f"uploaded {len(files)} files of {commit} to {provision.LANDING_LAKEHOUSE}/{PROJECT}")


def main() -> int:
    publish()
    missing = sorted(install_jumpstart.expected() - install_jumpstart.deployed())
    if missing:
        print("published, but not in the workspace: "
              + ", ".join(f"{n}.{t}" for n, t in missing), file=sys.stderr)
        return 1
    print(f"published {len(install_jumpstart.expected())} items as {ENVIRONMENT}")
    upload()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
