#!/usr/bin/env python3
"""Deploy from CI: publish fabric_items/ from THIS CHECKOUT, and upload the project
to OneLake for the `run` notebook to read.

    python .github/scripts/deploy.py <dev | production>

The workspace is FABRIC_WORKSPACE_ID: set by CI from the GitHub Environment, or on a laptop
read from the repo's .env, the same file the notebooks use.

THE PRODUCTION INSTALL, into the DEV workspace or the PRODUCTION one: the same items, the
same project, a different value set. deploy.yml runs it as the GitHub Environment of the
same name, which is what holds that workspace's id. install_jumpstart.py is the demo
install. The demo needs the repo to be public twice over: Jumpstart clones it from GitHub to install, and the `ingest` notebook
downloads the dbt project from it once. Nothing here or after it fetches from GitHub, so
this works from a private copy of the repo, and with no secret: the login is OIDC.

  * THE ITEMS are published with fabric-cicd, which is what Jumpstart installs with, from
    the checkout instead of a clone.
  * THE PROJECT goes to dbt_landing/Files/project/ as a folder, file by file, so what is
    deployed can be opened and read in the lakehouse. Only what the `run` notebook uses
    (UPLOADED, below), not the repo. Its COMMIT file names the commit.
  * `repo_ref` BECOMES EMPTY because fabric-cicd activates the Variable Library value set
    named after the environment it publishes as, so `ingest` never downloads the public repo
    into this workspace, even with the folder gone. Jumpstart names no environment, so a
    demo install keeps the default. tests_py/test_fabric_items.py pins the names.

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

# The value sets in deploy_config.VariableLibrary/valueSets/; a deploy activates one.
ENVIRONMENTS = ("dev", "production")
# Where the project goes in the landing lakehouse. The `run` notebook's PROJECT is this
# folder, seen through its default lakehouse.
PROJECT = "Files/project"
# What is uploaded: the dbt project, and the lists the `run` notebook pip-installs dbt from.
UPLOADED = ["dbt_project.yml", "profiles.yml", "models", "macros", "tests", "requirements"]


def publish(environment: str) -> None:
    from azure.identity import AzureCliCredential
    from fabric_cicd import FabricWorkspace, publish_all_items

    # NOT bulk publish (fabric-cicd 1.2+): tried, and fabric-cicd falls back to one publish
    # per item, because it supports neither a Warehouse nor parameter.yml's $workspace /
    # $items variables in bulk, and this project needs both.
    with tempfile.TemporaryDirectory() as tmp:
        # The layout Jumpstart publishes from: parameter.yml at the root, the items in a
        # folder named after the logical_id, which becomes the workspace folder.
        shutil.copy(ITEMS / "parameter.yml", tmp)
        shutil.copytree(ITEMS, Path(tmp, install_jumpstart.INSTALL["logical_id"]),
                        ignore=shutil.ignore_patterns("parameter.yml"))
        publish_all_items(FabricWorkspace(
            workspace_id=os.environ["FABRIC_WORKSPACE_ID"],
            repository_directory=tmp,
            item_type_in_scope=install_jumpstart.INSTALL["items_in_scope"],
            environment=environment,
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


def dotenv() -> None:
    """A laptop's .env, KEY=value lines. What the environment already sets wins, so CI,
    which has no .env, is unaffected."""
    path = REPO / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            key, sep, value = line.partition("=")
            if sep and not key.strip().startswith("#"):
                os.environ.setdefault(key.strip(), value.strip())


def main() -> int:
    dotenv()
    if len(sys.argv) != 2 or sys.argv[1] not in ENVIRONMENTS:
        print(f"usage: deploy.py <{' | '.join(ENVIRONMENTS)}>", file=sys.stderr)
        return 2
    environment = sys.argv[1]
    publish(environment)
    missing = sorted(install_jumpstart.expected() - install_jumpstart.deployed())
    if missing:
        print("published, but not in the workspace: "
              + ", ".join(f"{n}.{t}" for n, t in missing), file=sys.stderr)
        return 1
    print(f"published {len(install_jumpstart.expected())} items as {environment}")
    upload()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
