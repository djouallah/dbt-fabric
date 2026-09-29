"""Pin the Fabric items against each other and against the scripts they run.

fabric_items/ is what Fabric Jumpstart installs, and nothing parses it before a user does.
Every check here is a way it has gone wrong, or would, with the install still green: a
notebook reading a variable the library does not declare, a pipeline pointing at no
notebook, a leg that lands.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import json
import re
import sys

from _layout import ENGINES, REPO

ITEMS = REPO / "fabric_items"
NOTEBOOK = ITEMS / "run.Notebook" / "notebook-content.ipynb"
VARIABLES = ITEMS / "deploy_config.VariableLibrary" / "variables.json"
PIPELINE = ITEMS / "run_pipeline.DataPipeline" / "pipeline-content.json"

sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / ".github" / "scripts"))
import fabric_run  # noqa: E402
import install_jumpstart  # noqa: E402


def _platform(folder) -> dict:
    return json.loads((folder / ".platform").read_text(encoding="utf-8"))


def _cells() -> list[dict]:
    return json.loads(NOTEBOOK.read_text(encoding="utf-8"))["cells"]


def _notebook_source() -> str:
    return "\n".join("".join(c["source"]) for c in _cells() if c["cell_type"] == "code")


def _activities() -> dict[str, dict]:
    doc = json.loads(PIPELINE.read_text(encoding="utf-8"))
    return {a["name"]: a for a in doc["properties"]["activities"]}


def test_every_item_folder_is_named_after_its_platform_file():
    """Jumpstart and fabric-cicd read the name and type from the FOLDER in some places and
    from .platform in others."""
    folders = [p for p in ITEMS.iterdir() if p.is_dir()]
    assert folders, "fabric_items/ holds no item folders"
    for folder in folders:
        meta = _platform(folder)["metadata"]
        assert folder.name == f"{meta['displayName']}.{meta['type']}"


def test_logical_ids_are_unique():
    ids = [_platform(p)["config"]["logicalId"] for p in ITEMS.iterdir() if p.is_dir()]
    assert len(ids) == len(set(ids))


def test_the_install_covers_every_item_type():
    types = {p.name.rsplit(".", 1)[-1] for p in ITEMS.iterdir() if p.is_dir()}
    assert types == set(install_jumpstart.INSTALL["items_in_scope"]), (
        "an item type that is not in items_in_scope is skipped by the install, silently"
    )
    assert install_jumpstart.INSTALL["entry_point"] in {p.name for p in ITEMS.iterdir()}


def test_lakehouses_are_schema_enabled():
    """Both engines write <engine>_landing and <engine>_mart. fabric-cicd creates a lakehouse
    with schemas only when lakehouse.metadata.json names a defaultSchema."""
    lakehouses = sorted(ITEMS.glob("*.Lakehouse"))
    assert len(lakehouses) == 2
    for lh in lakehouses:
        meta = json.loads((lh / "lakehouse.metadata.json").read_text(encoding="utf-8"))
        assert "defaultSchema" in meta, lh.name


def test_notebook_cell_sources_are_arrays_of_lines():
    for c in _cells():
        assert isinstance(c["source"], list) and all(isinstance(s, str) for s in c["source"])


def test_notebook_variables_match_the_library():
    """Every library variable the notebook reads is declared, and none is declared for nothing."""
    src = _notebook_source()
    read = set(re.findall(r"\bvl\.(\w+)", src))
    for names in re.findall(r"for k in \((.*?)\):\s*\n\s*os\.environ\[k\] = getattr\(vl, k\)",
                            src, re.S):
        read |= set(re.findall(r"[\"'](\w+)[\"']", names))
    declared = {v["name"] for v in json.loads(VARIABLES.read_text(encoding="utf-8"))["variables"]}
    assert read == declared, f"notebook reads {sorted(read)}, library declares {sorted(declared)}"


def test_the_notebook_takes_its_step_from_a_parameters_cell():
    tagged = [c for c in _cells() if "parameters" in c.get("metadata", {}).get("tags", [])]
    assert len(tagged) == 1, "exactly one cell is the parameters cell"
    src = "".join(tagged[0]["source"])
    assert re.search(r"^step = ", src, re.M) and re.search(r"^run_id = ", src, re.M)


def test_pipeline_runs_the_notebook_in_this_folder():
    notebook = _platform(NOTEBOOK.parent)["config"]["logicalId"]
    for name, a in _activities().items():
        props = a["typeProperties"]
        assert props["notebookId"] == notebook, name
        # All zeros is "the workspace being installed into"; a real GUID is somebody else's.
        assert props["workspaceId"] == "00000000-0000-0000-0000-000000000000", name


def test_pipeline_steps_are_the_ones_fabric_run_knows():
    steps = {name: a["typeProperties"]["parameters"]["step"]["value"]
             for name, a in _activities().items()}
    assert set(steps.values()) == set(fabric_run.STEPS)
    assert set(fabric_run.ENGINES) == set(ENGINES)
    for name, a in _activities().items():
        run_id = a["typeProperties"]["parameters"]["run_id"]["value"]
        assert run_id == {"value": "@pipeline().RunId", "type": "Expression"}, (
            f"{name}: parity compares the fingerprints of ONE run, found by this id"
        )


def test_pipeline_lands_once_then_fans_out():
    acts = _activities()

    def needs(name):
        assert all(d["dependencyConditions"] == ["Succeeded"] for d in acts[name]["dependsOn"])
        return {d["activity"] for d in acts[name]["dependsOn"]}

    assert needs("land") == set()
    for engine in ENGINES:
        assert needs(engine) == {"land"}
    assert needs("parity") == set(ENGINES)


def test_the_legs_do_not_land():
    """download_aemo.py rewrites csv_raw_archive_log.parquet in place, so legs landing at
    once race on that one file, and the engines would be compared on different inputs."""
    import inspect

    assert "download_aemo" in inspect.getsource(fabric_run.land)
    for fn in (fabric_run.build, fabric_run.parity):
        assert "download_aemo" not in inspect.getsource(fn), fn.__name__


def test_fabric_run_is_not_a_dbt_plugin():
    """dbt imports every importable module named dbt_* when a command starts, and the repo
    root is the project directory."""
    assert not [p.name for p in REPO.glob("dbt_*.py")]
