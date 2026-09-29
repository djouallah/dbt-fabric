"""Pin the Fabric items against each other and against the scripts they run.

fabric-medallion-dbt/ is what Fabric Jumpstart installs and what deploy.py publishes, and
nothing parses it before a user does. Every check here is a way it has gone wrong, or would,
with the install still green: a notebook reading a variable the library does not declare, a
pipeline pointing at no notebook, a leg that lands, a deploy that leaves the run on GitHub.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import ast
import json
import re
import sys

from _layout import ENGINES, REPO, patch_dir

ITEMS = REPO / "fabric-medallion-dbt"
NOTEBOOK = ITEMS / "run.Notebook" / "notebook-content.ipynb"
LIBRARY = ITEMS / "deploy_config.VariableLibrary"
VARIABLES = LIBRARY / "variables.json"
PIPELINE = ITEMS / "run_pipeline.DataPipeline" / "pipeline-content.json"

sys.path.insert(0, str(REPO / ".github" / "scripts"))
import deploy  # noqa: E402
import install_jumpstart  # noqa: E402


def _platform(folder) -> dict:
    return json.loads((folder / ".platform").read_text(encoding="utf-8"))


def _cells() -> list[dict]:
    return json.loads(NOTEBOOK.read_text(encoding="utf-8"))["cells"]


def _notebook_source() -> str:
    return "\n".join("".join(c["source"]) for c in _cells() if c["cell_type"] == "code")


def _code_cells() -> list[str]:
    """Each code cell with its comments dropped, so a rule quoted in one is not mistaken for
    the code it describes."""
    return ["".join(s for s in c["source"] if not s.lstrip().startswith("#"))
            for c in _cells() if c["cell_type"] == "code"]


def _constant(name: str):
    """A literal the notebook assigns at the top level, e.g. ENGINES."""
    m = re.search(rf"^{name} = (.+)$", _notebook_source(), re.M)
    assert m, f"the notebook assigns no {name}"
    return ast.literal_eval(m.group(1))


def _activities() -> dict[str, dict]:
    doc = json.loads(PIPELINE.read_text(encoding="utf-8"))
    return {a["name"]: a for a in doc["properties"]["activities"]}


def test_every_item_folder_is_named_after_its_platform_file():
    """Jumpstart and fabric-cicd read the name and type from the FOLDER in some places and
    from .platform in others."""
    folders = [p for p in ITEMS.iterdir() if p.is_dir()]
    assert folders, "fabric-medallion-dbt/ holds no item folders"
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


def test_pipeline_steps_are_the_ones_the_notebook_runs():
    steps = {name: a["typeProperties"]["parameters"]["step"]["value"]
             for name, a in _activities().items()}
    assert set(_constant("ENGINES")) == set(ENGINES)
    assert 'STEPS = ("land", *ENGINES, "parity")' in _notebook_source()
    assert set(steps.values()) == {"land", *ENGINES, "parity"}
    # One cell per kind of step, and every one of them guarded by the step it is.
    for guard in ('if step == "land":', "if step in ENGINES:", 'if step == "parity":'):
        assert sum(guard in cell for cell in _code_cells()) == 1, guard
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
    landing = [cell for cell in _code_cells() if "download_aemo" in cell]
    assert len(landing) == 1, "exactly one cell lands"
    assert landing[0].startswith('if step == "land":\n')
    assert "step in ENGINES" not in landing[0] and '"parity"' not in landing[0]


def test_the_notebook_installs_each_steps_own_requirements():
    """dbt is pip-installed by the notebook, from the lists CI installs from. One adapter
    per step: dbt-fabric and dbt-fabricspark cannot share an environment."""
    m = re.search(r"^requirements = (\{.*?\})\.get\(step\)$", _notebook_source(), re.M)
    assert m, "the notebook no longer names what each step installs"
    needs = ast.literal_eval(m.group(1))
    assert set(needs) == {"land", *ENGINES, "parity"}
    for engine in ENGINES:
        assert needs[engine] == engine
    for name in set(needs.values()):
        assert (REPO / "requirements" / f"{name}.txt").is_file(), name


def test_the_notebook_fetches_from_github_or_onelake_and_nowhere_else():
    """No fallback to GitHub: in a private copy of the repo that would run the public
    repo's code against the workspace."""
    fetch = [cell for cell in _code_cells() if "vl.project_source" in cell]
    assert len(fetch) == 1
    branches = re.findall(r"^(?:if|elif) vl\.project_source == \"(\w+)\":$", fetch[0], re.M)
    assert branches == ["github", "onelake"]
    assert re.search(r"^else:\n    raise ValueError", fetch[0], re.M)
    default = {v["name"]: v["value"]
               for v in json.loads(VARIABLES.read_text(encoding="utf-8"))["variables"]}
    assert default["project_source"] == "github", "a Jumpstart install runs from the public repo"


def test_a_deploy_switches_the_run_to_onelake():
    """fabric-cicd activates the value set named after the environment it publishes as, and
    deploy.py publishes as ENVIRONMENT. The three names are in three files."""
    declared = {v["name"] for v in json.loads(VARIABLES.read_text(encoding="utf-8"))["variables"]}
    sets = {p.stem: json.loads(p.read_text(encoding="utf-8"))
            for p in (LIBRARY / "valueSets").glob("*.json")}
    assert set(sets) == {deploy.ENVIRONMENT}
    order = json.loads((LIBRARY / "settings.json").read_text(encoding="utf-8"))["valueSetsOrder"]
    assert order == [deploy.ENVIRONMENT]
    for name, doc in sets.items():
        assert doc["name"] == name, "the file name must be the value set's name"
        overrides = {o["name"]: o["value"] for o in doc["variableOverrides"]}
        assert set(overrides) <= declared
        assert overrides == {"project_source": "onelake"}


def test_the_deploy_ships_the_zip_the_notebook_reads():
    assert _constant("PROJECT_ZIP") == deploy.PROJECT_ZIP
    assert deploy.PROJECT_ZIP.split("/")[0] == "Files"
    # The zip's one top-level folder; the notebook takes the project folder's name from it.
    assert deploy.ITEMS.name == install_jumpstart.INSTALL["logical_id"]


def _bim(engine: str) -> str:
    return (ITEMS / f"aemo_{engine}.SemanticModel" / "model.bim").read_text(encoding="utf-8-sig")


def _rebound(engine: str) -> str:
    """model.bim as fabric-cicd leaves it: every find_replace whose filters match this model,
    applied in file order. The `$...` values are resolved at install, so they stay as written."""
    import yaml

    text = _bim(engine)
    doc = yaml.safe_load((ITEMS / "parameter.yml").read_text(encoding="utf-8"))
    for rule in doc["find_replace"]:
        assert set(rule["replace_value"]) == {"_ALL_"}, "Jumpstart installs with no environment"
        if rule.get("item_type", "SemanticModel") != "SemanticModel":
            continue
        if rule.get("item_name", f"aemo_{engine}") != f"aemo_{engine}":
            continue
        assert rule["find_value"] in text, f"{rule['find_value']!r} matches nothing in model.bim"
        text = text.replace(rule["find_value"], rule["replace_value"]["_ALL_"])
    return text


def test_the_two_models_are_one_model():
    """One gold layer: the engines' models differ in what parameter.yml rewrites and in
    nothing else. A measure added to one copy only is a divergence no engine test sees."""
    assert _bim("dwh") == _bim("spark")
    assert set(ENGINES) == {p.name[len("aemo_"):-len(".SemanticModel")]
                            for p in ITEMS.glob("*.SemanticModel")}


def test_parameter_yml_rebinds_every_placeholder():
    """Direct Lake has no schema, workspace or item parameter. A placeholder that survives
    the install is a model bound to a schema no engine writes, or to Desktop's workspace."""
    item = {"dwh": "$items.Warehouse.dbt_dwh.$id", "spark": "$items.Lakehouse.dbt.$id"}
    for engine in ENGINES:
        out = _rebound(engine)
        assert not re.search(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", out)
        assert f"onelake.dfs.fabric.microsoft.com/$workspace.$id/{item[engine]}" in out
        for table in json.loads(_bim(engine))["model"]["tables"]:
            assert f'"sourceLineageTag": "[{engine}_mart].[{table["name"]}]"' in out
        schemas = set(re.findall(r'"schemaName": "(\w+)"', out))
        assert schemas == {f"{engine}_mart"}, schemas


def test_parameter_yml_names_items_that_exist():
    import yaml

    doc = yaml.safe_load((ITEMS / "parameter.yml").read_text(encoding="utf-8"))
    folders = {p.name for p in ITEMS.iterdir() if p.is_dir()}
    for rule in doc["find_replace"]:
        if "item_name" in rule:
            assert f"{rule['item_name']}.{rule['item_type']}" in folders
        m = re.fullmatch(r"\$items\.(\w+)\.(\w+)\.\$id", rule["replace_value"]["_ALL_"])
        if m:
            assert f"{m.group(2)}.{m.group(1)}" in folders


def _documented_columns() -> dict[str, set[str]]:
    import yaml

    cols: dict[str, set[str]] = {}
    for f in ("_marts.yml", "_dimensions.yml"):
        doc = yaml.safe_load((patch_dir("dwh") / f).read_text(encoding="utf-8"))
        for m in doc.get("models", []):
            cols[m["name"]] = {c["name"] for c in m.get("columns", [])}
    return cols


def test_bim_matches_the_documented_mart():
    """A Direct Lake model that binds a missing column does not fail the install with a
    useful message; it fails the refresh, minutes later. Check the three ways the bim goes
    stale against the model YML both engines are tested against: a column that does not
    exist, a relationship endpoint that was never added, DAX naming a dropped column."""
    model = json.loads(_bim("dwh"))["model"]
    documented = _documented_columns()
    bim_cols = {t["name"]: {c["name"] for c in t.get("columns", [])} for t in model["tables"]}
    measures = {m["name"].lower() for t in model["tables"] for m in t.get("measures", [])}
    problems = []
    for t in model["tables"]:
        assert t["name"] in documented, f"{t['name']} is not a documented model"
        for c in t.get("columns", []):
            src = c.get("sourceColumn", c["name"])
            if src not in documented[t["name"]]:
                problems.append(f"{t['name']}.{src}: not a documented column of {t['name']}")
        for m in t.get("measures", []):
            expression = m["expression"]
            if isinstance(expression, list):
                expression = "\n".join(expression)
            for table, column in re.findall(r"(\w+)\[([^\]]+)\]", expression):
                known = {x.lower() for x in bim_cols.get(table, set())} | measures
                if column.lower() not in known:
                    problems.append(f"measure {m['name']}: {table}[{column}] undefined")
    for r in model.get("relationships", []):
        for side in ("from", "to"):
            table, column = r[f"{side}Table"], r[f"{side}Column"]
            if column not in bim_cols.get(table, set()):
                problems.append(f"relationship {r.get('name', '?')}: {table}[{column}] undefined")
    assert not problems, "semantic model is stale against the mart:\n  " + "\n  ".join(problems)


def test_the_repo_root_holds_no_dbt_plugin():
    """dbt imports every importable module named dbt_* when a command starts, and the repo
    root is the project directory."""
    assert not [p.name for p in REPO.glob("dbt_*.py")]


def test_the_items_folder_is_the_default_workspace_path():
    """Jumpstart looks in `<logical_id>/` when no workspace_path is given, which is what keeps
    that argument out of the snippet in README.md. A missing folder is not an error: the
    install falls back to the repo root and never finds parameter.yml."""
    assert "workspace_path" not in install_jumpstart.INSTALL
    assert ITEMS.name == install_jumpstart.INSTALL["logical_id"]
    assert (ITEMS / "parameter.yml").is_file()
