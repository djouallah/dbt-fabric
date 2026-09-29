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

import pytest
import yaml

from _layout import ENGINES, REPO, patch_dir

ITEMS = REPO / "fabric-medallion-dbt"
# The three steps of a run, each its own notebook: land the files, build one engine, compare.
NOTEBOOKS = ("ingest", "run", "parity")
LIBRARY = ITEMS / "deploy_config.VariableLibrary"
VARIABLES = LIBRARY / "variables.json"
PIPELINE = ITEMS / "run_pipeline.DataPipeline" / "pipeline-content.json"
WORKFLOWS = REPO / ".github" / "workflows"
# "The workspace being installed into"; fabric-cicd swaps it for the real one.
THIS_WORKSPACE = "00000000-0000-0000-0000-000000000000"

sys.path.insert(0, str(REPO / ".github" / "scripts"))
import deploy  # noqa: E402
import install_jumpstart  # noqa: E402


def _platform(folder) -> dict:
    return json.loads((folder / ".platform").read_text(encoding="utf-8"))


def _notebook(name: str) -> dict:
    path = ITEMS / f"{name}.Notebook" / "notebook-content.ipynb"
    return json.loads(path.read_text(encoding="utf-8"))


def _source(name: str) -> str:
    return "\n".join("".join(c["source"]) for c in _notebook(name)["cells"]
                     if c["cell_type"] == "code")


def _code(name: str) -> str:
    """The notebook's code with its comments dropped, so a rule quoted in a comment is not
    mistaken for the code it describes."""
    return "\n".join(line for line in _source(name).splitlines()
                     if not line.lstrip().startswith("#"))


def _constant(notebook: str, name: str):
    """A literal the notebook assigns at the top level, e.g. ENGINES."""
    m = re.search(rf"^{name} = (.+)$", _source(notebook), re.M)
    assert m, f"the {notebook} notebook assigns no {name}"
    return ast.literal_eval(m.group(1))


def _parameters(name: str) -> set[str]:
    """What the notebook's parameters cell assigns; empty when it has no such cell."""
    tagged = [c for c in _notebook(name)["cells"]
              if "parameters" in c.get("metadata", {}).get("tags", [])]
    assert len(tagged) <= 1, f"{name}: more than one parameters cell"
    return set(re.findall(r"^(\w+) = ", "".join(tagged[0]["source"]), re.M)) if tagged else set()


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


def test_the_notebooks_are_the_three_steps():
    assert {p.name for p in ITEMS.glob("*.Notebook")} == {f"{n}.Notebook" for n in NOTEBOOKS}


@pytest.mark.parametrize("name", NOTEBOOKS)
def test_notebook_cell_sources_are_arrays_of_lines(name):
    for c in _notebook(name)["cells"]:
        assert isinstance(c["source"], list) and all(isinstance(s, str) for s in c["source"])
        if c["cell_type"] == "code":
            compile("".join(c["source"]), name, "exec")


@pytest.mark.parametrize("name", NOTEBOOKS)
def test_every_notebook_has_the_landing_lakehouse_as_its_default(name):
    """Every path in the notebooks is under /lakehouse/default/Files. The binding is the
    lakehouse's logicalId and the all-zero workspace: fabric-cicd swaps both at install, the
    way it does for the notebook a pipeline activity names."""
    landing = _platform(ITEMS / "dbt_landing.Lakehouse")["config"]["logicalId"]
    lakehouse = _notebook(name)["metadata"]["dependencies"]["lakehouse"]
    assert lakehouse["default_lakehouse"] == landing
    assert lakehouse["default_lakehouse_name"] == "dbt_landing"
    assert lakehouse["default_lakehouse_workspace_id"] == THIS_WORKSPACE
    assert lakehouse["known_lakehouses"] == [{"id": landing}]


def test_notebook_variables_match_the_library():
    """Every library variable a notebook reads is declared, and none is declared for nothing."""
    read = {v for name in NOTEBOOKS for v in re.findall(r"\bvl\.(\w+)", _source(name))}
    declared = {v["name"] for v in json.loads(VARIABLES.read_text(encoding="utf-8"))["variables"]}
    assert read == declared, f"notebooks read {sorted(read)}, library declares {sorted(declared)}"


def test_the_pipeline_is_ingest_then_the_engines_then_parity():
    acts = _activities()

    def needs(name):
        assert all(d["dependencyConditions"] == ["Succeeded"] for d in acts[name]["dependsOn"])
        return {d["activity"] for d in acts[name]["dependsOn"]}

    assert set(acts) == {"ingest", *ENGINES, "parity"}
    assert needs("ingest") == set()
    for engine in ENGINES:
        assert needs(engine) == {"ingest"}
    assert needs("parity") == set(ENGINES)


def test_every_activity_runs_a_notebook_in_this_folder_with_its_parameters():
    notebook_of = {"ingest": "ingest", "parity": "parity", **{e: "run" for e in ENGINES}}
    for name, a in _activities().items():
        props = a["typeProperties"]
        notebook = notebook_of[name]
        folder = ITEMS / f"{notebook}.Notebook"
        assert props["notebookId"] == _platform(folder)["config"]["logicalId"], name
        assert props["workspaceId"] == THIS_WORKSPACE, name
        passed = props.get("parameters", {})
        assert set(passed) == _parameters(notebook), (
            f"{name} passes {sorted(passed)}, {notebook} takes {sorted(_parameters(notebook))}"
        )
        if "engine" in passed:
            assert passed["engine"]["value"] == name
        if "run_id" in passed:
            assert passed["run_id"]["value"] == {"value": "@pipeline().RunId", "type": "Expression"}, (
                f"{name}: parity compares the fingerprints of ONE run, found by this id"
            )
    assert set(_constant("run", "ENGINES")) == set(ENGINES)
    assert set(_constant("parity", "ENGINES")) == set(ENGINES)


def test_only_ingest_lands():
    """The ingest notebook rewrites csv_raw_archive_log.parquet in place, so two steps
    landing at once would race on that one file, and the engines would be compared on
    different inputs."""
    assert "nemweb" in _code("ingest") and "csv_raw" in _code("ingest")
    for name in ("run", "parity"):
        assert "nemweb" not in _code(name) and "csv_raw" not in _code(name), name


def test_the_run_notebook_installs_the_engines_own_requirements():
    """dbt is pip-installed by the notebook, from the list CI tests the project with. One
    adapter per session: dbt-fabric and dbt-fabricspark cannot share an environment."""
    assert '"pip", "install", "-q", "-r", f"requirements/{engine}.txt"' in _code("run")
    for engine in ENGINES:
        assert (REPO / "requirements" / f"{engine}.txt").is_file(), engine
    for name in ("ingest", "parity"):
        assert "pip" not in _code(name), f"{name} installs nothing"


def test_the_run_notebook_fetches_from_github_or_onelake_and_nowhere_else():
    """No fallback to GitHub: in a private copy of the repo that would run the public
    repo's code against the workspace."""
    code = _code("run")
    branches = re.findall(r"^(?:if|elif) vl\.project_source == \"(\w+)\":$", code, re.M)
    assert branches == ["github", "onelake"]
    assert re.search(r"^else:\n    raise ValueError", code, re.M)
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


def test_the_deploy_uploads_the_folder_the_run_notebook_copies():
    assert _constant("run", "PROJECT") == f"/lakehouse/default/{deploy.PROJECT}"
    assert 'Path(repo, "COMMIT")' in _code("run"), "the run prints the commit it builds"
    assert '"COMMIT"' in (REPO / ".github" / "scripts" / "deploy.py").read_text(encoding="utf-8")


def test_only_ci_runs_on_push():
    """install.yml and deploy.yml create Fabric items and decide what a workspace runs; a
    push must not do that by itself."""
    for wf in sorted(WORKFLOWS.glob("*.yml")):
        doc = yaml.safe_load(wf.read_text(encoding="utf-8"))
        # PyYAML parses the bare key `on` as the boolean True.
        on = doc[True] if True in doc else doc["on"]
        triggers = {on} if isinstance(on, str) else set(on)
        if wf.name != "ci.yml":
            assert "push" not in triggers, f"{wf.name} must not run on push"


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
