"""Pin the in-Fabric demo items against each other and against deploy.py.

The demo notebook reads its configuration from the deploy_config Variable Library and runs
the CI scripts; deploy.py rewrites the semantic model's schema per engine because Direct
Lake has no schema parameter. Both drifted silently once (a target named `dev`, a library
missing the variables the notebook read, a model bound to a schema no engine writes).

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

from _layout import REPO, patch_dir
NOTEBOOK = REPO / "fabric_items" / "run.Notebook" / "notebook-content.ipynb"
VARIABLES = REPO / "fabric_items" / "deploy_config.VariableLibrary" / "variables.json"
PIPELINE = REPO / "fabric_items" / "run_pipeline.DataPipeline" / "pipeline-content.json"
BIM = REPO / "semantic_model" / "aemo_electricity.SemanticModel" / "model.bim"

sys.path.insert(0, str(REPO))
import deploy  # noqa: E402


def _notebook_source() -> str:
    nb = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    return "".join("".join(c["source"]) for c in nb["cells"])


def test_notebook_cell_sources_are_arrays_of_lines():
    """AGENTS.md: keep each cell's `source` as an array of lines, never one string."""
    nb = json.loads(NOTEBOOK.read_text(encoding="utf-8"))
    for c in nb["cells"]:
        assert isinstance(c["source"], list) and all(isinstance(s, str) for s in c["source"]), (
            f"cell {c.get('id')} has a {type(c['source']).__name__} source"
        )


def test_notebook_variables_match_the_library():
    """Every library variable the notebook reads is declared, and none is declared for nothing."""
    src = _notebook_source()
    read = set(re.findall(r"\bvl\.(\w+)", src))
    for m in re.finditer(r'for k in \((.*?)\):\s*\n\s*os\.environ\[k\] = getattr\(vl, k\)', src, re.S):
        read |= set(re.findall(r"'(\w+)'|\"(\w+)\"", m.group(1)) and
                    {a or b for a, b in re.findall(r"'(\w+)'|\"(\w+)\"", m.group(1))})
    declared = {v["name"] for v in json.loads(VARIABLES.read_text(encoding="utf-8"))["variables"]}
    assert read == declared, f"notebook reads {sorted(read)}, library declares {sorted(declared)}"


def test_notebook_targets_a_real_engine_from_the_library():
    src = _notebook_source()
    assert "vl.dbt_target" in src
    assert "dbt_target = '" not in src and 'dbt_target = "' not in src, "target must come from the library"
    default = next(v["value"] for v in json.loads(VARIABLES.read_text(encoding="utf-8"))["variables"]
                   if v["name"] == "dbt_target")
    assert default in ("duckrun", "iceberg", "ducklake"), "remote_dbt.ENGINES: the DuckDB-family legs"


def test_pipeline_escalates_vcores_through_pipelinecore():
    """The pipeline runs the notebook at 2 vCores and again at 8 if that fails; the notebook's
    %%configure cell takes its vCores from that `pipelinecore` parameter (the source repo's
    mechanism -- it was once deleted here as "dead" because the %%configure cell was missing)."""
    p = json.loads(PIPELINE.read_text(encoding="utf-8"))
    acts = [a for a in p["properties"]["activities"] if a["type"] == "TridentNotebook"]
    assert [a["typeProperties"]["parameters"]["pipelinecore"]["value"] for a in acts] == [2, 8]
    assert acts[1]["dependsOn"] == [{"activity": acts[0]["name"], "dependencyConditions": ["Failed"]}]
    first = "".join(json.loads(NOTEBOOK.read_text(encoding="utf-8"))["cells"][0]["source"])
    assert first.startswith("%%configure") and '"parameterName": "pipelinecore"' in first


def _documented_columns() -> dict[str, set[str]]:
    import yaml

    cols: dict[str, set[str]] = {}
    for f in ("_marts.yml", "_dimensions.yml"):
        doc = yaml.safe_load((patch_dir("duckrun") / f).read_text(encoding="utf-8"))
        for m in doc.get("models", []):
            cols[m["name"]] = {c["name"] for c in m.get("columns", [])}
    return cols


def test_bim_matches_the_documented_mart():
    """The source repo's check_bim, offline: a Direct Lake model that binds a missing column does
    not fail at deploy with a useful message -- Fabric rejects the import naming an object id, or
    accepts it and fails the REFRESH minutes later. Check the three ways the bim goes stale
    against the model YML every engine is tested against: a column that does not exist, a
    relationship endpoint that was never added, DAX naming a dropped column."""
    model = json.loads(BIM.read_text(encoding="utf-8-sig"))["model"]
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
            for table, column in re.findall(r"(\w+)\[([^\]]+)\]", m["expression"]):
                known = {x.lower() for x in bim_cols.get(table, set())} | measures
                if column.lower() not in known:
                    problems.append(f"measure {m['name']}: {table}[{column}] undefined")
    for r in model.get("relationships", []):
        for side in ("from", "to"):
            table, column = r[f"{side}Table"], r[f"{side}Column"]
            if column not in bim_cols.get(table, set()):
                problems.append(f"relationship {r.get('name', '?')}: {table}[{column}] undefined")
    assert not problems, "semantic model is stale against the mart:\n  " + "\n  ".join(problems)


@pytest.mark.parametrize("dbt_schema,expected", [(None, "iceberg_mart"), ("mart", "iceberg_mart"),
                                                 ("test", "test_iceberg_mart")])
def test_mart_schema_mirrors_generate_schema_name(monkeypatch, dbt_schema, expected):
    if dbt_schema is None:
        monkeypatch.delenv("DBT_SCHEMA", raising=False)
    else:
        monkeypatch.setenv("DBT_SCHEMA", dbt_schema)
    assert deploy.mart_schema("iceberg") == expected


def test_rebind_schema_rewrites_every_partition_and_nothing_else():
    original = BIM.read_text(encoding="utf-8-sig")
    out = deploy.rebind_schema(original, "iceberg_mart")
    model = json.loads(out)["model"]
    tables = model["tables"]
    assert tables, "no tables in the bim"
    for t in tables:
        for part in t["partitions"]:
            assert part["mode"] == "directLake"
            assert part["source"]["schemaName"] == "iceberg_mart", t["name"]
        assert t["sourceLineageTag"] == f"[iceberg_mart].[{t['name']}]"
    # The placeholder GUIDs survive untouched: duckrun's deploy(lakehouse=) rewrites them.
    guids = re.findall(r"onelake\.dfs\.fabric\.microsoft\.com/([0-9a-f-]{36})/([0-9a-f-]{36})", original)
    assert guids and re.findall(r"onelake\.dfs\.fabric\.microsoft\.com/([0-9a-f-]{36})/([0-9a-f-]{36})", out) == guids
    assert '"schemaName": "mart"' not in out
