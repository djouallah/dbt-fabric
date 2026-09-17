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

REPO = Path(__file__).resolve().parents[1]
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
    """CLAUDE.md: keep each cell's `source` as an array of lines, never one string."""
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


def test_pipeline_has_one_notebook_activity_and_no_parameters():
    p = json.loads(PIPELINE.read_text(encoding="utf-8"))
    acts = [a for a in p["properties"]["activities"] if a["type"] == "TridentNotebook"]
    assert len(acts) == 1
    assert "parameters" not in acts[0]["typeProperties"], "the notebook has no parameters cell"


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
