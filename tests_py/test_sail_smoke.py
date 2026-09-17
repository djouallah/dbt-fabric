"""The Sail smoke test must stay a smoke test.

.github/workflows/sail_smoke.yml probes whether LakeSail's Sail could become a SIXTH engine.
It is not one. The whole design rests on that: it touches no gating, no parity, no deploy,
and it is dispatch-only so it costs Fabric capacity only when somebody asks for it.

That is a claim about absence, which is the kind that rots quietly -- somebody adds
requirements/sail.txt, or wires the workflow into pipeline.yml "so it runs with everything
else", and the probe becomes a leg nobody decided to build. These tests pin the absence.

The probe-shape tests exist for a different reason. The two statements that decide the whole
question are the merge (written exactly as dbt-spark's macro renders it) and
`show table extended` (how dbt-spark decides an incremental model already exists). Tidying
the merge into something more readable would silently change what is being measured, and a
probe list that drifts out of step with EXPECTED turns probe 10 -- the one that checks the
writes actually landed -- into a test that always fails or always passes.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import importlib.util
import os
import re

import pytest
import yaml

from _layout import ENGINES, REPO

SMOKE_PY = REPO / ".github" / "scripts" / "sail_smoke.py"
SMOKE_YML = REPO / ".github" / "workflows" / "sail_smoke.yml"
SMOKE_REQ = REPO / "requirements" / "sail_smoke.txt"


@pytest.fixture(scope="module")
def smoke():
    """Import sail_smoke.py with dummy credentials.

    It reads WAREHOUSE_PATH and ONELAKE_TOKEN at IMPORT time, deliberately: Sail parses
    SAIL_CATALOG__LIST when the server starts, so the config has to be in the environment
    before anything touches pysail. That makes the module un-importable without them, hence
    the dummies. Nothing here starts a server or opens a connection.
    """
    os.environ.setdefault("WAREHOUSE_PATH", "ws-guid/item-guid")
    os.environ.setdefault("ONELAKE_TOKEN", "dummy-token")
    spec = importlib.util.spec_from_file_location("sail_smoke", SMOKE_PY)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---- it is not an engine -----------------------------------------------------------------

def test_sail_is_not_a_registered_engine():
    assert "sail" not in ENGINES, (
        "sail appeared in the engine list. If it has really become a sixth engine it needs a "
        "model tree, a profile target, gating, parity and a deploy binding -- see CLAUDE.md. "
        "If it has not, take it back out."
    )


def test_check_gating_does_not_know_about_sail():
    # check_gating.ENGINES is the one place the engine -> project mapping lives, and every
    # other registry mirrors it. Read the source rather than importing: the module pulls dbt.
    src = (REPO / ".github" / "scripts" / "check_gating.py").read_text(encoding="utf-8")
    block = re.search(r"ENGINES\s*=\s*\{(.*?)\}", src, re.S)
    assert block, "check_gating.py's ENGINES dict moved -- this test is now blind"
    assert "sail" not in block.group(1)


@pytest.mark.parametrize("script", ["provision.py", "remote_dbt.py", "run_in_fabric.py"])
def test_ci_scripts_do_not_know_about_sail(script):
    src = (REPO / ".github" / "scripts" / script).read_text(encoding="utf-8")
    assert not re.search(r"""["']sail["']""", src), (
        f"{script} names 'sail' as an engine. The smoke job runs none of these -- it assumes "
        f"the shared lakehouse and composes its own catalog url -- so a 'sail' key here means "
        f"the probe has quietly become a leg."
    )


def test_there_is_no_engine_shaped_requirements_file():
    # build.yml resolves an engine's dependencies as requirements/<engine>.txt (falling back
    # from <engine>_runner.txt). A file at that stem is how this stops being a probe.
    assert not (REPO / "requirements" / "sail.txt").exists()
    assert SMOKE_REQ.exists()


def test_no_jvm_spark_in_the_smoke_env():
    """The probe must not install Apache Spark to talk to the thing replacing Apache Spark.

    Sail's whole claim is Spark compatibility with no JVM. `pyspark` and `pyspark[connect]`
    are the full distribution, jars included; `pyspark-client` is Apache's slim Connect client
    and keeps the same `pyspark.sql` import path, so nothing in the script depends on which
    one is installed -- which is exactly why this needs pinning rather than noticing.
    """
    lines = [ln.split("#")[0].strip() for ln in SMOKE_REQ.read_text(encoding="utf-8").splitlines()]
    pkgs = [ln for ln in lines if ln]
    assert "pyspark-client" in pkgs, f"the Connect client went missing: {pkgs}"
    for bad in ("pyspark", "pyspark[connect]"):
        assert bad not in pkgs, (
            f"requirements/sail_smoke.txt installs {bad!r}, which is Apache Spark with its "
            f"JVM jars. Use pyspark-client."
        )


def test_there_is_no_sail_model_tree():
    for project in ("dbt1", "dbt2"):
        assert not (REPO / project / "models" / "aemo" / "sail").exists()
        assert not (REPO / project / "tests" / "aemo" / "sail").exists()


# ---- it stays dispatch-only and non-gating -----------------------------------------------

@pytest.fixture(scope="module")
def workflow():
    return yaml.safe_load(SMOKE_YML.read_text(encoding="utf-8"))


def test_workflow_is_dispatch_only(workflow):
    # PyYAML parses the bare `on:` key as the boolean True (YAML 1.1's Norway problem).
    triggers = workflow[True] if True in workflow else workflow["on"]
    assert set(triggers) == {"workflow_dispatch"}, (
        f"sail_smoke.yml gained a trigger: {sorted(triggers)}. It spends Fabric capacity, and "
        f"it answers a question somebody has to be asking."
    )


def test_workflow_never_gates(workflow):
    job = workflow["jobs"]["smoke"]
    assert job["continue-on-error"] is True, (
        "the smoke job must not fail the run -- a red result is its output, not a breakage."
    )


def test_no_other_workflow_calls_it():
    for wf in (REPO / ".github" / "workflows").glob("*.yml"):
        if wf.name == "sail_smoke.yml":
            continue
        assert "sail_smoke" not in wf.read_text(encoding="utf-8"), (
            f"{wf.name} references sail_smoke. Wiring the probe into the pipeline makes it a "
            f"leg by accident."
        )


def test_workflow_provisions_nothing():
    """A probe must not create Fabric items to answer a question about SQL support.

    It ran `provision.py iceberg` once, to resolve the warehouse path. That failed on a
    missing `requests` (the smoke env installs neither provision.py's dependencies nor
    duckrun), but the real objection is the other one: the shared `dbt` lakehouse is created
    by every real leg, so a probe should assume it and compose the catalog url from the
    workspace secret and the item's known name.
    """
    body = SMOKE_YML.read_text(encoding="utf-8")
    # An INVOCATION, not a mention -- the header comment explains at length why there is no
    # provisioning here, and a bare substring check trips over its own explanation.
    called = [ln for ln in body.splitlines()
              if "provision.py" in ln and not ln.lstrip().startswith("#")]
    assert not called, (
        f"sail_smoke.yml calls provision.py again: {called}. The lakehouse is assumed to "
        f"exist; the catalog url is composed, not resolved."
    )
    assert "WAREHOUSE_PATH:" in body, "the composed catalog url went missing"


def test_script_always_exits_zero():
    assert "sys.exit(0)" in SMOKE_PY.read_text(encoding="utf-8")


# ---- the probes measure what they claim to -----------------------------------------------

def test_probe_numbers_are_contiguous(smoke):
    numbers = [n for n, _, _, _ in smoke.probes()]
    assert numbers == list(range(1, len(numbers) + 1))


def test_merge_probes_use_dbt_spark_shape(smoke):
    merges = {n: sql for n, name, sql, _ in smoke.probes() if "MERGE" in name}
    assert set(merges) == {6, 7}, "the two merge probes moved"

    for n, sql in merges.items():
        # dbt-spark's merge macro renders these aliases. A parser that accepts a tidied-up
        # merge and rejects dbt's is exactly the result worth finding.
        assert "DBT_INTERNAL_DEST" in sql and "DBT_INTERNAL_SOURCE" in sql, f"probe {n}"
        assert "when not matched then insert *" in sql, f"probe {n}"

    # 6 is the full strategy; 7 is the insert-only shape dim_calendar and the wide facts take.
    assert "when matched then update set *" in merges[6]
    assert "when matched" not in merges[7], (
        "probe 7 is the INSERT-ONLY merge. A matched clause there means both probes measure "
        "the same thing and the insert-only answer is never obtained."
    )


def test_relation_listing_probe_exists(smoke):
    # The quiet one. Empty output here means dbt-spark cannot see existing relations and every
    # run silently full-refreshes -- worse than an error, so it must stay probed.
    sql = {n: sql for n, _, sql, _ in smoke.probes()}[8]
    assert "show table extended" in sql


def test_expected_rows_cover_every_key_the_probes_write(smoke):
    """EXPECTED and the probe SQL have to move together, or probe 10 stops meaning anything."""
    sql = " ".join(s for _, _, s, _ in smoke.probes()) + " " + smoke.INSERT_ONLY_SOURCE

    # Keys are the first int of each `select <k> as k, <v> as v` / `select <k>, <v>` pair, and
    # of each `values (<k>, <v>)`.
    written = {int(k) for k, _ in re.findall(r"select\s+(\d+)(?:\s+as\s+k)?\s*,\s*(\d+)", sql)}
    written |= {int(k) for k, _ in re.findall(r"values\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", sql)}

    assert written, "no literal rows found in the probe SQL -- this test has gone blind"
    assert {k for k, _ in smoke.EXPECTED} == written, (
        f"EXPECTED covers {sorted(k for k, _ in smoke.EXPECTED)} but the probes write "
        f"{sorted(written)}. Probe 10 compares the table against EXPECTED; out of step, it "
        f"either always fails or stops catching a merge that silently applied nothing."
    )
