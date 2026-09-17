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
    assert "WAREHOUSE_PATH=" in body, "the resolved catalog url went missing"
    # Resolving an existing item is not provisioning, but only if it stays a GET.
    for verb in ('-X POST', '-X PUT', '-X PATCH', '"POST"', "'POST'"):
        assert verb not in body, f"sail_smoke.yml issues a {verb} -- it must only read"


def test_script_always_exits_zero_and_actually_exits():
    """It must end the PROCESS, not just the interpreter's main function.

    The embedded Sail server runs on non-daemon threads, so a normal sys.exit() waits on them
    and the job sits idle after printing its whole summary -- burning the runner until the
    timeout kills it. os._exit is what actually ends it, and the flushes before it are not
    optional: os._exit skips them, and the output goes through `| tee` into the artifact.
    """
    src = SMOKE_PY.read_text(encoding="utf-8")
    assert "os._exit(0)" in src, (
        "the probe no longer hard-exits. The Sail server's threads will keep the process "
        "alive after the report and the job will hang until its timeout."
    )
    assert "sys.stdout.flush()" in src and "sys.stderr.flush()" in src


def test_probe_step_is_time_bounded(workflow):
    """Backstop for the same hang, so a regression costs 10 minutes and not the job budget."""
    step = next(s for s in workflow["jobs"]["smoke"]["steps"]
                if s.get("name") == "Probe Sail")
    assert step.get("timeout-minutes"), "the Probe Sail step lost its timeout"


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


def test_every_create_names_the_table_format(smoke):
    """A bare CREATE TABLE gets refused by the catalog.

    Sail defaults to parquet, and the OneLake Iceberg REST catalog will not have it: "not
    supported: Iceberg REST catalog cannot create 'parquet' tables" (run 35188636093). This is
    dbt-spark's `file_format: iceberg` said in DDL, and forgetting it on one of the three
    creates fails that probe alone, which reads like a finding about the statement rather than
    about the missing clause.
    """
    src = SMOKE_PY.read_text(encoding="utf-8")
    creates = [ln for ln in src.splitlines() if "create table {" in ln]
    assert len(creates) >= 4, f"a create statement went missing: {creates}"
    for ln in creates:
        assert "using {FILE_FORMAT}" in ln, f"create without a format: {ln.strip()}"
    assert smoke.FILE_FORMAT == "iceberg"


def test_probe_7_does_not_depend_on_probe_6(smoke):
    """The two merge shapes are independent questions.

    Probe 7's source table was created in probe 6's success path, so when the full merge
    failed, probe 7 reported "Table not found" and the insert-only shape -- which is what the
    wide facts and dim_calendar would actually use -- went unmeasured (run 35188799850).
    """
    src = SMOKE_PY.read_text(encoding="utf-8")
    assert "def ensure_insert_only_source" in src, (
        "probe 7's source is inline again; it must be created regardless of probe 6."
    )
    # It has to run BEFORE the statement, not in any branch that a failure skips.
    body = src.splitlines()
    gate = [j for j, ln in enumerate(body) if ln.strip() == "if n == 7:"]
    assert gate, "the probe-7 gate moved"
    assert "ensure_insert_only_source(conn)" in body[gate[0] + 1]


def test_merge_target_sets_merge_on_read(smoke):
    """Sail rejects a merge against a copy-on-write Iceberg table."""
    create = {n: sql for n, _, sql, _ in smoke.probes()}[3]
    assert "tblproperties" in create and "merge-on-read" in smoke.MERGE_MODE, (
        "the merge target lost write.merge.mode=merge-on-read, which Sail requires: "
        "'Iceberg MERGE with `write.merge.mode=copy-on-write` is not supported yet'"
    )


def test_phase_b_covers_what_the_models_do(smoke):
    """Phase A can be all green while the gold layer still cannot run.

    Two-column literal tables exercise none of what the AEMO models do, so phase B probes the
    shapes that have actually broken engines in this repo: a CSV temp view with an explicit
    all-STRING schema over a brace glob, input_file_name() for provenance, whether a temp view
    is really temporary (Fabric Spark's is not, which is why the spark leg stages through a
    Delta table), slash-date parsing, sequence+explode, a 130-column record and a
    multi-column merge key.
    """
    shapes = smoke.model_shape_probes(["abfss://w@h/a/one.CSV", "abfss://w@h/a/two.CSV"])
    by_name = {name.lower(): sql for _, name, sql, _ in shapes}
    blob = " ".join(by_name.values()).lower()

    for needed in ("using csv", "input_file_name()", "to_timestamp", "explode(sequence(",
                   "row_number() over", "decimal(18,6)", "merge into"):
        assert needed.lower() in blob, f"phase B stopped probing {needed}"

    # The brace glob, not a folder glob: a run folds exactly the files it chose to fold.
    assert "{one.csv,two.csv}" in blob

    # 130 columns, because fct_price is AEMO's DREGION record in full.
    wide = next(sql for name, sql in by_name.items() if "wide" in name)
    assert wide.count(" as c") == 130

    # A multi-column ON clause -- fct_summary keys on (date, time, DUID).
    keyed = next(sql for name, sql in by_name.items() if "three-column" in name)
    assert keyed.count("DBT_INTERNAL_SOURCE.c") == 3


def test_csv_probes_skip_rather_than_fail_without_files(smoke):
    """No landing files is a fact about the landing zone, not a finding about Sail."""
    for n, _, sql, _ in smoke.model_shape_probes([]):
        if n in (12, 13, 14):
            assert sql == "", f"probe {n} would run without a file and report a false FAIL"


def test_relation_listing_probe_exists(smoke):
    # The quiet one. Empty output here means dbt-spark cannot see existing relations and every
    # run silently full-refreshes -- worse than an error, so it must stay probed.
    sql = {n: sql for n, _, sql, _ in smoke.probes()}[8]
    assert "show table extended" in sql


def test_a_run_that_never_connected_is_inconclusive(smoke):
    """A failed connection must not be reported as a verdict about Sail.

    The first real run failed every probe with `Failed to load config: 400 Bad Request` --
    the catalog url was wrong -- and the summary announced "not viable yet, dbt-spark cannot
    see existing relations". That is a conclusion drawn from the probe's own misconfiguration,
    which is worse than no conclusion.
    """
    all_failed = [(n, "x", "FAIL - config") for n in (1, 6, 8, 10)]
    assert "INCONCLUSIVE" in smoke.read_verdict(all_failed)

    # ... but a reached catalog with a failed merge is still a real finding.
    reached = [(1, "x", "PASS (3 row(s))"), (3, "x", "PASS (1 row(s))"),
               (6, "x", "FAIL - unsupported"), (8, "x", "PASS (2 row(s))"),
               (10, "x", "MISMATCH")]
    assert "INCONCLUSIVE" not in smoke.read_verdict(reached)


def test_a_run_that_created_nothing_is_inconclusive(smoke):
    """Probes 6-10 can only report a missing table when CREATE TABLE failed.

    Observed on run 35188439628: the catalog was reachable and `create schema` passed, but
    every write died on the storage credential. Probe 8 then listed nothing -- correctly, the
    schema was empty -- and the summary announced "not viable yet, dbt-spark cannot see
    existing relations". Same class of wrong answer as the connection case above.
    """
    no_table = [(1, "x", "PASS (9 row(s))"), (3, "x", "FAIL - Identity not found"),
                (6, "x", "FAIL - Table not found"), (8, "x", "EMPTY - listed NOTHING"),
                (10, "x", "FAIL - Table not found")]
    v = smoke.read_verdict(no_table)
    assert "INCONCLUSIVE" in v
    # and it should name the actual cause rather than leaving it to be rediscovered
    assert "AZURE_STORAGE_TOKEN" in v


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
