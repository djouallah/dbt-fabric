"""The chDB smoke test must stay a smoke test, and must stay read-only.

.github/workflows/chdb_smoke.yml probes whether chDB could become a SIXTH engine. It is not
one. The whole design rests on that: it touches no gating, no parity, no deploy, and it is
dispatch-only so it costs Fabric capacity only when somebody asks for it. That is a claim
about absence, which is the kind that rots quietly -- somebody adds requirements/chdb.txt, or
wires the workflow into pipeline.yml "so it runs with everything else", and the probe becomes
a leg nobody decided to build.

THE OTHER HALF OF THIS FILE IS A DATA-LOSS GUARD, and it matters more than the absence tests.
chDB's DatabaseDataLake::dropTable calls table->drop() on the REAL Iceberg table, so a
`DROP TABLE` against the attached catalog would delete the iceberg leg's gold layer -- and
`DROP DATABASE` is the same hazard one level up. The probe reads; the one CREATE it attempts
is the no-op measurement, and a table that somehow appears is left in place and flagged.
test_probe_never_writes is what keeps that true.

And one correctness test: CREATE TABLE through a DataLakeCatalog RETURNS SUCCESS and registers
nothing. Reporting that as PASS would be reporting the exact failure mode -- a green run that
built nothing -- the probe was written to catch.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import importlib.util
import os
import re

import pytest
import yaml

from _layout import ENGINES, REPO

SMOKE_PY = REPO / ".github" / "scripts" / "chdb_smoke.py"
SMOKE_YML = REPO / ".github" / "workflows" / "chdb_smoke.yml"
SMOKE_REQ = REPO / "requirements" / "chdb_smoke.txt"


@pytest.fixture(scope="module")
def smoke():
    """Import chdb_smoke.py with dummy credentials.

    It reads WAREHOUSE_PATH and ONELAKE_TOKEN at IMPORT time, so the module is un-importable
    without them -- hence the dummies. Nothing here opens a session or touches the network;
    `chdb` itself is never imported, which is why this test module does not need the wheel.
    """
    os.environ.setdefault("WAREHOUSE_PATH", "ws-guid/item-guid")
    os.environ.setdefault("ONELAKE_TOKEN", "dummy-token")
    os.environ.setdefault("LANDING_PATH",
                          "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/land-guid/Files")
    spec = importlib.util.spec_from_file_location("chdb_smoke", SMOKE_PY)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---- it is not an engine -----------------------------------------------------------------

def test_chdb_is_not_a_registered_engine():
    assert "chdb" not in ENGINES, (
        "chdb appeared in the engine list. If it has really become a sixth engine it needs a "
        "model tree, a profile target, gating, parity and a deploy binding -- see AGENTS.md. "
        "If it has not, take it back out."
    )


def test_check_gating_does_not_know_about_chdb():
    # check_gating.ENGINES is the one place the engine -> project mapping lives, and every
    # other registry mirrors it. Read the source rather than importing: the module pulls dbt.
    src = (REPO / ".github" / "scripts" / "check_gating.py").read_text(encoding="utf-8")
    block = re.search(r"ENGINES\s*=\s*\{(.*?)\}", src, re.S)
    assert block, "check_gating.py's ENGINES dict moved -- this test is now blind"
    assert "chdb" not in block.group(1)


@pytest.mark.parametrize("script", ["provision.py", "remote_dbt.py", "run_dbt.py"])
def test_ci_scripts_do_not_know_about_chdb(script):
    src = (REPO / ".github" / "scripts" / script).read_text(encoding="utf-8")
    assert not re.search(r"""["']chdb["']""", src), (
        f"{script} names 'chdb' as an engine. The smoke job runs none of these -- it assumes "
        f"the shared lakehouse and composes its own warehouse path -- so a 'chdb' key here "
        f"means the probe has quietly become a leg."
    )


def test_there_is_no_engine_shaped_requirements_file():
    # pipeline.yml resolves an engine's dependencies as requirements/<engine>.txt. A file at
    # that stem is how this stops being a probe.
    assert not (REPO / "requirements" / "chdb.txt").exists()
    assert SMOKE_REQ.exists()


def test_the_wheel_floor_is_a_floor_not_a_pin():
    """Below chdb 4.4.0 the setting the probe is built on does not exist.

    `onelake_bearer_token` landed in chdb-core 26.7, which chdb 4.4.0 is the first release to
    require. Resolve onto anything older and probe 1 fails with "Unknown setting", which reads
    like a finding about OneLake support rather than about the wheel -- so the floor is
    load-bearing. An `==` pin is the opposite mistake: it freezes the answer at the moment the
    probe was written, which is the thing being measured.
    """
    lines = [ln.split("#")[0].strip() for ln in SMOKE_REQ.read_text(encoding="utf-8").splitlines()]
    pkgs = [ln for ln in lines if ln]
    assert pkgs == ["chdb>=4.4.0"], f"the chdb requirement changed shape: {pkgs}"


def test_there_is_no_chdb_model_tree():
    assert not (REPO / "dbt1" / "models" / "aemo" / "chdb").exists()
    assert not (REPO / "dbt1" / "tests" / "aemo" / "chdb").exists()


# ---- it stays dispatch-only and non-gating -----------------------------------------------

@pytest.fixture(scope="module")
def workflow():
    return yaml.safe_load(SMOKE_YML.read_text(encoding="utf-8"))


def test_workflow_is_dispatch_only(workflow):
    # PyYAML parses the bare `on:` key as the boolean True (YAML 1.1's Norway problem).
    triggers = workflow[True] if True in workflow else workflow["on"]
    assert set(triggers) == {"workflow_dispatch"}, (
        f"chdb_smoke.yml gained a trigger: {sorted(triggers)}. It spends Fabric capacity, and "
        f"it answers a question somebody has to be asking."
    )


def test_workflow_never_gates(workflow):
    job = workflow["jobs"]["smoke"]
    assert job["continue-on-error"] is True, (
        "the smoke job must not fail the run -- a red result is its output, not a breakage."
    )


def test_no_other_workflow_calls_it():
    for wf in (REPO / ".github" / "workflows").glob("*.yml"):
        if wf.name == "chdb_smoke.yml":
            continue
        assert "chdb_smoke" not in wf.read_text(encoding="utf-8"), (
            f"{wf.name} references chdb_smoke. Wiring the probe into the pipeline makes it a "
            f"leg by accident."
        )


def test_workflow_provisions_nothing():
    """A probe must not create Fabric items to answer a question about engine support."""
    body = SMOKE_YML.read_text(encoding="utf-8")
    # An INVOCATION, not a mention -- the header comment explains at length why there is no
    # provisioning here, and a bare substring check trips over its own explanation.
    called = [ln for ln in body.splitlines()
              if "provision.py" in ln and not ln.lstrip().startswith("#")]
    assert not called, (
        f"chdb_smoke.yml calls provision.py: {called}. The lakehouse is assumed to exist; the "
        f"warehouse path is composed, not resolved."
    )
    assert "WAREHOUSE_PATH=" in body, "the resolved warehouse path went missing"
    # Resolving an existing item is not provisioning, but only if it stays a GET.
    for verb in ('-X POST', '-X PUT', '-X PATCH', '"POST"', "'POST'"):
        assert verb not in body, f"chdb_smoke.yml issues a {verb} -- it must only read"


def test_probe_step_is_time_bounded(workflow):
    step = next(s for s in workflow["jobs"]["smoke"]["steps"]
                if s.get("name") == "Probe chdb")
    assert step.get("timeout-minutes"), "the Probe chdb step lost its timeout"


def test_the_token_is_minted_for_storage_not_fabric():
    """The audience is the whole point.

    chDB's OneLake catalog takes ONE token for the catalog REST calls AND the blob reads, and
    it has to be the storage.azure.com one: an api.fabric.microsoft.com token authenticates
    against the catalog and then reads nothing, which looks like a storage bug rather than a
    wrong audience.
    """
    body = SMOKE_YML.read_text(encoding="utf-8")
    mint = [ln for ln in body.splitlines()
            if "get-access-token" in ln and "ONELAKE_TOKEN" not in ln]
    assert any("https://storage.azure.com/" in ln for ln in mint), (
        f"no storage.azure.com token is minted: {mint}"
    )
    assert "::add-mask::" in body, "the token reaches $GITHUB_ENV unmasked"


# ---- the probe never writes --------------------------------------------------------------

def executed_sql(mod):
    """Every statement the script can actually hand the engine.

    Two sources, because there are two ways in. The probe lists carry most of it, and main()
    issues a few inline -- the SET, the attach, SHOW TABLES, DESCRIBE. The inline ones are
    read out of the AST by finding `run(sess, <sql>)` calls and rendering their argument.

    Deliberately NOT a substring scan over the file. The script explains at length why it
    never drops anything, and probe 8's own name is "INSERT INTO" -- a grep for the verbs
    matches the prose about them, which is the trap test_workflow_provisions_nothing sidesteps
    by looking for an invocation rather than a mention.
    """
    import ast

    out = [s for s in mod.attach_sql()]
    for probes in (mod.phase_a_probes("some_mart.some_table",
                                      [("settlementdate", "DateTime"), ("duid", "String")]),
                   mod.model_shape_probes(["land-guid/Files/csv_raw/a.CSV"]),
                   mod.model_shape_probes([])):
        for _, _, sql, _ in probes:
            out.extend(sql if isinstance(sql, (list, tuple)) else [sql])

    def render(node):
        """A literal or f-string as text; the interpolated slots become empty, which is fine
        -- a table NAME cannot turn a SELECT into a DROP."""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.JoinedStr):
            return "".join(render(v) for v in node.values)
        return ""

    tree = ast.parse(SMOKE_PY.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                and node.func.id == "run" and len(node.args) >= 2):
            out.append(render(node.args[1]))
    return [s for s in out if s]


def test_probe_never_writes(smoke):
    """THE DATA-LOSS GUARD.

    DatabaseDataLake::dropTable calls table->drop() on the real Iceberg table, so a DROP
    against the attached catalog deletes the iceberg leg's gold layer -- and the only tables
    in that catalog ARE the leg's. DROP DATABASE is the same hazard one level up. The session
    is in-memory, so dying is the cleanup and none of these are needed for anything.
    """
    statements = " ".join(executed_sql(smoke)).lower()
    for anchor in ("select count()", "show tables from", "create database"):
        assert anchor in statements, (
            f"{anchor!r} is missing, so this test is no longer reading the statements the "
            f"script runs -- it would pass on a file that dropped everything"
        )
    for banned in ("drop table", "drop database", "insert into", "alter table",
                   "optimize table", "truncate table"):
        assert banned not in statements, (
            f"chdb_smoke.py issues {banned!r}. The probe reads. The catalog's drop path "
            f"deletes real data, and the only tables in it belong to the iceberg leg."
        )

    # The session must stay ephemeral -- a path would persist the catalog attachment past the
    # process, which is the state this design exists to not have.
    assert "session.Session()" in SMOKE_PY.read_text(encoding="utf-8"), (
        "the probe's session gained a path; it must be in-memory and die with the process"
    )


def test_the_only_write_attempt_is_the_no_op_probe(smoke):
    """One CREATE, into a namespace no leg uses, and it is the measurement."""
    creates = [sql for _, _, sql, _ in smoke.phase_a_probes("some_mart.some_table", [])
               if isinstance(sql, str) and "CREATE TABLE" in sql]
    assert len(creates) == 1, f"the write attempts changed: {creates}"
    # AND IT MUST NAME AN ENGINE. Without one chDB falls back to MergeTree and dies on
    # "MergeTree storages require data path" before the statement reaches the database, so the
    # no-op goes unmeasured and the probe reports a failure about storage engines instead
    # (run 35433920589). Memory constructs without touching a disk or the lake, which leaves
    # the DATABASE's handling of the create as the only thing under test.
    assert "ENGINE = Memory" in creates[0], creates[0]
    assert smoke.PROBE_TABLE.startswith(smoke.SCHEMA + "."), (
        "probe 7 must create inside the probe's own namespace, not a leg's"
    )
    for engine in ENGINES:
        assert not smoke.PROBE_TABLE.startswith(f"{engine}_"), (
            f"probe 7 writes into {engine}'s namespace"
        )


# ---- the probes measure what they claim to -----------------------------------------------

def test_create_table_no_op_is_not_reported_as_pass(smoke):
    """The whole reason probe 7 re-lists the catalog instead of trusting the statement.

    createTable has an empty body upstream: the statement succeeds and nothing is registered.
    A PASS there would be this repo's signature failure mode -- a green run that built
    nothing -- reported as a success by the probe meant to catch it.
    """
    absent = [("iceberg_mart.fct_price",), ("iceberg_landing.stg_csv_archive_log",)]
    status = smoke.interpret(7, absent)
    assert status.startswith("SILENT NO-OP"), status
    assert not status.startswith("PASS")

    # ... and if it ever DOES register, that is a different answer and must not be tidied
    # away -- nor auto-dropped, which is the path that deletes data.
    present = absent + [(smoke.PROBE_TABLE,)]
    created = smoke.interpret(7, present)
    assert created.startswith("CREATED"), created
    assert "LEFT IN PLACE" in created


def test_probe_numbers_are_contiguous(smoke):
    """1 and 2 are run by main() (the read probes need a table discovered from 2's output),
    3-8 are phase A, 9-17 phase B. Gaps mean a probe was dropped without renumbering, and the
    report reads as if it were still being measured."""
    nums = ([1, 2]
            + [n for n, _, _, _ in smoke.phase_a_probes("s.t", [])]
            + [n for n, _, _, _ in smoke.model_shape_probes(["land-guid/Files/csv_raw/a.CSV"])])
    assert nums == list(range(1, len(nums) + 1)), nums


def test_catalog_table_names_are_backticked(smoke):
    """The OneLake catalog reports <namespace>.<table> as ONE name and ClickHouse has no
    second namespace level, so the whole thing goes inside one pair of backticks. Unquoted it
    resolves as a database that does not exist, and every read probe fails for a reason that
    has nothing to do with chDB."""
    assert smoke.quoted("iceberg_mart.fct_price") == "`iceberg_mart.fct_price`"
    for n, _, sql, _ in smoke.phase_a_probes("iceberg_mart.fct_price", []):
        if sql and "iceberg_mart" in sql:
            assert "`iceberg_mart.fct_price`" in sql, f"probe {n} lost its backticks: {sql}"


def test_phase_b_covers_what_the_models_do(smoke):
    """Phase A can be all green while the gold layer still cannot be built.

    Reading the iceberg leg's marts exercises none of what the models DO: every one of them
    starts by reading a ragged AEMO CSV out of Files/, which is exactly the section the
    catalog does not cover.
    """
    shapes = smoke.model_shape_probes(["land-guid/Files/csv_raw/PUBLIC_DAILY.CSV"])
    by_name = {name.lower(): sql for _, name, sql, _ in shapes}
    blob = " ".join(by_name.values())

    for needed in ("azureBlobStorage(", "url(", "headers('Authorization'='Bearer",
                   "x-ms-version", "_file", "parseDateTime", "toDecimal64",
                   "arrayJoin", "row_number() OVER"):
        assert needed in blob, f"phase B stopped probing {needed}"

    # The ragged read needs BOTH: a structure padded past the widest record in the file, and
    # the setting that lets every narrower row through. Either alone fails, in opposite
    # directions.
    csv_probe = next(sql for name, sql in by_name.items() if "ragged aemo csv" in name)
    assert smoke.RAGGED in csv_probe
    assert smoke.CSV_WIDTH >= 131, "the structure must be at least as wide as DREGION"
    assert csv_probe.count(" String") == smoke.CSV_WIDTH

    # Probe 10 must be the plain-bytes read. Using CSV there would let a parsing failure be
    # reported as an auth failure, and auth is the entire question this job was built for.
    reach = next(sql for name, sql in by_name.items() if "escape hatch" in name)
    assert "LineAsString" in reach and "CSV," not in reach

    # Probe 9 is the control and MUST be allowed to fail: azureBlobStorage() has no
    # bearer-token argument, so counting its refusal against phase B would misreport the leg.
    assert 9 in smoke.MAY_FAIL
    assert 14 in smoke.MAY_FAIL, (
        "the bare-cast probe is expected to error, and an error there is the BETTER outcome"
    )


def test_csv_probes_skip_rather_than_fail_without_files(smoke):
    """No landing files is a fact about the landing zone, not a finding about chDB."""
    for n, _, sql, _ in smoke.model_shape_probes([]):
        if n in (9, 10, 11, 12):
            assert sql == "", f"probe {n} would run without a file and report a false FAIL"


def test_aggregate_probes_skip_rather_than_guess_the_schema(smoke):
    """Probes 5 and 6 pick their columns out of DESCRIBE.

    Hardcoding settlementdate/duid would report FAIL against a workspace whose marts are named
    differently -- a finding about the probe's guess, not about chDB.
    """
    typed = [("settlementdate", "Nullable(DateTime64(6))"), ("duid", "Nullable(String)")]
    by_n = {n: sql for n, _, sql, _ in smoke.phase_a_probes("m.fct", typed)}
    assert "settlementdate" in by_n[5] and "uniqExact(duid)" in by_n[5]
    assert "settlementdate" in by_n[6]

    untyped = [("k", "Nullable(Int64)")]
    blind = {n: sql for n, _, sql, _ in smoke.phase_a_probes("m.fct", untyped)}
    assert blind[5] == "" and blind[6] == "", (
        "with no date-like column these must SKIP, not invent one"
    )


def test_token_is_redacted_from_the_artifact(smoke):
    """The token is INSIDE phase B's SQL -- url()'s headers() and probe 9's account slots --
    so it is in the statements the script prints and in the errors they raise. ::add-mask::
    covers the workflow log; the artifact is the thing that gets attached to an issue."""
    assert smoke.redact(f"headers('Authorization'='Bearer {smoke.TOKEN}')") \
        == "headers('Authorization'='Bearer ***')"
    src = SMOKE_PY.read_text(encoding="utf-8")
    assert "redact(oneline(e))" in src, (
        "phase B error text is printed unredacted, and probe 9 puts the token in its SQL"
    )


# ---- a run that never connected is inconclusive ------------------------------------------

def test_a_run_that_never_attached_is_inconclusive(smoke):
    """A failed attach must not be reported as a verdict about chDB.

    The sail probe announced "not viable yet" across runs whose catalog config had been
    rejected before a single statement was planned. That is a conclusion drawn from the
    probe's own misconfiguration, which is worse than no conclusion.
    """
    v = smoke.read_verdict([(n, "x", "FAIL - config") for n in (1, 2, 4, 7)])
    assert "INCONCLUSIVE" in v
    # ... and it must name the two things that actually break it, rather than leaving them to
    # be rediscovered.
    assert "storage.azure.com" in v and "workspace-id" in v


def test_an_empty_catalog_is_inconclusive_not_a_finding(smoke):
    """The OneLake catalog is the ICEBERG table API. A workspace where the iceberg leg has
    never run lists nothing, and that says nothing about chDB."""
    v = smoke.read_verdict([(1, "x", "PASS"),
                            (2, "x", "EMPTY - the catalog answered and listed NOTHING")])
    assert "INCONCLUSIVE" in v
    assert "iceberg" in v.lower()


def test_catalog_without_storage_is_its_own_finding(smoke):
    """The bearer token has two jobs. One working and the other not is a real result, and a
    different one from "chDB cannot reach OneLake"."""
    v = smoke.read_verdict([(1, "x", "PASS"), (2, "x", "PASS (8 table(s))"),
                            (4, "x", "FAIL - ACCESS_DENIED")])
    assert "CATALOG YES, STORAGE NO" in v


def test_the_no_op_verdict_rules_out_an_engine_not_a_reader(smoke):
    """The distinction the whole job turns on: chDB reading the gold layer is useful and is
    not a sixth engine. A verdict that blurred them would put a leg on the roadmap that
    cannot materialise a single model."""
    green = [(1, "x", "PASS"), (2, "x", "PASS (8 table(s))"), (4, "x", "PASS (1 row(s))"),
             (7, "x", "SILENT NO-OP -- the statement succeeded and the table is NOT in the "
                      "catalog"),
             (10, "x", "PASS (1 row(s))"), (11, "x", "PASS (1 row(s))")]
    v = smoke.read_verdict(green)
    assert "READER ONLY" in v
    assert "CANNOT WRITE" in v
    # Phase B has to be able to speak for itself: reads working says nothing about ingest.
    assert "INGEST:" in v


def test_unreachable_files_is_reported_as_blocked_ingest(smoke):
    """Files/ is where every model starts. The catalog covers Tables/ and nothing else, so a
    reader verdict that stayed silent about the landing zone would be half an answer."""
    blocked = [(1, "x", "PASS"), (2, "x", "PASS (8 table(s))"), (4, "x", "PASS (1 row(s))"),
               (7, "x", "SILENT NO-OP"),
               (10, "x", "FAIL - 400 Bad Request"), (11, "x", "FAIL - 400 Bad Request")]
    v = smoke.read_verdict(blocked)
    assert "INGEST: BLOCKED" in v

    skipped = [x for x in blocked if x[0] not in (10, 11)] + [
        (10, "x", "SKIP - no landing files"), (11, "x", "SKIP - no landing files")]
    assert "INGEST: UNPROVEN" in smoke.read_verdict(skipped)


def test_a_read_that_worked_cannot_be_called_blocked(smoke):
    """THE REGRESSION. Run 35433920589 announced "INGEST: BLOCKED -- Files/ is unreachable"
    on a run where probes 11 and 12 had both read 334k rows off the very same url. Probe 10
    had died inside the script's own JSON parsing, and the verdict keyed off 10 alone.

    A headline that survives its own evidence is the bug -- the same one the sail probe was
    hardened against -- so the ragged read decides and probe 10 only corroborates.
    """
    real = [(1, "x", "PASS"), (2, "x", "PASS (21 table(s))"), (4, "x", "PASS (1 row(s))"),
            (7, "x", "SILENT NO-OP"),
            (10, "x", "FAIL - JSONDecodeError: Extra data"),
            (11, "x", "PASS (1 row(s))"), (12, "x", "PASS - _file resolves")]
    v = smoke.read_verdict(real)
    assert "BLOCKED" not in v, v
    assert "one file at a time" in v
    # ... and it must say that probe 10's failure is about probe 10, not about Files/.
    assert "about probe 10" in v
