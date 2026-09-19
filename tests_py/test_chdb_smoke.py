"""The chDB smoke test must stay a smoke test, and must never drop anything.

.github/workflows/chdb_smoke.yml probes whether chDB could become a SIXTH engine. It is not
one. The whole design rests on that: it touches no gating, no parity, no deploy, and it is
dispatch-only so it costs Fabric capacity only when somebody asks for it. That is a claim
about absence, which is the kind that rots quietly -- somebody adds requirements/chdb.txt, or
wires the workflow into pipeline.yml "so it runs with everything else", and the probe becomes
a leg nobody decided to build.

THE OTHER HALF OF THIS FILE IS A DATA-LOSS GUARD, and it matters more than the absence tests.
chDB's DatabaseDataLake::dropTable calls table->drop(), which unregisters the REAL table --
and every table in that catalog except the probe's own is a leg's gold layer. So the rule is
that nothing is ever dropped, with no exceptions: a rule with no exceptions cannot be typo'd
into deleting a mart. Probes 7 and 8 DO write, to one run-scoped table in a namespace no leg
uses, and test_writes_only_ever_target_the_probes_own_table keeps it there.

And the correctness tests, which exist because this probe answered the write question WRONG
once -- run 35434183971 published "VERDICT: READER ONLY" off a create that had never reached
the Iceberg writer. A create through a catalog can return success and register nothing; an
insert can return without landing a row. Both are graded on what the catalog shows
afterwards, never on the statement, and the probe has to get past `allow_insert_into_iceberg`
and a local storage engine before it is measuring chDB at all.

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


def test_the_probe_never_drops(smoke):
    """THE DATA-LOSS GUARD, and it has no exceptions.

    DatabaseDataLake::dropTable calls table->drop(), which unregisters the real table -- and
    every table in this catalog other than the probe's own is a leg's gold layer. So there is
    no DROP at all rather than a careful DROP: a rule with no exceptions cannot be typo'd into
    deleting a mart. The cost is a small table left behind per run, which is why the probe
    table's name is run-scoped.
    """
    statements = " ".join(executed_sql(smoke)).lower()
    for anchor in ("select count()", "show tables from", "create database"):
        assert anchor in statements, (
            f"{anchor!r} is missing, so this test is no longer reading the statements the "
            f"script runs -- it would pass on a file that dropped everything"
        )
    for banned in ("drop table", "drop database", "truncate table", "alter table",
                   "optimize table"):
        assert banned not in statements, (
            f"chdb_smoke.py issues {banned!r}. Nothing here drops anything: the catalog's "
            f"drop path unregisters the real table, and the rest of this catalog is the "
            f"legs' gold layers."
        )


def test_writes_only_ever_target_the_probes_own_table(smoke):
    """Probes 7 and 8 write. Everything they write goes to one run-scoped table in a
    namespace no leg uses -- the legs' marts are not a test fixture."""
    writes = [st for st in executed_sql(smoke)
              if "INSERT INTO" in st or "CREATE TABLE" in st]
    assert writes, "the write probes vanished; 7 and 8 are what decide a leg is possible"
    for st in writes:
        assert smoke.PROBE_TABLE in st, f"a write targets something else: {st}"

    assert smoke.PROBE_TABLE.startswith(smoke.SCHEMA + "."), (
        "the write target must sit in the probe's own namespace, not a leg's"
    )
    for engine in ENGINES:
        assert not smoke.PROBE_TABLE.startswith(f"{engine}_"), (
            f"the write target is inside {engine}'s namespace"
        )
    # RUN-SCOPED, because nothing is ever dropped: a fixed name would be created once and
    # every later run would measure "it already exists" instead of "the create worked".
    assert smoke.PROBE_TABLE.rsplit("_", 1)[-1], smoke.PROBE_TABLE

def test_the_create_reaches_the_iceberg_writer(smoke):
    """THE TWO THINGS THAT MADE THIS PROBE ANSWER WRONG.

    It reported "VERDICT: READER ONLY" (run 35434183971) because the create never reached the
    Iceberg writer: no `allow_insert_into_iceberg`, which gates the whole write path including
    the initial create, and `ENGINE = Memory` -- added to dodge an unrelated MergeTree error --
    which routed the statement into a local storage engine. What answered was the empty
    IDatabase::createTable hook, which is not the create path at all: a datalake database
    holds no table metadata of its own, so creation goes through catalog->createTable.

    ClickHouse's support matrix lists OneLake as one of three catalogs that are NOT read-only.
    """
    creates = [sql for _, _, sql, _ in smoke.phase_a_probes("some_mart.some_table", [])
               if isinstance(sql, str) and "CREATE TABLE" in sql]
    assert len(creates) == 1, f"the write attempts changed: {creates}"
    assert "ENGINE = Iceberg" in creates[0], creates[0]
    assert "Memory" not in creates[0], (
        "ENGINE = Memory never reaches the Iceberg writer; it measures a local table"
    )
    assert smoke.WRITE_GATE == "allow_insert_into_iceberg"
    src = SMOKE_PY.read_text(encoding="utf-8")
    assert 'run(sess, f"SET {WRITE_GATE}=1")' in src, (
        "the write gate is never opened, so probes 7 and 8 are refused and the refusal reads "
        "as 'OneLake is read-only'"
    )
    assert smoke.PROBE_TABLE.startswith(smoke.SCHEMA + "."), (
        "probe 7 must create inside the probe's own namespace, not a leg's"
    )
    for engine in ENGINES:
        assert not smoke.PROBE_TABLE.startswith(f"{engine}_"), (
            f"probe 7 writes into {engine}'s namespace"
        )


# ---- the probes measure what they claim to -----------------------------------------------

def test_a_write_that_did_not_land_is_never_reported_as_pass(smoke):
    """Neither write probe may be answered by its own statement.

    A create through a catalog can return success and register nothing; an insert can return
    without landing a row. Both look identical to the real thing from the statement alone, so
    7 re-lists the catalog and 8 reads the table back -- and what those return is what
    interpret() grades.
    """
    absent = [("iceberg_mart.fct_price",), ("duckrun_mart.fct_summary",)]
    assert smoke.interpret(7, absent).startswith("SILENT NO-OP")
    present = absent + [(smoke.PROBE_TABLE,)]
    assert smoke.interpret(7, present).startswith("PASS")

    assert smoke.interpret(8, [(1, 10), (2, 20)]).startswith("PASS")
    assert smoke.interpret(8, []).startswith("SILENT NO-OP")
    assert smoke.interpret(8, [(1, 10)]).startswith("MISMATCH")


def test_a_silent_write_outranks_the_happy_path(smoke):
    """The worst outcome has to be checked first.

    A create that registered plus an insert that vanished would otherwise read as a partial
    success rather than as "do not build on this".
    """
    base = [(1, "x", "PASS"), (2, "x", "PASS (21 table(s))"), (4, "x", "PASS (1 row(s))"),
            (10, "x", "PASS"), (11, "x", "PASS (1 row(s))")]
    v = smoke.read_verdict(base + [(7, "x", "PASS -- registered"),
                                   (8, "x", "SILENT NO-OP -- the table is EMPTY")])
    assert "SUSPECT" in v
    # ... and it must not resurrect the conclusion it got wrong before.
    assert "READER ONLY" not in v
    assert "allow_insert_into_iceberg" in v


def test_read_and_write_is_the_verdict_when_both_land(smoke):
    """The distinction the job now turns on: reading the gold layer is useful, writing it is
    what makes a sixth ENGINE possible at all."""
    base = [(1, "x", "PASS"), (2, "x", "PASS (21 table(s))"), (4, "x", "PASS (1 row(s))"),
            (10, "x", "PASS"), (11, "x", "PASS (1 row(s))")]
    v = smoke.read_verdict(base + [(7, "x", "PASS -- registered"),
                                   (8, "x", "PASS -- both rows read back")])
    assert "READ AND WRITE" in v
    assert "INGEST:" in v, "phase B must still speak for itself; access is not ingest"

    # Create without insert is its own answer: every fact model here is an incremental append.
    half = smoke.read_verdict(base + [(7, "x", "PASS -- registered"),
                                      (8, "x", "FAIL - not supported")])
    assert "CREATE YES, INSERT NO" in half
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
    assert "reads the real ragged AEMO CSV" in v
    # ... and it must say that probe 10's failure is about probe 10, not about Files/.
    assert "about probe 10" in v


def test_the_read_shape_is_probe_18s_answer_not_an_assumption(smoke):
    """The verdict asserted "ONE url with no glob and no listing" for a whole run without ever
    having tried a glob -- and url() expands {a,b} client-side, which is exactly the shape
    spark_read_csv.sql emits. An untested limit stated as a finding is what this file exists
    to not do, so the three outcomes have to read differently.
    """
    base = [(1, "x", "PASS"), (2, "x", "PASS (21 table(s))"), (4, "x", "PASS (1 row(s))"),
            (7, "x", "SILENT NO-OP"), (10, "x", "PASS"), (11, "x", "PASS (1 row(s))")]

    worked = smoke.read_verdict(base + [(18, "x", "PASS - ONE statement read 2 files")])
    assert "ONE statement" in worked and "ports" in worked

    didnt = smoke.read_verdict(base + [(18, "x", "PARTIAL - the alternation did not expand")])
    assert "one url per file" in didnt and "own enumeration" in didnt

    # And with no answer it must say UNMEASURED rather than pick one.
    unknown = smoke.read_verdict(base + [(18, "x", "SKIP - needs two landing files, found 1")])
    assert "UNMEASURED" in unknown


def test_the_brace_glob_names_the_files_rather_than_wildcarding(smoke):
    """A folder wildcard would be a different and easier question.

    spark_read_csv.sql names this run's files explicitly so a run folds exactly what it decided
    to fold and a backlog converges instead of restarting. A `*` here would pass while proving
    nothing about that shape.
    """
    g = smoke.brace_glob(["item/Files/csv_raw/daily/A.CSV", "item/Files/csv_raw/daily/B.CSV"])
    assert g.endswith("/{A.CSV,B.CSV}"), g
    assert "*" not in g

    # It must also be SKIPPED, not faked, when the zone holds only one file -- a one-file
    # "glob" is indistinguishable from no glob at all.
    one = {n: sql for n, _, sql, _ in smoke.model_shape_probes(["item/Files/csv_raw/A.CSV"])}
    assert one[18] == ""
    two = {n: sql for n, _, sql, _ in
           smoke.model_shape_probes(["item/Files/csv_raw/A.CSV", "item/Files/csv_raw/B.CSV"])}
    assert "{A.CSV,B.CSV}" in two[18]
    # GROUP BY _file, because a glob that read only the first file returns a plausible count.
    assert "_file" in two[18] and "GROUP BY" in two[18]


def test_a_one_file_glob_is_not_reported_as_a_working_glob(smoke):
    assert smoke.interpret(18, [("A.CSV", 10)]).startswith("PARTIAL")
    assert smoke.interpret(18, [("A.CSV", 10), ("B.CSV", 20)]).startswith("PASS")
