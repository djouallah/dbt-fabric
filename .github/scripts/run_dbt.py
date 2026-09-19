#!/usr/bin/env python3
"""The dbt commands ONE engine runs, WHEREVER that engine runs.

    python .github/scripts/run_dbt.py <duckrun | iceberg | ducklake> [--no-fingerprint]

TWO CALLERS, ONE SEQUENCE -- build, the resume loop, a guarded `dbt retry`, the parity
fingerprint -- so a leg on Fabric compute and a leg on the GitHub runner are the same run on
different hardware, not two implementations that agree by inspection:

  * remote_dbt.py, as the `entry` of a throwaway Fabric notebook. The steady state: DuckDB
    folds the AEMO archive in memory and the 7 GB hosted runner was shut down mid-fct_scada
    twice in one day.
  * pipeline.yml's "dbt build on this runner" step, when the `local_runner` input is on.

It used to be run_in_fabric.py, and the runner ran a bare `dbt build || dbt retry` instead.
That bare form is why ducklake could not go local at all: its catalog is an auto-pausing
Fabric SQL DB, the first connection after a pause is SQL 40613, and dbt dies at the ATTACH
with no run_results.json for `dbt retry` to heal. See RESUME_SIGNATURE below.

NOTHING HERE ASKS WHERE IT IS RUNNING. The environment is the caller's business -- see the
DUCKDB_TEMP_DIR note below -- so there is no "am I in Fabric" test to get wrong.

ALL THREE ARE dbt-core 1.x, INVOKED IN-PROCESS through dbtRunner. Keeping it in-process is
worth it here: the notebook kernel already has the adapter imported, and res.exception gives a
real traceback in the streamed log. There used to be a second path -- iceberg spent a day on
dbt OSS 2, a Rust engine with no dbtRunner to import, launched as a subprocess out of a
separate dbt2/ project. Both are gone with it.

THE FILE NAME MUST NOT START WITH `dbt_`. dbt 1.x's plugin manager imports every importable
module whose name starts with dbt_ when a command starts, and this script's own directory is
sys.path[0] -- so as dbt_in_fabric.py it was imported as a "plugin" during `dbt build` and
re-executed itself recursively inside dbt's own invoke.

dwh and spark do NOT come through here. dbt is only a client for them -- the compute is the
Warehouse / the Livy session -- so they have no in-Fabric path and therefore no divergence to
close, and moving them off a `dbt` subprocess onto dbtRunner is a change nothing offline can
judge (it would also take away check_gating.py's DBT_BIN escape hatch, which needs an
executable). PROJECT below is the shape that would take them if that is ever decided.

In Fabric it runs as a subprocess of the notebook kernel with cwd = the unpacked repo, and the
tokens are already in the environment (remote_dbt.py's setup hook minted them from
notebookutils); on the runner pipeline.yml mints them into $GITHUB_ENV. Either way the config
arrives as env vars and stdout is streamed to the log.
"""
import os
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

# engine -> project directory. The DuckDB SUBSET, not all five (see the docstring): tests_py's
# _layout.PROJECT_OF holds every engine, check_gating.py's ENGINES holds every engine, and
# tests_py/test_local_runner.py asserts this subset and pipeline.yml's dwh/spark build step
# partition the five between them -- an engine in neither reaches no build step at all, which
# is silent and green.
PROJECT = {"duckrun": "dbt1", "iceberg": "dbt1", "ducklake": "dbt1"}

argv = sys.argv[1:]
# DEFAULT ON, and the polarity is load-bearing. The Fabric path and the demo notebook need the
# fingerprint in THIS process, because remote_dbt.py lifts its JSON back out of the streamed
# notebook log; the runner passes --no-fingerprint and runs the same run-operation as a step of
# its own, the one every local leg (dwh and spark too) already shares. An opt-IN flag would
# mean a caller who forgets it publishes no fingerprint at all -- and a missing
# history/parity/<engine>.json is invisible, because parity.py compare just reports it has
# fewer than two to compare and exits 0. Forgetting an opt-OUT costs one redundant query.
fingerprint = "--no-fingerprint" not in argv
engine = next((a for a in argv if not a.startswith("--")), "")
if engine not in PROJECT:
    sys.exit(f"usage: run_dbt.py [{' | '.join(PROJECT)}] [--no-fingerprint]")
project_dir = REPO / PROJECT[engine]

# DuckDB spill. THE CALLER OWNS THIS: pipeline.yml sets DUCKDB_TEMP_DIR at job level for a
# runner leg, and in Fabric nothing does -- remote_dbt.py's FORWARD allowlist deliberately
# leaves it behind -- so there it is derived from TMPDIR, which the harness put on the
# notebook's ~135 GiB work disk (/tmp is a ~19 GiB overlay). setdefault is the whole branch.
# The duckrun and iceberg on-run-start hooks read it.
os.environ.setdefault(
    "DUCKDB_TEMP_DIR", os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb_spill")
)
os.makedirs(os.environ["DUCKDB_TEMP_DIR"], exist_ok=True)

# NOTHING IS POPPED OUT OF THE ENVIRONMENT HERE. This script used to delete
# AZURE_TRANSPORT_OPTION_TYPE and CURL_CA_INFO as belt and braces -- inside Fabric DuckDB's
# default OneLake transport is the one that works -- which is the OPPOSITE of true on a GitHub
# runner, where pipeline.yml sets both at job level and DuckDB fails the OneLake TLS handshake
# without them ("Problem with the SSL CA cert (path? access rights?)"). They never reach Fabric
# in the first place: remote_dbt.py's FORWARD allowlist is the guarantee, and
# tests_py/test_local_runner.py pins it -- a test that fails before a run is spent, where the
# pop only ever masked the mistake.

BASE = ["--target", engine, "--profiles-dir", "."]


# The text of the last failure, for RESUMING below.
LAST_ERROR = ""


def run(*args) -> bool:
    """dbt-core 1.x, in-process. cwd is the project dir, which is how dbt finds the project."""
    global LAST_ERROR
    from dbt.cli.main import dbtRunner  # imported late: after the env is set

    print(f"=== dbt {' '.join(args)} ({engine}) ===", flush=True)
    t0 = time.time()
    res = dbtRunner().invoke([*args, *BASE])
    print(f"=== dbt {args[0]}: success={res.success} in {int(time.time() - t0)}s ===", flush=True)
    LAST_ERROR = "" if res.success else str(res.exception or "")
    if res.exception:
        print(res.exception, flush=True)
    return bool(res.success)


# dbtRunner has no --project-dir equivalent that also moves the profiles lookup, and
# --profiles-dir . is relative, so put the process in the project.
os.chdir(project_dir)

# A PAUSED SERVERLESS DATABASE IS NOT A BUILD FAILURE -- WAIT FOR IT.
#
# ducklake's catalog is a Fabric SQL DB, which auto-pauses when idle. The first connection
# after that gets SQL error 40613 ("Database ... is not currently available. Please retry the
# connection later"), WHICH IS THE SERVER ASKING US TO RETRY: the resume is already under way
# and takes about a minute. dbt has no notion of this and dies at the ATTACH, in ~26 seconds,
# before a single model runs -- and `dbt retry` below cannot help, because a crash at
# connection open writes no run_results.json to retry from. Observed on run 35294987062, the
# first ducklake run after a 12-hour gap; every earlier run was close enough behind the last
# that the database was still awake.
#
# Only this signature, and only the FIRST build: any other failure is a real one and must
# stay fast. Retrying the whole `dbt build` rather than probing the database first is
# deliberate -- the probe that matters is the one dbt itself makes, with its own connection,
# its own token and its own extension.
RESUME_SIGNATURE = "40613"
RESUME_ATTEMPTS = 5
RESUME_WAIT_SECONDS = 60

ok = run("build")
for attempt in range(1, RESUME_ATTEMPTS + 1):
    if ok or RESUME_SIGNATURE not in LAST_ERROR:
        break
    print(f"=== catalog database is resuming (SQL {RESUME_SIGNATURE}); waiting "
          f"{RESUME_WAIT_SECONDS}s and rebuilding, attempt {attempt}/{RESUME_ATTEMPTS} ===",
          flush=True)
    time.sleep(RESUME_WAIT_SECONDS)
    ok = run("build")

if not ok and (project_dir / "target" / "run_results.json").exists():
    # Same as the workflow's `|| dbt retry`: heals the tail of a run that lost a few nodes.
    # Only when the build got far enough to write run_results.json -- a crash at connection
    # open leaves nothing to retry, and `dbt retry` then just fails a second time.
    ok = run("retry")
if not ok:
    sys.exit(1)

if not fingerprint:
    sys.exit(0)
# Last, so its JSON is the tail of the log remote_dbt.py hands to parity.py capture -- and
# only after a green build above: the run-operation reads the gold TABLE, not the run, so
# taken after a failure it reports the previous run's numbers and parity.py would grade this
# run on them.
sys.exit(0 if run("run-operation", "parity_fingerprint") else 1)
