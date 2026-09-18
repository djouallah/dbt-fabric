#!/usr/bin/env python3
"""The dbt commands ONE engine runs INSIDE the throwaway Fabric notebook that remote_dbt.py
launches. Mirrors the local leg in pipeline.yml exactly -- `dbt build || dbt retry`, then the
parity fingerprint -- so a remote leg and a local leg are the same run on different compute.

    python .github/scripts/run_in_fabric.py <duckrun | iceberg | ducklake>

One thing it does that the local leg does not: it waits out a PAUSED serverless catalog
database (SQL error 40613) by rebuilding, rather than reporting it as a build failure. See
RESUME_SIGNATURE below.

ALL THREE ARE dbt-core 1.x, INVOKED IN-PROCESS through dbtRunner. Keeping it in-process is
worth it here: the notebook kernel already has the adapter imported, and res.exception gives a
real traceback in the streamed log. There used to be a second path -- iceberg spent a day on
dbt OSS 2, a Rust engine with no dbtRunner to import, launched as a subprocess out of a
separate dbt2/ project. Both are gone with it.

THE FILE NAME MUST NOT START WITH `dbt_`. dbt 1.x's plugin manager imports every importable
module whose name starts with dbt_ when a command starts, and this script's own directory is
sys.path[0] -- so as dbt_in_fabric.py it was imported as a "plugin" during `dbt build` and
re-executed itself recursively inside dbt's own invoke.

Runs as a subprocess of the notebook kernel with cwd = the unpacked repo. The tokens are
already in the environment (remote_dbt.py's setup hook minted them from notebookutils); the
config came across as env vars; stdout is streamed back to the runner.
"""
import os
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

# engine -> project directory. Keep in step with .github/scripts/check_gating.py's ENGINES
# and tests_py/_layout.py's PROJECT_OF.
PROJECT = {"duckrun": "dbt1", "iceberg": "dbt1", "ducklake": "dbt1"}

engine = sys.argv[1]
if engine not in PROJECT:
    sys.exit(f"usage: run_in_fabric.py [{' | '.join(PROJECT)}]")
project_dir = REPO / PROJECT[engine]

# DuckDB spill: the harness put TMPDIR on the notebook's ~135 GiB work disk; /tmp is a ~19 GiB
# overlay. The duckrun and iceberg on-run-start hooks read DUCKDB_TEMP_DIR.
os.environ["DUCKDB_TEMP_DIR"] = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb_spill")
os.makedirs(os.environ["DUCKDB_TEMP_DIR"], exist_ok=True)
# Inside Fabric DuckDB's default OneLake transport is the one that works (curl is the fix for
# a runner off Fabric). remote_dbt.py does not forward it; belt and braces.
os.environ.pop("AZURE_TRANSPORT_OPTION_TYPE", None)
os.environ.pop("CURL_CA_INFO", None)

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

# Last, so its JSON is the tail of the log remote_dbt.py hands to parity.py capture.
sys.exit(0 if run("run-operation", "parity_fingerprint") else 1)
