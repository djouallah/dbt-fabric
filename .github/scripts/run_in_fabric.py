#!/usr/bin/env python3
"""The dbt commands ONE engine runs INSIDE the throwaway Fabric notebook that remote_dbt.py
launches. Mirrors the local leg in build.yml exactly -- `dbt build || dbt retry`, then the
parity fingerprint -- so a remote leg and a local leg are the same run on different compute.

    python .github/scripts/run_in_fabric.py <duckrun | ducklake | iceberg>

TWO WAYS TO INVOKE dbt, because the repo holds two dbt major versions:

  duckrun, ducklake   dbt-core 1.x, in-process through dbtRunner. Keeping it in-process is
                      worth it here: the notebook kernel already has the adapter imported,
                      and res.exception gives a real traceback in the streamed log.
  iceberg             dbt OSS 2, as a SUBPROCESS of the `dbt` binary. v2 is a Rust engine
                      behind a thin launcher and ships no dbtRunner equivalent, so there is
                      nothing to import. It is also a different project directory (dbt2/).

THE FILE NAME MUST NOT START WITH `dbt_`. dbt 1.x's plugin manager imports every importable
module whose name starts with dbt_ when a command starts, and this script's own directory is
sys.path[0] -- so as dbt_in_fabric.py it was imported as a "plugin" during `dbt build` and
re-executed itself recursively inside dbt's own invoke.

Runs as a subprocess of the notebook kernel with cwd = the unpacked repo. The tokens are
already in the environment (remote_dbt.py's setup hook minted them from notebookutils); the
config came across as env vars; stdout is streamed back to the runner.
"""
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

# engine -> (project directory, dbt major version). Keep in step with
# .github/scripts/check_gating.py's ENGINES.
PROJECT = {"duckrun": "dbt1", "ducklake": "dbt1", "iceberg": "dbt2"}

engine = sys.argv[1]
if engine not in PROJECT:
    sys.exit(f"usage: run_in_fabric.py [{' | '.join(PROJECT)}]")
project_dir = REPO / PROJECT[engine]

# DuckDB spill: the harness put TMPDIR on the notebook's ~135 GiB work disk; /tmp is a ~19 GiB
# overlay. dbt1's on-run-start hooks and dbt2's read DUCKDB_TEMP_DIR.
os.environ["DUCKDB_TEMP_DIR"] = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb_spill")
os.makedirs(os.environ["DUCKDB_TEMP_DIR"], exist_ok=True)
# Inside Fabric DuckDB's default OneLake transport is the one that works (curl is the fix for
# a runner off Fabric). remote_dbt.py does not forward it; belt and braces.
os.environ.pop("AZURE_TRANSPORT_OPTION_TYPE", None)
os.environ.pop("CURL_CA_INFO", None)

BASE = ["--target", engine, "--profiles-dir", "."]


def run_v1(*args) -> bool:
    """dbt-core 1.x, in-process. cwd is the project dir, which is how dbt finds the project."""
    from dbt.cli.main import dbtRunner  # imported late: after the env is set

    print(f"=== dbt {' '.join(args)} ({engine}) ===", flush=True)
    t0 = time.time()
    res = dbtRunner().invoke([*args, *BASE])
    print(f"=== dbt {args[0]}: success={res.success} in {int(time.time() - t0)}s ===", flush=True)
    if res.exception:
        print(res.exception, flush=True)
    return bool(res.success)


def run_v2(*args) -> bool:
    """dbt OSS 2, as a subprocess.

    `dbt` is resolved next to THIS interpreter first. pip installed it into the notebook
    kernel's environment, whose scripts directory is not necessarily on the PATH a
    subprocess inherits -- shutil.which alone found nothing on the first attempt.

    stdout/stderr are inherited rather than captured so the log streams back live and the
    fingerprint JSON lands in remote_dbt.py's captured log, exactly as for the v1 legs.
    """
    exe = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
    dbt = str(exe) if exe.exists() else shutil.which("dbt")
    if dbt is None:
        print("no `dbt` executable found -- is dbt-oss installed?", flush=True)
        return False

    print(f"=== dbt {' '.join(args)} ({engine}) ===", flush=True)
    t0 = time.time()
    # stdin closed: an interactive prompt on a runner must DIE, not wait.
    r = subprocess.run([dbt, *args, *BASE], cwd=project_dir,
                       stdin=subprocess.DEVNULL)
    ok = r.returncode == 0
    print(f"=== dbt {args[0]}: success={ok} in {int(time.time() - t0)}s ===", flush=True)
    return ok


run = run_v2 if PROJECT[engine] == "dbt2" else run_v1

if PROJECT[engine] == "dbt1":
    # dbtRunner has no --project-dir equivalent that also moves the profiles lookup, and
    # --profiles-dir . is relative, so put the process in the project.
    os.chdir(project_dir)

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
