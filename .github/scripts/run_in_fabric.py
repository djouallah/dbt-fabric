#!/usr/bin/env python3
"""The dbt commands ONE DuckDB-family engine runs INSIDE the throwaway Fabric notebook that
remote_dbt.py launches. Mirrors the local leg in build.yml exactly -- `dbt build || dbt retry`,
then the parity fingerprint -- so a remote leg and a local leg are the same run on different
compute.

    python .github/scripts/run_in_fabric.py <duckrun | ducklake | iceberg>

THE FILE NAME MUST NOT START WITH `dbt_`. dbt's plugin manager imports every importable
module whose name starts with dbt_ when a command starts, and this script's own directory is
sys.path[0] -- so as dbt_in_fabric.py it was imported as a "plugin" during `dbt build` and
re-executed itself recursively inside dbt's own invoke.

Runs as a subprocess of the notebook kernel with cwd = the unpacked repo. The tokens are already
in the environment (remote_dbt.py's setup hook minted them from notebookutils); the config came
across as env vars; stdout is streamed back to the runner.
"""
import os
import sys
import time

engine = sys.argv[1]

# DuckDB spill: the harness put TMPDIR on the notebook's ~135 GiB work disk; /tmp is a ~19 GiB
# overlay. The on-run-start hooks read DUCKDB_TEMP_DIR.
os.environ["DUCKDB_TEMP_DIR"] = os.path.join(os.environ.get("TMPDIR", "/tmp"), "duckdb_spill")
os.makedirs(os.environ["DUCKDB_TEMP_DIR"], exist_ok=True)
# Inside Fabric DuckDB's default OneLake transport is the one that works (curl is the fix for
# a runner off Fabric). remote_dbt.py does not forward it; belt and braces.
os.environ.pop("AZURE_TRANSPORT_OPTION_TYPE", None)
os.environ.pop("CURL_CA_INFO", None)

from dbt.cli.main import dbtRunner  # noqa: E402  (after the env is set)

dbt = dbtRunner()
base = ["--target", engine, "--profiles-dir", "."]


def run(*args):
    print(f"=== dbt {' '.join(args)} ({engine}) ===", flush=True)
    t0 = time.time()
    res = dbt.invoke([*args, *base])
    print(f"=== dbt {args[0]}: success={res.success} in {int(time.time() - t0)}s ===", flush=True)
    if res.exception:
        print(res.exception, flush=True)
    return res


build = run("build")
if not build.success and os.path.exists("target/run_results.json"):
    # Same as the workflow's `|| dbt retry`: heals the tail of a run that lost a few nodes.
    # Only when the build got far enough to write run_results.json -- a crash at connection
    # open leaves nothing to retry, and `dbt retry` then just fails a second time.
    build = run("retry")
if not build.success:
    sys.exit(1)

# Last, so its JSON is the tail of the log remote_dbt.py hands to parity.py capture.
fingerprint = run("run-operation", "parity_fingerprint")
sys.exit(0 if fingerprint.success else 1)
