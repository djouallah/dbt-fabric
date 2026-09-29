#!/usr/bin/env python3
"""One step of the in-Fabric run. fabric_items/run.Notebook calls it; run_pipeline orders it.

    python fabric_run.py <land | dwh | spark | parity> <run_id>

    land  ->  dwh   ->  parity
          ->  spark ->

THE SAME COMMANDS pipeline.yml RAN ON A RUNNER, step for step -- provision.py, download_aemo.py,
`dbt build || dbt retry`, the parity fingerprint -- so there is no second implementation to
drift. What differs is only what a runner did around them:

  * provision.py's KEY=value stdout went to $GITHUB_ENV; here it goes into os.environ.
  * the fingerprints were handed over as artifacts; here they go to the landing lakehouse,
    under Files/parity/<run_id>/.
  * azure/login is notebookutils, which provision.py, onelake.py and both adapters already
    know how to use.

LANDING HAPPENS IN `land` AND NOWHERE ELSE. download_aemo.py rewrites
csv_raw_archive_log.parquet in place, so two legs landing at once race on that one file; and
one landing zone for both engines is what makes the parity comparison mean anything.
tests_py/test_fabric_items.py pins it.

EACH LEG IS ITS OWN NOTEBOOK SESSION, which is what lets both engines run at all: dbt-fabric
and dbt-fabricspark shadow each other under dbt.adapters and cannot share an environment.
dbt is a subprocess here for the same reason the adapter is pip-installed at run time -- the
notebook kernel never imports dbt, so there is nothing to restart.

THE FILE NAME MUST NOT START WITH `dbt_`. dbt's plugin manager imports every importable
module named dbt_* when a command starts, and this file's directory is the project root.

The configuration arrives as env vars (the notebook reads them from the deploy_config
Variable Library), so this also runs by hand after `az login`.
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parent
SCRIPTS = REPO / ".github" / "scripts"

ENGINES = ("dwh", "spark")
STEPS = ("land", *ENGINES, "parity")

# What each adapter calls "ask notebookutils for the token". Two names for one thing.
NOTEBOOK_AUTH = {"dwh": "notebookutils", "spark": "fabric_notebook"}


def log(msg: str) -> None:
    print(f"=== {msg} ===", flush=True)


def in_fabric() -> bool:
    try:
        import notebookutils  # type: ignore  # noqa: F401
    except ImportError:
        return False
    return True


def selected() -> list[str]:
    """The engines this run builds: the `engines` variable, `all` or one engine."""
    want = os.environ.get("engines", "all").strip() or "all"
    if want == "all":
        return list(ENGINES)
    if want not in ENGINES:
        raise SystemExit(f"engines={want!r}: expected all | {' | '.join(ENGINES)}")
    return [want]


def run(*cmd: str, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(list(cmd), cwd=REPO, check=True, **kw)


def pip(*names: str) -> None:
    """requirements/<name>.txt, into this session -- the same pins CI's gating job parsed with."""
    args = [a for n in names for a in ("-r", f"requirements/{n}.txt")]
    run(sys.executable, "-m", "pip", "install", "-q", *args)


def provision(what: str) -> None:
    """provision.py's KEY=value lines, into os.environ. Its diagnostics are on stderr."""
    out = run(sys.executable, str(SCRIPTS / "provision.py"), what,
              stdout=subprocess.PIPE, text=True).stdout
    os.environ.update(dict(line.split("=", 1) for line in out.splitlines() if "=" in line))


def dbt(*args: str, engine: str, **kw) -> subprocess.CompletedProcess:
    # `python -m`, not the `dbt` executable: pip may put that outside the notebook's PATH.
    return subprocess.run(
        [sys.executable, "-m", "dbt.cli.main", *args, "--target", engine, "--profiles-dir", "."],
        cwd=REPO, **kw)


def parity_store():
    """Files/parity in the landing lakehouse -- the one item both legs already write to."""
    sys.path.insert(0, str(REPO / "ingest"))
    from onelake import Store

    return Store(f"{os.environ['LANDING_PATH']}/parity")


def land() -> None:
    pip("ops")
    provision("landing")
    run(sys.executable, "ingest/download_aemo.py")


def build(engine: str, run_id: str) -> None:
    if engine not in selected():
        log(f"{engine} skipped: engines={os.environ.get('engines')}")
        return
    # ops as well: the fingerprint is uploaded through ingest/onelake.py.
    pip(engine, "ops")
    provision(engine)
    if in_fabric():
        # After provision(), which emits CLI for the runner it was written for.
        os.environ["FABRIC_AUTH"] = NOTEBOOK_AUTH[engine]
    # dbt-fabricspark 1.13.x runs OPTIMIZE after every Delta build, which rewrites the
    # layout the run just produced.
    os.environ.setdefault("DBT_FABRICSPARK_SKIP_OPTIMIZE", "true")

    log(f"dbt build ({engine})")
    # `dbt retry` needs --target too, or it renders the profile's default target.
    if dbt("build", engine=engine).returncode and dbt("retry", engine=engine).returncode:
        raise SystemExit(f"dbt build failed on {engine}, and so did the retry")

    # Only after a green build: the run-operation reads the gold TABLE, not the run, so taken
    # after a failure it would report the previous run's numbers.
    log(f"fingerprint ({engine})")
    fp = dbt("run-operation", "parity_fingerprint", engine=engine,
             stdout=subprocess.PIPE, text=True)
    if fp.returncode:
        print(fp.stdout, flush=True)
        raise SystemExit(f"parity_fingerprint failed on {engine}")
    with tempfile.TemporaryDirectory() as tmp:
        run(sys.executable, str(SCRIPTS / "parity.py"), "capture", tmp, input=fp.stdout, text=True)
        parity_store().push(tmp, run_id, overwrite=True)


def parity(run_id: str) -> None:
    engines = selected()
    if len(engines) < 2:
        log(f"parity skipped: only {engines[0]} was built")
        return
    pip("ops")
    provision("landing")
    store = parity_store()
    with tempfile.TemporaryDirectory() as tmp:
        # THIS RUN'S fingerprints, by run id. Files/parity also holds every earlier run's, and
        # a leg that failed this run must not be graded on a stale one.
        for engine in engines:
            data = store.read(f"{run_id}/{engine}.json")
            if data is None:
                raise SystemExit(f"no fingerprint for {engine} in run {run_id}")
            Path(tmp, f"{engine}.json").write_bytes(data)
        run(sys.executable, str(SCRIPTS / "parity.py"), "compare", tmp)


def main(argv: list[str]) -> int:
    if len(argv) != 3 or argv[1] not in STEPS:
        print(f"usage: fabric_run.py [{' | '.join(STEPS)}] <run_id>", file=sys.stderr)
        return 2
    step, run_id = argv[1], argv[2]
    log(f"{step} (run {run_id})")
    if step == "land":
        land()
    elif step == "parity":
        parity(run_id)
    else:
        build(step, run_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
