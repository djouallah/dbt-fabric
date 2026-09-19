"""Pin run_dbt.py -- the ONE build/resume/retry sequence both the Fabric notebook and the
GitHub runner execute.

ducklake's DuckLake catalog is a Fabric SQL DB, which auto-pauses when idle. The first
connection after that returns SQL error 40613 -- the server saying "resuming, retry shortly".
dbt dies at the ATTACH in ~26 seconds, before any model runs, and `dbt retry` cannot help: a
crash at connection open writes no run_results.json. Run 35294987062 lost the whole ducklake
leg to this after a 12-hour gap, with no code change involved.

The script therefore rebuilds on that signature alone. The loop is ducklake's fact, but it
lives in the script BOTH paths run, and that is what lets ducklake go local at all -- while
pipeline.yml's runner step was a bare `dbt build || dbt retry`, a paused catalog would have
killed a local ducklake leg outright.

There is no way to run a Fabric notebook offline, so everything here is pinned against the
source text: the loop must key on 40613, wait, and re-run `build` (not `retry`, which has
nothing to resume from); the retry must stay guarded; and the script must not fight the
environment its caller gave it.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import re

from _layout import REPO

SCRIPT = REPO / ".github" / "scripts" / "run_dbt.py"


def source() -> str:
    return SCRIPT.read_text(encoding="utf-8")


def test_resume_signature_is_the_sql_error_code():
    """40613 is 'database is not currently available / resuming'. Anything broader would
    swallow real failures and turn a fast red build into a slow one."""
    m = re.search(r'RESUME_SIGNATURE\s*=\s*"([^"]+)"', source())
    assert m, "run_dbt.py has no RESUME_SIGNATURE"
    assert m.group(1) == "40613", f"unexpected resume signature {m.group(1)!r}"


def test_resume_loop_rebuilds_and_is_bounded():
    src = source()
    loop = re.search(r"ok = run\(\"build\"\)\n(.*?)\nif not ok and", src, re.S)
    assert loop, "the build call and its resume loop are not where the test expects them"
    body = loop.group(1)
    assert "RESUME_SIGNATURE not in LAST_ERROR" in body, (
        "the loop must retry ONLY on the resume signature -- a bare retry hides real failures"
    )
    assert 'run("build")' in body, "the loop must re-run `build`, not `retry`"
    assert 'run("retry")' not in body, (
        "`dbt retry` cannot heal a crash at connection open: there is no run_results.json"
    )
    assert "time.sleep" in body, "the loop must wait between attempts"
    for name in ("RESUME_ATTEMPTS", "RESUME_WAIT_SECONDS"):
        assert re.search(rf"{name}\s*=\s*\d+", src), f"{name} must be a bounded constant"


def test_last_error_is_populated_on_failure():
    """The loop reads LAST_ERROR, so run_v1 must set it from the failed invocation."""
    src = source()
    assert re.search(r"LAST_ERROR = .*res\.success.*res\.exception", src), (
        "run_v1 must record the failure text in LAST_ERROR for the resume loop to read"
    )


def test_retry_is_guarded_on_run_results():
    """`dbt retry` after a crash at connection open cannot help -- there is no run_results.json
    -- and unguarded it fails a second time, burying the real ATTACH error under "Could not
    find previous run in 'target' directory". pipeline.yml's old `dbt build || dbt retry` did
    exactly that, and it is half of why ducklake could not run on the runner."""
    src = source()
    m = re.search(r"if not ok and (.*?):\n", src)
    assert m, "the guarded retry is not where this test expects it"
    guard = m.group(1)
    assert "run_results.json" in guard and ".exists()" in guard, (
        f"the retry's guard is {guard!r}; it must check that the build got far enough to write "
        f"run_results.json"
    )


def test_the_fingerprint_is_last_and_only_after_a_green_build():
    """The run-operation reads the gold TABLE, not the run. Taken after a failed build it
    reports the PREVIOUS run's numbers, and parity.py grades this run on them -- the quietest
    possible way to pass the acceptance test without having produced anything."""
    src = source()
    fp = src.index('run("run-operation", "parity_fingerprint")')
    assert src.index("sys.exit(1)") < fp, "the fingerprint is taken before the build's exit check"
    assert src.rindex('run("') == fp, "the fingerprint is not the last dbt command"


def test_the_fingerprint_can_be_skipped_but_is_on_by_default():
    """Default-OFF would mean a caller that forgets the flag publishes no fingerprint at all,
    which is the silent-green case (parity.py compare just reports it has fewer than two).
    Default-ON costs one redundant run-operation at worst, and lets remote_dbt.py and the demo
    notebook keep calling this with nothing but an engine name."""
    src = source()
    assert '"--no-fingerprint" not in argv' in src, (
        "the flag is opt-IN; it must be opt-OUT so the Fabric path needs no flag to fingerprint"
    )


def test_the_driver_does_not_fight_the_environment_it_is_given():
    """This script used to delete AZURE_TRANSPORT_OPTION_TYPE and CURL_CA_INFO on arrival --
    correct in a notebook, and the exact opposite on the GitHub runner, where pipeline.yml sets
    both on purpose and DuckDB fails the OneLake TLS handshake without them. One script runs in
    both places now, so it takes the environment as given; remote_dbt.py's FORWARD allowlist is
    what keeps the runner's settings out of Fabric (tests_py/test_local_runner.py)."""
    src = source()
    assert 'os.environ.setdefault(' in src and "DUCKDB_TEMP_DIR" in src, (
        "DUCKDB_TEMP_DIR must be a setdefault: the runner sets it at job level, and in Fabric "
        "nothing does, so it is derived from TMPDIR there"
    )
    assert "os.environ.pop(" not in src, (
        "a pop here silently breaks whichever of the two callers set the variable on purpose"
    )
