"""Pin the in-Fabric runner's wait for a paused serverless catalog database.

ducklake's DuckLake catalog is a Fabric SQL DB, which auto-pauses when idle. The first
connection after that returns SQL error 40613 -- the server saying "resuming, retry shortly".
dbt dies at the ATTACH in ~26 seconds, before any model runs, and `dbt retry` cannot help: a
crash at connection open writes no run_results.json. Run 35294987062 lost the whole ducklake
leg to this after a 12-hour gap, with no code change involved.

The runner therefore rebuilds on that signature alone. There is no way to run a Fabric
notebook offline, so this pins the shape of that loop: it must key on 40613, wait, and re-run
`build` (not `retry`, which has nothing to resume from).

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import re

from _layout import REPO

SCRIPT = REPO / ".github" / "scripts" / "run_in_fabric.py"


def source() -> str:
    return SCRIPT.read_text(encoding="utf-8")


def test_resume_signature_is_the_sql_error_code():
    """40613 is 'database is not currently available / resuming'. Anything broader would
    swallow real failures and turn a fast red build into a slow one."""
    m = re.search(r'RESUME_SIGNATURE\s*=\s*"([^"]+)"', source())
    assert m, "run_in_fabric.py has no RESUME_SIGNATURE"
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
