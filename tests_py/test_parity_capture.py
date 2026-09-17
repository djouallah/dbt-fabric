"""parity.py must lift the fingerprint JSON out of BOTH dbt versions' logs.

The fingerprint is the measurement the whole repo exists to make, and it survives the trip
from the engine to history/parity/<engine>.json as text scraped out of a log. dbt 1.x and
dbt OSS 2 do not format log lines the same way -- v2 can put its own output on the line that
carries the closing brace -- so the scrape is version-sensitive in a way nothing else is.

It also fails QUIETLY in the worst way: a leg that builds perfectly and then fails to yield
a fingerprint looks like a broken engine, and `parity.py compare` simply sees one fewer
engine and still says OK. These cases are cheap; the round trip to Fabric is not.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import json
import sys

import pytest

from _layout import REPO

sys.path.insert(0, str(REPO / ".github" / "scripts"))
import parity  # noqa: E402

FINGERPRINT = {
    "engine": "iceberg", "adapter": "duckdb", "model": "fct_summary",
    "rows_total": 32994039, "duids": 393, "days": 845,
    "date_min": "2023-01-01", "date_max": "2026-09-16",
    "mw_sum": 1234.5, "price_sum": 678.9,
}

BODY = json.dumps(FINGERPRINT, indent=2)

# Real log shapes, both taken from runs rather than invented.
LOGS = {
    # dbt-core 1.12.5 in the Fabric notebook: the brace sits alone on its own line.
    "v1_plain": f"22:52:40  Running with dbt=1.12.5\n{BODY}\n22:52:41  Done.\n",
    # The same with dbt's ANSI-reset + timestamp ahead of the opening brace, which is what
    # the notebook legs actually emit.
    "v1_ansi": f"\x1b[0m22:52:40  Registered adapter: duckdb=1.11.0\n{BODY}\n",
    # dbt OSS 2: engine output can share the closing line.
    "v2_suffix": f"12:36:13 Running with dbt=2.0.4\n{BODY[:-1]}\n}}  Finished 'run-operation'\n",
    # v2 with an ANSI-reset prefix, the way the demo run's log came back.
    "v2_prefixed": "\x1b[0m12:36:13 \n" + BODY + "\n\x1b[0m12:36:14 Done.\n",
}


@pytest.mark.parametrize("name", sorted(LOGS))
def test_capture_finds_the_fingerprint(name, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", __import__("io").StringIO(LOGS[name]))
    assert parity.capture(tmp_path) == 0, f"{name}: capture() found no fingerprint"

    written = tmp_path / "iceberg.json"
    assert written.is_file(), f"{name}: nothing written"
    got = json.loads(written.read_text(encoding="utf-8"))
    assert got["engine"] == "iceberg"
    assert got["rows_total"] == FINGERPRINT["rows_total"]
    capsys.readouterr()


def test_remote_legs_feed_capture_the_raw_log():
    """remote_dbt.py must hand parity.py `result.log`, not the text it printed.

    duckrun streams the notebook's output to the Actions log with a `[remote] ` prefix on
    every line. That copy is for humans: prefixing the fingerprint's own lines would make
    the captured text invalid JSON. The raw `result.log` is what has to reach capture(),
    and the distinction is invisible until a leg builds fine and yields no measurement.
    """
    src = (REPO / ".github" / "scripts" / "remote_dbt.py").read_text(encoding="utf-8")
    assert "input=result.log" in src, (
        "remote_dbt.py no longer passes the RAW notebook log to parity.py capture -- if it "
        "now passes the printed/prefixed copy, every remote leg silently stops producing a "
        "fingerprint while still reporting success"
    )


def test_capture_reports_failure_when_there_is_no_fingerprint(tmp_path, monkeypatch, capsys):
    """A build that died before the run-operation must be a non-zero exit, not an empty file.

    remote_dbt.py turns this into "build succeeded but no fingerprint was captured" and
    fails the leg -- which is the only thing standing between a silent measurement gap and a
    parity run that compares four engines and calls it agreement.
    """
    monkeypatch.setattr("sys.stdin", __import__("io").StringIO("22:00:00  Done. PASS=60\n"))
    assert parity.capture(tmp_path) == 1
    assert not list(tmp_path.glob("*.json"))
    capsys.readouterr()
