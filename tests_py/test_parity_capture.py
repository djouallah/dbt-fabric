"""The run notebook must lift the fingerprint JSON out of BOTH dbt versions' logs.

The fingerprint is the measurement the whole repo exists to make, and it survives the trip
from the engine to Files/parity/<run_id>/<engine>.json as text scraped out of a log. dbt 1.x
and dbt OSS 2 do not format log lines the same way -- v2 can put its own output on the line
that carries the closing brace -- so the scrape is version-sensitive in a way nothing else is.

A leg that builds perfectly and then fails to yield a fingerprint looks like a broken engine.
These cases are cheap; the round trip to Fabric is not.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import ast
import json
import re

import pytest

from _layout import REPO

NOTEBOOK = REPO / "fabric_items" / "run.Notebook" / "notebook-content.ipynb"

FINGERPRINT = {
    "engine": "spark", "adapter": "fabricspark", "model": "fct_summary",
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
    # what a dbt log line actually looks like.
    "v1_ansi": f"\x1b[0m22:52:40  Registered adapter: fabricspark=1.13.4\n{BODY}\n",
    # dbt OSS 2: engine output can share the closing line.
    "v2_suffix": f"12:36:13 Running with dbt=2.0.4\n{BODY[:-1]}\n}}  Finished 'run-operation'\n",
    # v2 with an ANSI-reset prefix, the way the demo run's log came back.
    "v2_prefixed": "\x1b[0m12:36:13 \n" + BODY + "\n\x1b[0m12:36:14 Done.\n",
}


def pattern() -> str:
    """FINGERPRINT_JSON, as the run notebook assigns it."""
    cells = json.loads(NOTEBOOK.read_text(encoding="utf-8"))["cells"]
    source = "\n".join("".join(c["source"]) for c in cells if c["cell_type"] == "code")
    m = re.search(r"^FINGERPRINT_JSON = (.+)$", source, re.M)
    assert m, "the run notebook assigns no FINGERPRINT_JSON"
    assert "re.search(FINGERPRINT_JSON, log.stdout, re.S)" in source
    return ast.literal_eval(m.group(1))


@pytest.mark.parametrize("name", sorted(LOGS))
def test_the_pattern_finds_the_fingerprint(name):
    found = re.search(pattern(), LOGS[name], re.S)
    assert found, f"{name}: no fingerprint found"
    assert json.loads(found.group(0)) == FINGERPRINT


def test_the_pattern_finds_nothing_when_there_is_no_fingerprint():
    """A build that died before the run-operation must fail the leg, not yield a fingerprint:
    that is what stands between a measurement gap and a parity run with one engine missing."""
    assert re.search(pattern(), "22:00:00  Done. PASS=60\n", re.S) is None
