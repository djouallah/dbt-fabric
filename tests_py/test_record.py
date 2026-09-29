"""Offline tests for the run record. No Fabric, no network, no credentials.

What these pin is the part that fails SILENTLY. A fragment that never lands, a stale
fingerprint folded in from history/parity/, a merge order that drops a field -- none of it is
an error. Each produces a record that looks fine and says the wrong thing.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import json
import os
import sys

import pytest

from _layout import REPO

sys.path.insert(0, str(REPO / ".github" / "scripts"))
import record  # noqa: E402


@pytest.fixture(autouse=True)
def _no_ambient_record(monkeypatch):
    """RUN_RECORD leaking in from the environment would make every test write to one real file."""
    monkeypatch.delenv("RUN_RECORD", raising=False)


def _doc(p):
    return json.loads(p.read_text(encoding="utf-8"))


def test_unset_run_record_is_a_no_op():
    """provision.py must stay runnable by hand -- recording is opt-in."""
    assert record.merge({"layout": {}}) is None


def test_fragments_sort_by_basename_not_by_path(tmp_path):
    """download-artifact nests each artifact in its OWN directory, so the full paths sort by
    artifact name and the numeric prefix's ordering would be lost."""
    for d, n in (("z-artifact", "record-00-run.json"), ("a-artifact", "record-30-layout.json")):
        (tmp_path / d).mkdir()
        (tmp_path / d / n).write_text("{}", encoding="utf-8")
    assert [os.path.basename(f) for f in record.fragments([str(tmp_path)])] == [
        "record-00-run.json", "record-30-layout.json"]


def _frag(tmp_path, name, obj):
    d = tmp_path / "fragments" / name.replace(".json", "")
    d.mkdir(parents=True)
    (d / name).write_text(json.dumps(obj), encoding="utf-8")


def test_finish_merges_every_fragment_and_folds_this_runs_fingerprints(tmp_path):
    _frag(tmp_path, "record-00-run.json", {"schema": 1, "run": {"id": "1", "started": "T0"},
                                           "inputs": {"engines": "all"}})
    _frag(tmp_path, "record-30-layout.json",
          {"layout": {"stats": {"dwh": {"fct_summary": {"total_rows": 7}}}}})
    parity = tmp_path / "parity"
    parity.mkdir()
    (parity / "dwh.json").write_text(json.dumps({"engine": "dwh", "rows_total": 7}),
                                     encoding="utf-8")
    (parity / "junk.json").write_text("[]", encoding="utf-8")

    dest = tmp_path / "out.json"
    record.finish(str(tmp_path / "fragments"), str(parity), str(dest))
    doc = _doc(dest)

    assert doc["inputs"] == {"engines": "all"}
    assert doc["layout"]["stats"]["dwh"]["fct_summary"]["total_rows"] == 7
    assert doc["parity"] == {"dwh": {"engine": "dwh", "rows_total": 7}}
    # started survives finished -- the run block is merged, not replaced.
    assert doc["run"]["started"] == "T0" and doc["run"]["finished"].endswith("Z")


def test_finish_without_a_parity_dir_omits_the_key_rather_than_emptying_it(tmp_path):
    """A single-engine run has no comparison. An empty `parity: {}` would read as 'compared and
    found nothing', which is a different statement."""
    _frag(tmp_path, "record-00-run.json", {"schema": 1})
    dest = tmp_path / "out.json"
    record.finish(str(tmp_path / "fragments"), None, str(dest))
    assert "parity" not in _doc(dest)
    record.finish(str(tmp_path / "fragments"), str(tmp_path / "absent"), str(dest))
    assert "parity" not in _doc(dest)


def test_init_reads_the_dispatch_inputs_and_skips_blanks(tmp_path, monkeypatch):
    monkeypatch.setenv("RUN_RECORD", str(tmp_path / "frag.json"))
    monkeypatch.setenv("GITHUB_RUN_ID", "42")
    monkeypatch.setenv("RUNIN_ENGINES", "all")
    # An input left blank is absent, not recorded as "": the record states what the run chose.
    monkeypatch.setenv("RUNIN_PROCESS_LIMIT", "")
    record._init()
    doc = _doc(tmp_path / "frag.json")
    assert doc["inputs"] == {"engines": "all"}
    assert doc["run"]["id"] == "42" and doc["run"]["started"].endswith("Z")
