"""Offline tests for the run record. No Fabric, no network, no credentials.

What these pin is the part that fails SILENTLY. A fragment that never lands, a leg-end that
replaces the leg-start instead of merging onto it, a stale fingerprint folded in from
history/parity/, a merge order that drops a field -- none of it is an error. Each produces a
record that looks fine and attributes capacity units to the wrong run, or to nothing.

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
    """provision.py and remote_dbt.py must stay runnable by hand -- recording is opt-in."""
    assert record.merge({"items": {"A": {}}}) is None
    assert record.item("A", "data", "Lakehouse", "dbt") is None
    assert record.leg("duckrun", started="T0") is None


def test_item_writes_under_the_upper_cased_guid(tmp_path, monkeypatch):
    p = tmp_path / "frag.json"
    monkeypatch.setenv("RUN_RECORD", str(p))
    record.item("abc-DEF", "warehouse", "Warehouse", "dbt_dwh", engine="dwh")
    doc = _doc(p)
    # The metrics model returns item ids upper-cased; normalising at write time is what makes
    # the join a dict lookup instead of a case-insensitive scan.
    assert list(doc["items"]) == ["ABC-DEF"]
    assert doc["items"]["ABC-DEF"] == {"role": "warehouse", "kind": "Warehouse",
                                       "name": "dbt_dwh", "engine": "dwh"}


def test_a_blank_guid_is_dropped_not_recorded_as_null(tmp_path, monkeypatch):
    """A null key would join to nothing and read as an item nobody can find."""
    monkeypatch.setenv("RUN_RECORD", str(tmp_path / "frag.json"))
    assert record.item(None, "compute", "Notebook", "dbt-spark-1") is None
    assert not (tmp_path / "frag.json").exists()


def test_compute_guids_are_upper_cased_and_blanks_dropped(tmp_path, monkeypatch):
    p = tmp_path / "frag.json"
    monkeypatch.setenv("RUN_RECORD", str(p))
    record.leg("spark", compute=["abc", None, ""])
    assert _doc(p)["legs"]["spark"]["compute"] == ["ABC"]


def test_leg_end_merges_onto_leg_start_rather_than_replacing_it(tmp_path, monkeypatch):
    """The leg-end step carries only `finished`/`outcome`; `started` and `compute` must survive.

    This is the whole reason `legs` is a dict keyed by engine and not a list -- a deep merge
    unions dicts and REPLACES lists, so a list would have made the last writer win the entry.
    """
    p = tmp_path / "frag.json"
    monkeypatch.setenv("RUN_RECORD", str(p))
    record.leg("dwh", started="2026-09-17T03:00:00Z", compute=["W1"])
    record.leg("dwh", finished="2026-09-17T03:40:00Z", outcome="success")
    assert _doc(p)["legs"]["dwh"] == {"started": "2026-09-17T03:00:00Z", "compute": ["W1"],
                                      "finished": "2026-09-17T03:40:00Z", "outcome": "success"}


def test_the_cli_writes_the_leg_window(tmp_path, monkeypatch):
    p = tmp_path / "frag.json"
    monkeypatch.setenv("RUN_RECORD", str(p))
    assert record.main(["leg-start", "duckrun"]) == 0
    assert record.main(["leg-end", "duckrun", "failure"]) == 0
    lg = _doc(p)["legs"]["duckrun"]
    assert lg["outcome"] == "failure"
    assert lg["started"].endswith("Z") and lg["finished"].endswith("Z")
    assert lg["started"] <= lg["finished"]


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
                                           "inputs": {"engines": "all"},
                                           "items": {"L": {"role": "landing", "name": "dbt_landing"}}})
    _frag(tmp_path, "record-20-build-dwh.json",
          {"items": {"W": {"role": "warehouse", "name": "dbt_dwh"}},
           "legs": {"dwh": {"started": "T1", "finished": "T2", "compute": ["W"]}}})
    _frag(tmp_path, "record-20-build-spark.json",
          {"legs": {"spark": {"started": "T1", "compute": ["D"]}}})
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

    assert sorted(doc["items"]) == ["L", "W"]
    assert sorted(doc["legs"]) == ["dwh", "spark"]
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


def test_compute_items_accumulate_within_a_leg(tmp_path, monkeypatch):
    """ducklake's compute is TWO items written by two scripts in one job -- the catalog SQL DB
    from provision.py, the notebook from remote_dbt.py -- and the merge replaces lists, so
    `leg()` has to union them itself. Upper-cased, de-duplicated, order kept."""
    p = tmp_path / "frag.json"
    monkeypatch.setenv("RUN_RECORD", str(p))
    record.leg("ducklake", compute=["db1"])
    record.leg("ducklake", started="T0")
    record.leg("ducklake", compute=["nb1", "DB1"])
    assert _doc(p)["legs"]["ducklake"] == {"compute": ["DB1", "NB1"], "started": "T0"}
