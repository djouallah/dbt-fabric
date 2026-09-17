"""Offline tests for the CU ledger. No token, no network, no Fabric. ~1s.

Every rule here fails the same way when it is wrong: a plausible number, printed with
confidence. Adding instead of taking the max multiplies a run's cost by how often it was read.
Overwriting blindly lets a truncated window erase a complete total. Counting a shared item's
hours outside the leg's window charges one engine for another's work. Counting `OneLake …`
operations charges every engine for all five's storage. None of it raises.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import json
import sys
import types
from datetime import datetime, timedelta

import pytest

from _layout import REPO

sys.path.insert(0, str(REPO / ".github" / "scripts"))
import measure_cu as m  # noqa: E402

COLS = {"item_id": "Item Id", "workspace_id": "Workspace Id", "cu": "CU (s)",
        "when": "Datetime", "operation": "Operation name", "duration": "Duration (s)"}
WS = "WORKSPACE-1"
# UTC 03:20 on the model's +10 clock is 13:20 -> the 13:00 bucket.
NOW = datetime(2026, 9, 17, 6, 0, 0)


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setattr(m, "WS_FILTER", WS)
    monkeypatch.setattr(m, "MODEL_OFFSET", timedelta(hours=10))
    monkeypatch.setattr(m, "FALLBACK_HOURS", 2.0)


def row(guid, value, op="Warehouse Query", hour="2026-09-17T13:00:00", ws=WS, seconds=None):
    r = {"Item Id": guid, "Workspace Id": ws, "Operation name": op, "Datetime": hour, "CU": value}
    if seconds is not None:
        r["Seconds"] = seconds
    return r


def leg(started="2026-09-17T03:20:00Z", finished="2026-09-17T03:50:00Z", compute=("W1",)):
    d = {"compute": list(compute)}
    if started:
        d["started"] = started
    if finished:
        d["finished"] = finished
    return d


def run(run_id="1", **legs):
    return {"run": {"id": run_id, "started": "2026-09-17T03:15:00Z"}, "legs": legs}


def read(led, runs, rows):
    """One full read: fold, attribute, merge. Returns how many CU pairs moved."""
    folded, _stamps = m.fold(rows, COLS)
    changed, _ = m.apply(led, m.attribute(runs, folded))
    return changed


# ----------------------------------------------------------------------------- attribution

def test_a_leg_gets_its_item_inside_its_window():
    led = m.blank()
    read(led, [run(dwh=leg())], [row("W1", 100.0)])
    assert m.total(led, "1", "dwh") == 100.0
    assert led["runs"]["1"]["engines"]["dwh"]["window"] == ["2026-09-17T03:20:00Z",
                                                            "2026-09-17T03:50:00Z"]


def test_hours_outside_the_window_are_not_counted():
    """The warehouse outlives every run, so its CU in other hours belongs to other runs."""
    led = m.blank()
    read(led, [run(dwh=leg())], [row("W1", 100.0, hour="2026-09-17T12:00:00"),
                                 row("W1", 5.0, hour="2026-09-17T13:00:00"),
                                 row("W1", 100.0, hour="2026-09-17T14:00:00")])
    assert m.total(led, "1", "dwh") == 5.0


def test_a_window_spanning_hours_sums_every_hour_it_touches():
    led = m.blank()
    read(led, [run(dwh=leg(started="2026-09-17T03:20:00Z", finished="2026-09-17T05:10:00Z"))],
         [row("W1", 1.0, hour="2026-09-17T13:00:00"), row("W1", 2.0, hour="2026-09-17T14:00:00"),
          row("W1", 4.0, hour="2026-09-17T15:00:00"), row("W1", 8.0, hour="2026-09-17T16:00:00")])
    assert m.total(led, "1", "dwh") == 7.0


def test_another_engines_item_in_the_same_hour_is_not_counted():
    """Parallel legs share the hour; they must not share the bill."""
    led = m.blank()
    read(led, [run(dwh=leg(compute=["W1"]), spark=leg(compute=["LH"]))],
         [row("W1", 10.0), row("LH", 20.0, op="High Concurrency Session Livy Run")])
    assert m.total(led, "1", "dwh") == 10.0 and m.total(led, "1", "spark") == 20.0


def test_storage_operations_are_excluded():
    """On a SHARED lakehouse the OneLake transactions in any window are everybody's."""
    led = m.blank()
    read(led, [run(spark=leg(compute=["LH"]))],
         [row("LH", 20.0, op="High Concurrency Session Livy Run"),
          row("LH", 999.0, op="OneLake Write via Redirect"),
          row("LH", 999.0, op="OneLake Read via Proxy")])
    assert led["runs"]["1"]["engines"]["spark"]["cu"] == {"High Concurrency Session Livy Run": 20.0}


def test_a_leg_without_finished_gets_the_fallback_window_and_is_flagged():
    led = m.blank()
    read(led, [run(duckrun=leg(finished=None, compute=["NB"]))],
         [row("NB", 1.0, op="Notebook run", hour="2026-09-17T13:00:00"),
          row("NB", 2.0, op="Notebook run", hour="2026-09-17T15:00:00"),   # +2h fallback: in
          row("NB", 4.0, op="Notebook run", hour="2026-09-17T16:00:00")])  # out
    e = led["runs"]["1"]["engines"]["duckrun"]
    assert e["partial"] is True and m.total(led, "1", "duckrun") == 3.0


def test_a_leg_without_started_or_compute_is_skipped():
    led = m.blank()
    read(led, [run(dwh=leg(started=None), spark=leg(compute=[]))], [row("W1", 100.0)])
    assert led["runs"] == {}


def test_rows_outside_the_workspace_are_dropped():
    led = m.blank()
    read(led, [run(dwh=leg())], [row("W1", 5.0, ws="SOMEONE-ELSE")])
    assert led["runs"]["1"]["engines"]["dwh"]["cu"] == {}


# ----------------------------------------------------------------------------- the ledger rules

def test_a_re_read_of_the_same_number_changes_nothing():
    led = m.blank()
    read(led, [run(dwh=leg())], [row("W1", 100.0)])
    assert read(led, [run(dwh=leg())], [row("W1", 100.0)]) == 0
    assert m.total(led, "1", "dwh") == 100.0


def test_an_undercounted_first_read_is_raised_by_the_next_one():
    """An hour keeps growing for ~70 minutes after the fact; the bigger number simply wins."""
    led = m.blank()
    read(led, [run(dwh=leg())], [row("W1", 40.0)])
    read(led, [run(dwh=leg())], [row("W1", 125.0)])
    assert m.total(led, "1", "dwh") == 125.0


def test_a_smaller_later_read_never_lowers_a_total():
    led = m.blank()
    read(led, [run(dwh=leg())], [row("W1", 125.0)])
    assert read(led, [run(dwh=leg())], [row("W1", 4.0)]) == 0
    assert m.total(led, "1", "dwh") == 125.0


def test_repeated_reads_never_accumulate():
    led = m.blank()
    for _ in range(5):
        read(led, [run(dwh=leg())], [row("W1", 100.0)])
    assert m.total(led, "1", "dwh") == 100.0


def test_a_run_absent_from_a_read_keeps_its_value():
    """Past retention a run stops being returned. Deleting on absence would erase exactly the
    history this ledger exists to keep."""
    led = m.blank()
    read(led, [run("1", dwh=leg())], [row("W1", 100.0)])
    read(led, [run("2", dwh=leg(started="2026-09-18T03:20:00Z", finished="2026-09-18T03:50:00Z"))],
         [row("W1", 9.0, hour="2026-09-18T13:00:00")])
    assert m.total(led, "1", "dwh") == 100.0 and m.total(led, "2", "dwh") == 9.0


def test_seconds_ride_beside_cu_and_take_the_max_too():
    led = m.blank()
    read(led, [run(dwh=leg())], [row("W1", 40.0, seconds=10.0)])
    read(led, [run(dwh=leg())], [row("W1", 125.0, seconds=31.0)])
    read(led, [run(dwh=leg())], [row("W1", 90.0, seconds=22.0)])
    assert m.total(led, "1", "dwh") == 125.0
    assert m.total(led, "1", "dwh", "seconds") == 31.0


def test_a_read_with_no_duration_column_writes_no_seconds_at_all():
    """Empty, never zeros: "not measured" and "took no time" are different claims."""
    led = m.blank()
    read(led, [run(dwh=leg())], [row("W1", 100.0)])
    assert led["runs"]["1"]["engines"]["dwh"]["seconds"] == {}


def test_the_ledger_round_trips_and_sorts_its_keys(tmp_path):
    p = tmp_path / "cu.json"
    led = m.blank()
    led["runs"] = {"2": {"engines": {}}, "1": {"engines": {}}}
    m.save_ledger(led, str(p))
    assert list(json.loads(p.read_text(encoding="utf-8"))["runs"]) == ["1", "2"]
    assert m.load_ledger(str(p))["schema"] == m.SCHEMA


def test_a_missing_ledger_starts_empty_rather_than_raising(tmp_path):
    led = m.load_ledger(str(tmp_path / "nope.json"))
    assert led["runs"] == {} and led["schema"] == m.SCHEMA


def test_non_records_in_the_runs_dir_are_skipped(tmp_path):
    (tmp_path / "a.json").write_text(json.dumps({"run": {"id": "1"}}), encoding="utf-8")
    (tmp_path / "b.json").write_text(json.dumps(["not", "a", "record"]), encoding="utf-8")
    (tmp_path / "README.md").write_text("docs", encoding="utf-8")
    assert [r["_file"] for r in m.load_runs(str(tmp_path))] == ["a.json"]


# ----------------------------------------------------------------------------- the floor

def test_the_floor_is_the_earliest_leg_start_in_the_model_clock():
    runs = [run("1", dwh=leg(started="2026-09-17T09:00:00Z")),
            run("2", spark=leg(started="2026-09-17T05:19:24Z"))]
    # 05:19 UTC -> 15:19 model -> floored to 15:00; the run's own 03:15 start is later than
    # neither leg and not earlier than the horizon, so the earliest LEG wins... unless the
    # run block's start is earlier -- here it is (03:15 UTC = 13:00 model).
    assert m.floor_for(runs, NOW + timedelta(hours=10)) == datetime(2026, 9, 17, 13, 0)


def test_the_floor_is_clamped_to_retention():
    runs = [{"run": {"id": "1", "started": "2026-01-01T00:00:00Z"}}]
    now_model = datetime(2026, 9, 17, 16, 0)
    assert m.floor_for(runs, now_model) == datetime(2026, 9, 3, 16, 0), "14 days back, on the hour"


def test_compute_guids_cover_only_legs_inside_retention():
    runs = [run("1", dwh=leg(started="2026-09-17T03:20:00Z", compute=["W1"])),
            run("2", dwh=leg(started="2026-01-01T03:20:00Z", compute=["OLD"]))]
    assert m.compute_guids(runs, datetime(2026, 9, 3)) == {"W1"}


# ----------------------------------------------------------------------------- the DAX

def test_the_dax_narrows_to_hours_items_and_compute_operations():
    dax = m.dax_for("CAP", datetime(2026, 9, 3, 16, 0), COLS, {"w1", "NB"})
    assert "'Metrics By Item Operation And Hour'[Datetime]," in dax          # the hour grain
    assert "DATE(2026, 9, 3) + TIME(16, 0, 0)" in dax
    assert f'[Workspace Id] = "{WS}"' in dax
    assert '[Item Id] IN { "NB", "w1" }' in dax
    assert 'LEFT ( \'Metrics By Item Operation And Hour\'[Operation name], 7 ) = "OneLake"' in dax
    assert "FILTER" not in dax, "a FILTER(VALUES()) inside SUMMARIZECOLUMNS is silently ignored"
    assert '"Seconds", SUM (' in dax and "Duration (s)" in dax


def test_the_duration_sum_is_only_in_the_dax_when_the_column_resolved():
    assert "Seconds" not in m.dax_for("CAP", None, dict(COLS, duration=None), {"A"})


def test_an_optional_column_that_is_missing_is_none_rather_than_fatal(monkeypatch):
    schema = [{"Table": m.TABLE, "Name": n}
              for n in ("Item Id", "Workspace Id", "CU (s)", "Datetime", "Operation name")]
    monkeypatch.setattr(m, "execute_dax", lambda *a, **k: schema)
    got = m.discover_columns()
    assert got["duration"] is None and got["cu"] == "CU (s)"
    schema.append({"Table": m.TABLE, "Name": "Duration (s)"})
    assert m.discover_columns()["duration"] == "Duration (s)"


# ----------------------------------------------------------------------------- the endpoint

class _Resp:
    def __init__(self, status, payload=None):
        self.status_code = status
        self._payload = payload or {}
        self.text = json.dumps(self._payload)
        self.headers = {}

    def json(self):
        return self._payload


_OK = {"results": [{"tables": [{"rows": [{"n": 1}]}]}]}


def _stub_requests(monkeypatch, post):
    monkeypatch.setattr(m, "requests", types.SimpleNamespace(post=post))
    monkeypatch.setattr(m, "_TOKEN", "t")
    monkeypatch.setattr(m.time, "sleep", lambda *_: None)


def test_a_500_from_execute_queries_is_retried(monkeypatch):
    """Retrying is risk-free here: every read re-reads the window and merges with max()."""
    calls = []
    _stub_requests(monkeypatch, lambda *a, **kw: (calls.append(1),
                                                  _Resp(500) if len(calls) < 3 else _Resp(200, _OK))[1])
    assert m.execute_dax("EVALUATE {1}") == [{"n": 1}]
    assert len(calls) == 3


def test_a_403_is_never_retried_and_stays_fatal(monkeypatch):
    calls = []
    _stub_requests(monkeypatch, lambda *a, **kw: (calls.append(1), _Resp(403))[1])
    with pytest.raises(SystemExit):
        m.execute_dax("EVALUATE {1}")
    assert len(calls) == 1


def test_the_summary_names_the_caveats():
    led = m.blank()
    runs = [run("1", dwh=leg(), duckrun=leg(finished=None, compute=["NB"]))]
    folded, _ = m.fold([row("W1", 10.0)], COLS)
    rd = m.attribute(runs, folded)
    m.apply(led, rd)
    table = m.summary_table(led, rd, datetime(2026, 9, 17, 4, 0))
    assert "| 1 | 2026-09-17T03:15:00Z | dwh | 10.0 | 0 | may still rise |" in table
    assert "no leg-end recorded; window assumed; not yet visible" in table
