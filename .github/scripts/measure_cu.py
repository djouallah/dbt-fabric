#!/usr/bin/env python3
"""Read capacity units -- and duration -- PER RUN AND ENGINE from the Fabric Capacity Metrics
model, and keep a cumulative ledger at history/cu.json. Ported from
djouallah/direct-lake-parquet-layout's cu/measure.py; the attribution is what changed.

Fabric exposes no per-operation CU REST API. The Capacity Metrics app's own semantic model is
the only authoritative source, and it is read here with DAX over the Power BI `executeQueries`
endpoint: `Metrics By Item Operation And Hour`, summed server-side per (item, operation, hour).

WHY THE HOUR IS BACK IN THE GRAIN. The source repo deletes every item when a run finishes, so a
GUID belongs to exactly one run and "CU per item, cumulative" is already CU per run per engine.
Here NOTHING is torn down: five engines share one lakehouse, the warehouse outlives every run,
and only the three throwaway notebooks are per-run items. So every leg records the hours it ran
(`legs.<engine>.started` / `finished`, record.py) and the GUIDs its compute bills against
(`legs.<engine>.compute`), and a leg's CU is the sum over those items, those hours.

COMPUTE ONLY. Every `OneLake …` operation is storage, and on a SHARED lakehouse the storage
transactions in any window are the sum of all five legs plus anything else that touched the
item -- there is nothing to attribute. They are excluded in the query and again here. What is
left is unambiguous per engine: each DuckDB leg's notebook run, dwh's `Warehouse Query` on its
own item, spark's `High Concurrency Session Livy Run` on the lakehouse (only the spark leg
opens Livy sessions there).

WHAT THE WINDOW CANNOT DO. Two runs less than an hour apart share an hour on dwh and spark
(both get that hour's whole CU); a Livy session idling past leg-end bills into the next hour
and is missed. Both are recorded caveats, neither is fixed. Parallel legs of ONE run never
collide: different items.

THE LEDGER -- history/cu.json:

    {"schema": 1, "updated": "...",
     "reads": [{"at", "since", "runs", "changed", "timed", "pending"}],
     "runs": {"<run id>": {"started": "...Z", "engines": {
         "<engine>": {"cu": {"<operation>": 123.4}, "seconds": {"<operation>": 88.1},
                      "window": ["...Z", "...Z"], "partial": true?}}}}}

Three rules keep re-reading safe, and none of them needs any state (unchanged from the source):
only runs the read RETURNED are touched (one past retention keeps its value); `max(old, new)`
per (run, engine, operation), never a blind overwrite and never `+` (a sum over a fixed window
only ever grows as the model's ingestion catches up, so the larger number is the more complete
one); and the floor is the earliest recorded leg start, clamped to the app's ~14-day retention.
A run measured minutes after it finished is a LOWER BOUND (~6 min ingestion lag, then 5-64 min
of smoothing); the daily read raises it.

Env in: `CU_METRICS_WORKSPACE_ID`, `CU_METRICS_MODEL_ID`, `CU_CAPACITY_ID`, `CU_WORKSPACE_FILTER`.
`PBI_TOKEN` is optional -- unset, duckrun mints the Power BI audience from the same OIDC
exchange deploy.py's model refresh uses. Optional: `CU_SINCE` (override the floor, in the
MODEL's clock), `CU_MODEL_OFFSET_HOURS` (10), `CU_RETENTION_DAYS` (14), `CU_FALLBACK_HOURS`
(2, the window of a leg that never wrote `finished`), `CU_RUNS_DIR`, `CU_LEDGER`.

stdout is a one-line summary; diagnostics go to stderr; the markdown table goes to
$GITHUB_STEP_SUMMARY when set.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

try:
    import requests
except ImportError:  # the offline tests stub it; the workflow installs duckrun, which brings it
    requests = None

PBI = "https://api.powerbi.com/v1.0/myorg"
WS = os.environ.get("CU_METRICS_WORKSPACE_ID", "").strip()
MODEL = os.environ.get("CU_METRICS_MODEL_ID", "").strip()
CAPACITY = os.environ.get("CU_CAPACITY_ID", "").strip()
# A column of the fact table itself, which is what makes a GUID-only read possible with no
# join and no name resolution.
WS_FILTER = os.environ.get("CU_WORKSPACE_FILTER", "").strip().upper()

RUNS_DIR = os.environ.get("CU_RUNS_DIR", "history/runs").strip()
LEDGER = os.environ.get("CU_LEDGER", "history/cu.json").strip()

# The metrics model stamps its timestamps in the offset configured IN THE APP, not in UTC. A
# wrong value here reads as "no activity" rather than as an error. +10 for this tenant.
MODEL_OFFSET = timedelta(hours=float(os.environ.get("CU_MODEL_OFFSET_HOURS", "10")))
# What the app keeps. The floor is clamped to it: reading further back cannot return anything.
RETENTION_DAYS = float(os.environ.get("CU_RETENTION_DAYS", "14"))
# A leg that died before leg-end has no `finished`. Cancelling the GitHub job does not stop
# Fabric, so the compute went on for a while; this is how long a while is assumed to be.
FALLBACK_HOURS = float(os.environ.get("CU_FALLBACK_HOURS", "2"))
# ~6 min ingestion lag plus up to 64 min of smoothing: younger than this, a number may rise.
SETTLE_MINUTES = 70
SCHEMA = 1

TABLE = "Metrics By Item Operation And Hour"
# Column names move between versions of the app -- Microsoft's own accelerator ships four DAX
# variants for exactly this reason -- so every role is resolved against the real schema and a
# miss fails specifically, naming what was actually there. The first name in each list is the
# one MEASURED against the live model (2026-08-02, in the source repo); the rest are fallbacks.
# Note `Datetime`: the table also has a date-only `Date` column, and resolving `when` to that
# would compare a floor against midnight and silently widen every window.
REQUIRED = {
    "item_id": ["Item Id", "ItemId", "Item"],
    "workspace_id": ["Workspace Id", "WorkspaceId", "Workspace"],
    "cu": ["CU (s)", "CU(s)", "Total CU (s)", "CU"],
    "when": ["Datetime", "Date Hour", "DateHour", "Date/Time"],
    "operation": ["Operation name", "Operation", "Operation Name"],
}
# Resolved if present, SKIPPED if not. Duration is a bonus riding an existing query; a guessed
# name in REQUIRED could kill the CU read that works today.
OPTIONAL = {
    "duration": ["Duration (s)", "Duration", "Total Duration (s)", "Operation Duration (s)"],
}
# Every operation whose name starts with this is a storage transaction.
STORAGE_PREFIX = "OneLake"


def log(msg: str) -> None:
    sys.stderr.write(msg + "\n")


def die(msg: str):
    log("ERROR: " + msg)
    raise SystemExit(1)


def is_storage(op) -> bool:
    return str(op or "").strip().startswith(STORAGE_PREFIX)


# --------------------------------------------------------------------------------- the model

_TOKEN = None


def token() -> str:
    """`PBI_TOKEN` when set (the by-hand escape hatch), else duckrun's own OIDC exchange for
    the Power BI audience -- the one deploy.py already uses to refresh a semantic model."""
    global _TOKEN
    if _TOKEN:
        return _TOKEN
    t = os.environ.get("PBI_TOKEN", "").strip()
    if not t:
        try:
            from duckrun.auth import get_powerbi_token

            t = get_powerbi_token()
        except Exception as ex:  # noqa: BLE001
            die(f"no PBI_TOKEN and duckrun could not mint one ({type(ex).__name__}: {ex}). "
                f"By hand: `az account get-access-token --resource "
                f"https://analysis.windows.net/powerbi/api --query accessToken -o tsv` as PBI_TOKEN.")
    _TOKEN = t
    return t


def execute_dax(dax: str, tries: int = 4, fatal: bool = True):
    """POST one DAX query. Retries the rate limits and 5xx, honouring `Retry-After`.

    500 IS IN THE RETRY SET: it is the transient the service returns most often. Retrying is
    risk-free here specifically, because every read re-reads the whole window from the floor
    and merges with `max(old, new)`, so a repeated query is idempotent and monotonic. 401/403
    stay fatal -- those are a credential problem and must remain loud.
    """
    url = f"{PBI}/groups/{WS}/datasets/{MODEL}/executeQueries"
    body = {"queries": [{"query": dax}], "serializerSettings": {"includeNulls": True}}
    for i in range(tries):
        r = requests.post(url, headers={"Authorization": f"Bearer {token()}"}, json=body,
                          timeout=300)
        if r.status_code == 200:
            return r.json()["results"][0]["tables"][0].get("rows", [])
        if r.status_code in (429, 500, 502, 503, 504) and i < tries - 1:
            wait = int(r.headers.get("Retry-After") or min(60, 5 * 2 ** i))
            log(f"  {r.status_code} from executeQueries; retrying in {wait}s")
            time.sleep(wait)
            continue
        if r.status_code in (401, 403):
            die(f"executeQueries returned {r.status_code}. The service principal needs read "
                f"access to the Capacity Metrics model. A user token works as a manual escape "
                f"hatch: `az account get-access-token --resource "
                f"https://analysis.windows.net/powerbi/api --query accessToken -o tsv` as PBI_TOKEN.")
        if fatal:
            die(f"executeQueries returned {r.status_code}: {r.text[:400]}")
        log(f"  executeQueries returned {r.status_code}: {r.text[:200]}")
        return None
    return None


def strip_prefix(rows):
    """executeQueries returns keys as `Table[Column]` or `[Alias]`. Reduce to the bare name."""
    return [{re.sub(r"^.*\[|\]$", "", k): v for k, v in row.items()} for row in (rows or [])]


def discover_columns() -> dict:
    """Resolve every role against the model's real schema, so a version bump fails specifically.
    A REQUIRED role that misses is fatal; an OPTIONAL one is None and is NAMED in the log."""
    rows = strip_prefix(execute_dax("EVALUATE INFO.VIEW.COLUMNS()"))
    cols = {r.get("Name") for r in rows if r.get("Table") == TABLE}
    if not cols:
        die(f"the model at {WS}/{MODEL} has no table named '{TABLE}'. Either that is not the "
            f"Fabric Capacity Metrics model, or this app version renamed it. Tables present: "
            f"{sorted({r.get('Table') for r in rows if r.get('Table')})}")
    got, missing = {}, []
    for role, candidates in REQUIRED.items():
        hit = next((c for c in candidates if c in cols), None)
        (got.__setitem__(role, hit) if hit else missing.append(f"{role} (tried {candidates})"))
    if missing:
        die(f"'{TABLE}' exists but these columns were not found: {'; '.join(missing)}. "
            f"Present: {sorted(cols)}. Add the actual name to REQUIRED in this file.")
    for role, candidates in OPTIONAL.items():
        got[role] = next((c for c in candidates if c in cols), None)
        if got[role]:
            log(f"  {role} -> '{got[role]}'")
        else:
            log(f"  no {role} column (tried {candidates}) -- that measurement is skipped, the "
                f"rest is unaffected. '{TABLE}' has: {sorted(cols)}")
    return got


def capacities() -> list[str]:
    if CAPACITY:
        return [CAPACITY]
    for col in ("Capacity Id", "capacity Id", "CapacityId"):
        rows = execute_dax(f"EVALUATE VALUES('Capacities'[{col}])", fatal=False)
        if rows:
            ids = [str(v) for r in strip_prefix(rows) for v in r.values() if v]
            if ids:
                return ids
    die("could not read any capacity id; set CU_CAPACITY_ID")


def dax_for(cap: str, since: datetime | None, c: dict, guids) -> str:
    """CU and DURATION per (item, operation, HOUR) for one capacity, from `since` onward,
    summed server-side and narrowed to the items the run records name.

    Every predicate is a plain boolean inside CALCULATETABLE, never `FILTER(VALUES(...))`
    inside SUMMARIZECOLUMNS -- the latter is accepted and silently changes nothing, and the
    caller checks the earliest hour that came back against the floor it asked for. The GUID
    and operation predicates matter here in a way they did not in the source: the workspace
    holds ~175 items and the hour grain multiplies rows, and executeQueries caps a result.
    ONE CAPACITY PER QUERY: these tables are DirectQuery and resolve one data location.
    """
    secs = (f',\n        "Seconds", SUM ( \'{TABLE}\'[{c["duration"]}] )' if c.get("duration") else "")
    inner = f"""SUMMARIZECOLUMNS (
        '{TABLE}'[{c['item_id']}],
        '{TABLE}'[{c['workspace_id']}],
        '{TABLE}'[{c['operation']}],
        '{TABLE}'[{c['when']}],
        "CU", SUM ( '{TABLE}'[{c['cu']}] ){secs}
    )"""
    preds = []
    if since:
        preds.append(f"'{TABLE}'[{c['when']}] >= DATE({since.year}, {since.month}, {since.day}) + "
                     f"TIME({since.hour}, {since.minute}, 0)")
    if WS_FILTER:
        preds.append(f"'{TABLE}'[{c['workspace_id']}] = \"{WS_FILTER}\"")
    if guids:
        preds.append(f"'{TABLE}'[{c['item_id']}] IN {{ "
                     + ", ".join(f'"{g}"' for g in sorted(guids)) + " }")
    preds.append(f"NOT ( LEFT ( '{TABLE}'[{c['operation']}], {len(STORAGE_PREFIX)} ) "
                 f"= \"{STORAGE_PREFIX}\" )")
    body = "CALCULATETABLE (\n        " + inner + ",\n        " + ",\n        ".join(preds) + "\n    )"
    return f"""
DEFINE
    MPARAMETER 'CapacitiesList' = {{ "{cap}" }}
EVALUATE
    {body}
""".strip()


def read_cu(cap: str, since: datetime | None, c: dict, guids):
    return strip_prefix(execute_dax(dax_for(cap, since, c, guids), fatal=False))


# --------------------------------------------------------------------------------- time

def parse_utc(s) -> datetime:
    """An ISO stamp (`Z`, `+00:00` or naive-as-UTC) as a naive UTC datetime."""
    t = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    if t.tzinfo:
        t = t.astimezone(timezone.utc).replace(tzinfo=None)
    return t


def iso_z(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def model_hour(t_utc: datetime) -> datetime:
    """A UTC instant as the metrics model's hour bucket."""
    return (t_utc + MODEL_OFFSET).replace(minute=0, second=0, microsecond=0)


def hour_key(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:00:00")


def leg_window(lg: dict):
    """`(start, end, partial)` in UTC, or None when the leg never recorded a start."""
    started = (lg or {}).get("started")
    if not started:
        return None
    try:
        s = parse_utc(started)
    except ValueError:
        return None
    partial = False
    f = lg.get("finished")
    try:
        e = parse_utc(f) if f else None
    except ValueError:
        e = None
    if e is None:
        e, partial = s + timedelta(hours=FALLBACK_HOURS), True
    if e < s:
        e = s
    return s, e, partial


def hours_in(s: datetime, e: datetime) -> set[str]:
    """Every model-clock hour bucket the UTC window [s, e] touches."""
    out, h, last = set(), model_hour(s), model_hour(e)
    while h <= last:
        out.add(hour_key(h))
        h += timedelta(hours=1)
    return out


# --------------------------------------------------------------------------------- the ledger

def blank() -> dict:
    return {"schema": SCHEMA, "updated": None, "reads": [], "runs": {}}


def load_ledger(path: str | None = None) -> dict:
    path = path or LEDGER
    try:
        with open(path, encoding="utf-8") as f:
            doc = json.load(f)
    except (OSError, ValueError):
        return blank()
    doc.setdefault("runs", {})
    doc.setdefault("reads", [])
    return doc


def save_ledger(doc: dict, path: str | None = None) -> str:
    path = path or LEDGER
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        # sort_keys so a read that moves one number is a one-line diff; indent=1 so the diff
        # is readable at all.
        json.dump(doc, f, indent=1, sort_keys=True)
    return path


def load_runs(directory: str | None = None) -> list[dict]:
    """Every run record, oldest first (the filenames start with the UTC stamp). Anything that
    is not a JSON object is skipped by SHAPE, not just by name."""
    directory = directory or RUNS_DIR
    out = []
    try:
        names = sorted(os.listdir(directory))
    except OSError:
        return out
    for n in names:
        if not n.endswith(".json"):
            continue
        try:
            with open(os.path.join(directory, n), encoding="utf-8") as f:
                rec = json.load(f)
        except Exception as ex:  # noqa: BLE001
            log(f"  skipping {n}: unreadable ({type(ex).__name__})")
            continue
        if not isinstance(rec, dict):
            log(f"  skipping {n}: not a record ({type(rec).__name__})")
            continue
        rec["_file"] = n
        out.append(rec)
    return out


def fold(rows, cols: dict):
    """`({(guid, op, hour): [cu, seconds|None]}, [hours seen])` for this workspace's rows.
    The workspace and storage filters were in the DAX too; re-applied here because a rejected
    row is easier to explain from this side, and a DAX predicate can be silently ignored."""
    out, stamps = {}, []
    for r in rows or []:
        guid = str(r.get(cols["item_id"]) or "").upper()
        wsid = str(r.get(cols["workspace_id"]) or "").upper()
        value = r.get("CU")
        if not guid or value is None:
            continue
        if WS_FILTER and wsid != WS_FILTER:
            continue
        op = str(r.get(cols["operation"]) or "(unnamed)").strip()
        if is_storage(op):
            continue
        hour = str(r.get(cols["when"]) or "")[:19]
        if not hour:
            continue
        stamps.append(hour)
        cur = out.setdefault((guid, op, hour), [0.0, None])
        cur[0] = round(cur[0] + float(value), 3)
        seconds = r.get("Seconds")
        if seconds is not None:
            cur[1] = round((cur[1] or 0.0) + float(seconds), 3)
    return out, stamps


def attribute(runs: list[dict], folded: dict) -> dict:
    """`{run_id: {"started", "engines": {engine: {"cu", "seconds", "window", ["partial"]}}}}`.

    A leg's CU is the sum over ITS compute items, in the model-clock hours ITS window touches,
    compute operations only. A leg with no `started` or no `compute` is skipped -- there is
    nothing to ask -- and a run with no attributable leg is absent.
    """
    out: dict = {}
    for rec in runs:
        run = rec.get("run") or {}
        run_id = str(run.get("id") or "")
        if not run_id:
            continue
        for engine, lg in sorted((rec.get("legs") or {}).items()):
            w = leg_window(lg)
            compute = {str(g).upper() for g in ((lg or {}).get("compute") or []) if g}
            if not w or not compute:
                continue
            s, e, partial = w
            hours = hours_in(s, e)
            cu, secs = {}, {}
            for (guid, op, hour), (c, sec) in folded.items():
                if guid in compute and hour in hours:
                    cu[op] = round(cu.get(op, 0.0) + c, 3)
                    if sec is not None:
                        secs[op] = round(secs.get(op, 0.0) + sec, 3)
            entry = {"cu": cu, "seconds": secs, "window": [iso_z(s), iso_z(e)]}
            if partial:
                entry["partial"] = True
            out.setdefault(run_id, {"started": run.get("started") or (lg or {}).get("started"),
                                    "engines": {}})["engines"][engine] = entry
    return out


def apply(ledger: dict, read: dict):
    """Merge a read into the ledger, keeping the LARGER number per (run, engine, operation).
    Returns `(cu pairs moved, seconds pairs moved)`. `window`/`partial`/`started` are derived
    from the record and simply overwritten.

    `max`, not overwrite, and never `+`: a per-window sum only ever grows as ingestion catches
    up, so the larger value is the more complete one -- which makes a re-read idempotent, an
    undercounted first read self-correcting, and a run past retention (absent from the read)
    untouched. Adding would multiply a run's cost by the number of times it was read.
    """
    changed = changed_s = 0
    for run_id, run in read.items():
        cur = ledger.setdefault("runs", {}).setdefault(run_id, {})
        if run.get("started"):
            cur["started"] = run["started"]
        engines = cur.setdefault("engines", {})
        for engine, entry in run["engines"].items():
            e = engines.setdefault(engine, {})
            e["window"] = entry["window"]
            if entry.get("partial"):
                e["partial"] = True
            else:
                e.pop("partial", None)
            for key in ("cu", "seconds"):
                dest = e.setdefault(key, {})
                for op, value in entry[key].items():
                    if dest.get(op) is None or value > dest[op]:
                        dest[op] = value
                        if key == "cu":
                            changed += 1
                        else:
                            changed_s += 1
    return changed, changed_s


def total(ledger: dict, run_id: str, engine: str, key: str = "cu") -> float:
    """One leg's compute CU -- or seconds -- across every operation it was billed for."""
    return sum((((ledger.get("runs") or {}).get(run_id) or {}).get("engines") or {})
               .get(engine, {}).get(key, {}).values())


def floor_for(runs: list[dict], now_model: datetime) -> datetime:
    """The earliest hour worth asking about: the first recorded leg start (falling back to the
    run's start), in the model's clock, clamped to retention."""
    horizon = (now_model - timedelta(days=RETENTION_DAYS)).replace(minute=0, second=0, microsecond=0)
    starts = []
    for rec in runs:
        stamps = [lg.get("started") for lg in (rec.get("legs") or {}).values() if lg]
        stamps.append((rec.get("run") or {}).get("started"))
        for stamp in stamps:
            if not stamp:
                continue
            try:
                starts.append(model_hour(parse_utc(stamp)))
            except ValueError:
                continue
    if not starts:
        return horizon
    return max(min(starts), horizon)


def compute_guids(runs: list[dict], horizon_utc: datetime) -> set[str]:
    """Every compute GUID of every leg that started inside retention -- the IN list."""
    out = set()
    for rec in runs:
        for lg in (rec.get("legs") or {}).values():
            w = leg_window(lg)
            if not w or w[0] < horizon_utc:
                continue
            out |= {str(g).upper() for g in (lg.get("compute") or []) if g}
    return out


def summary_table(ledger: dict, read: dict, now_utc: datetime) -> str:
    """One row per (run, engine) the read touched, newest first: compute CU, seconds, caveats."""
    lines = ["## 💰 Capacity units per run and engine (compute only)\n",
             "| run | started (UTC) | engine | CU | seconds | note |",
             "| --- | --- | --- | --: | --: | --- |"]
    rows = []
    for run_id, run in read.items():
        for engine, entry in run["engines"].items():
            rows.append((run.get("started") or "", run_id, engine, entry))
    for started, run_id, engine, entry in sorted(rows, reverse=True):
        notes = []
        try:
            if (now_utc - parse_utc(entry["window"][1])) < timedelta(minutes=SETTLE_MINUTES):
                notes.append("may still rise")
        except ValueError:
            pass
        if entry.get("partial"):
            notes.append("no leg-end recorded; window assumed")
        if not entry["cu"]:
            notes.append("not yet visible")
        lines.append(f"| {run_id} | {started} | {engine} | "
                     f"{total(ledger, run_id, engine):,.1f} | "
                     f"{total(ledger, run_id, engine, 'seconds'):,.0f} | {'; '.join(notes)} |")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    if requests is None:
        die("`requests` is not installed (pip install -r requirements/duckrun.txt).")
    if not (WS and MODEL):
        die("CU_METRICS_WORKSPACE_ID and CU_METRICS_MODEL_ID must both be set.")

    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    now_model = now_utc + MODEL_OFFSET
    ledger = load_ledger()
    runs = load_runs()
    if not runs:
        print(f"{LEDGER}: no run records under {RUNS_DIR}; nothing to attribute")
        return 0
    floor = floor_for(runs, now_model)
    override = os.environ.get("CU_SINCE", "").strip()
    if override:
        try:
            floor = datetime.fromisoformat(override.replace("Z", ""))
        except ValueError:
            die(f"CU_SINCE={override!r} is not ISO-8601. It is in the MODEL's clock, not UTC.")
        log(f"  CU_SINCE overrides the computed floor: {floor}")
    guids = compute_guids(runs, floor - MODEL_OFFSET - timedelta(hours=1))
    log(f"  {len(runs)} run record(s); {len(guids)} compute item(s); reading from {floor} "
        f"(model clock, UTC+{MODEL_OFFSET.total_seconds() / 3600:g})")
    if not guids:
        print(f"{LEDGER}: no leg inside retention names a compute item; nothing to read")
        return 0

    cols = discover_columns()
    folded, stamps, rows_seen = {}, [], 0
    for cap in capacities():
        rows = read_cu(cap, floor, cols, guids)
        if rows is None:
            log(f"  capacity {cap}: refused the query -- skipping it")
            continue
        rows_seen += len(rows)
        f, st = fold(rows, cols)
        for key, (c, sec) in f.items():
            cur = folded.setdefault(key, [0.0, None])
            cur[0] = round(cur[0] + c, 3)
            if sec is not None:
                cur[1] = round((cur[1] or 0.0) + sec, 3)
        stamps += st
        log(f"  capacity {cap}: {len(rows)} row(s), {len(f)} (item, operation, hour) in scope")

    # Verify the floor actually BOUND -- see dax_for.
    if stamps:
        lo = min(stamps)
        log(f"  earliest hour returned: {lo}")
        if lo < floor.strftime("%Y-%m-%dT%H:%M:%S"):
            die(f"the `since` filter did NOT bind: asked for >= {floor}, got rows from {lo}. "
                f"Refusing to write a ledger that silently includes excluded time.")

    read = attribute(runs, folded)
    changed, changed_s = apply(ledger, read)
    pending = sum(1 for run in read.values() for e in run["engines"].values() if not e["cu"])
    timed = sum(1 for run in read.values() for e in run["engines"].values() if e["seconds"])

    ledger["schema"] = SCHEMA
    ledger["updated"] = iso_z(now_utc)
    ledger["reads"].append({"at": ledger["updated"], "since": floor.isoformat(),
                            "runs": len(read), "changed": changed, "timed": timed,
                            "pending": pending})
    path = save_ledger(ledger)

    table = summary_table(ledger, read, now_utc)
    log(table)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(table + "\n")
    if pending:
        log(f"  {pending} leg(s) returned no rows yet. Expected for a run that finished in the "
            f"last ~10 minutes (ingestion lag) -- the daily read picks them up.")
    print(f"{path}: {len(ledger['runs'])} run(s) in the ledger, {len(read)} read, "
          f"{rows_seen} row(s), {changed} CU value(s) raised"
          + (f", {changed_s} duration(s)" if changed_s else "")
          + (f", {pending} leg(s) not yet visible" if pending else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
