#!/usr/bin/env python3
"""One JSON document per pipeline run -- every job merges a FRAGMENT into it, nothing is passed
as text. Ported from djouallah/direct-lake-parquet-layout's record.py.

    RUN_RECORD=<path> python .github/scripts/record.py init
    RUN_RECORD=<path> python .github/scripts/record.py leg-start <engine>
    RUN_RECORD=<path> python .github/scripts/record.py leg-end <engine> <outcome>
    python .github/scripts/record.py finish <fragment dir> <parity dir | -> <dest>

**The point of this file is the Fabric item GUID plus the leg's time window.** The CU ledger
(measure_cu.py) attributes capacity units to a run and an engine by asking the Capacity Metrics
model for the compute items each leg used, inside the hours that leg ran. Nothing else in the
repo writes those two facts down: provision.py resolves every GUID and logs it to stderr,
remote_dbt.py prints the notebook's, and the leg's start/finish exist only in the Actions UI.

Unlike the source repo, NOTHING IS TORN DOWN HERE. The five engines share one lakehouse and the
warehouse outlives every run, so a GUID does not belong to one run -- which is why every leg
records `started` / `finished` beside its `compute` GUIDs, and why the ledger is keyed per run
and engine rather than per item. See history/README.md.

Env in: `RUN_RECORD`, the fragment this job writes. **Unset is a silent no-op**, deliberately:
provision.py and remote_dbt.py must stay runnable by hand (and inside the demo notebook) without
a record path. The cost of that choice is that a job which forgets its RUN_RECORD env fails
silently, producing a record missing those items -- so every fragment upload is
`if-no-files-found: ignore` and `finish` logs the item table it assembled.

Each job writes its OWN fragment -- separate jobs run on separate runners and cannot share one --
and the `record` job merges them by BASENAME order. `items` and `legs` are dicts keyed by GUID
and engine rather than lists: a deep merge unions dicts and REPLACES lists, so the leg-end
fragment's `{finished, outcome}` lands on top of the leg-start fragment's `{started}` without
either knowing about the other. tests_py/test_record.py pins this.
"""
from __future__ import annotations

import glob
import json
import os
import sys
from datetime import datetime, timezone


def path(p: str | None = None) -> str | None:
    """The fragment this job writes, or None when there is no record to write."""
    return p or os.environ.get("RUN_RECORD") or None


def now() -> str:
    """UTC, second precision, `Z` suffix. measure_cu.py converts to the metrics model's clock."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def deep_update(a: dict, b: dict) -> dict:
    """Recursive dict union, b winning. Lists and scalars are replaced, not appended."""
    for k, v in b.items():
        if isinstance(v, dict) and isinstance(a.get(k), dict):
            deep_update(a[k], v)
        else:
            a[k] = v
    return a


def merge(obj: dict, p: str | None = None) -> str | None:
    """Deep-merge `obj` into the fragment. No RUN_RECORD set -> nothing happens, silently."""
    p = path(p)
    if not p:
        return None
    cur = {}
    if os.path.exists(p):
        with open(p, encoding="utf-8") as f:
            cur = json.load(f)
    deep_update(cur, obj)
    d = os.path.dirname(p)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(cur, f, indent=1, sort_keys=True, default=str)
    return p


def item(guid: str | None, role: str, kind: str, name: str, **extra) -> str | None:
    """Record one Fabric item under its GUID.

    `role` is a closed vocabulary -- `landing` | `data` | `warehouse` | `catalog` | `folder` |
    `compute` -- and `kind` is Fabric's own item type (`Lakehouse`, `Warehouse`, `SQLDatabase`,
    `Notebook`, `Folder`). The GUID is upper-cased because the metrics model returns item ids
    upper-cased; normalising at write time makes the join a dict lookup.

    A blank GUID is dropped rather than recorded as null: a null key joins to nothing and reads
    as an item nobody can find.
    """
    if not guid:
        return None
    rec = {"role": role, "kind": kind, "name": name, **extra}
    return merge({"items": {str(guid).upper(): rec}})


def leg(engine: str, **fields) -> str | None:
    """Record facts about one engine's leg: `started`, `finished`, `outcome`, `compute` (the
    GUIDs whose compute operations belong to this leg). Merged, so the fields accumulate across
    the steps that write them."""
    if not engine:
        return None
    if "compute" in fields:
        fields["compute"] = [str(g).upper() for g in fields["compute"] if g]
    return merge({"legs": {engine: fields}})


def fragments(paths) -> list[str]:
    """Every *.json under each path, sorted by BASENAME.

    Basename, not full path: `actions/download-artifact` nests each artifact in its own
    directory, so the full paths sort by artifact name and the intended `record-00-run.json`
    first ordering is lost. Ordering only decides who wins a scalar collision -- the dicts union
    either way.
    """
    found = []
    for p in paths:
        if os.path.isdir(p):
            found += glob.glob(os.path.join(p, "**", "*.json"), recursive=True)
        elif os.path.exists(p):
            found.append(p)
    return sorted(found, key=os.path.basename)


def combine(paths, dest: str) -> list[str]:
    """Merge every fragment into `dest`. Returns the list merged, so the caller can log it."""
    got = fragments(paths)
    for f in got:
        with open(f, encoding="utf-8") as fh:
            merge(json.load(fh), dest)
    return got


def _init() -> None:
    """Seed the run block from the workflow environment. `RUNIN_*` are the dispatch inputs."""
    merge({
        "schema": 1,
        "run": {
            "id": os.environ.get("GITHUB_RUN_ID"),
            "sha": os.environ.get("GITHUB_SHA"),
            "started": now(),
            "url": (f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/"
                    f"{os.environ.get('GITHUB_REPOSITORY', '')}/actions/runs/"
                    f"{os.environ.get('GITHUB_RUN_ID', '')}"),
        },
        # An input left blank is absent, not recorded as "": the record states what the run chose.
        "inputs": {k.lower()[6:]: v for k, v in os.environ.items()
                   if k.startswith("RUNIN_") and v != ""},
    })


def parity_fingerprints(parity_dir: str | None) -> dict:
    """`{engine: fingerprint}` from the `<engine>.json` files parity.py capture wrote.

    THE DOWNLOADED ARTIFACTS, NEVER `history/parity/`: after the first commit that directory
    holds the previous run's fingerprints too, so a leg that failed this run would fold a stale
    fingerprint into this run's record. Anything that is not a dict carrying `engine` is skipped.
    """
    out = {}
    if not parity_dir or not os.path.isdir(parity_dir):
        return out
    for f in sorted(glob.glob(os.path.join(parity_dir, "*.json"))):
        try:
            with open(f, encoding="utf-8") as fh:
                fp = json.load(fh)
        except (OSError, ValueError):
            continue
        if isinstance(fp, dict) and fp.get("engine"):
            out[str(fp["engine"])] = fp
    return out


def finish(frag_dir: str, parity_dir: str | None, dest: str) -> str:
    """Close the record: merge every fragment, fold in this run's parity fingerprints, stamp
    `run.finished`. `parity_dir` may be None (no comparison ran) -- the record then simply has
    no `parity` key, which is a different statement from an empty one."""
    got = combine([frag_dir], dest)
    fps = parity_fingerprints(parity_dir)
    if fps:
        merge({"parity": fps}, dest)
    merge({"run": {"finished": now()}}, dest)
    with open(dest, encoding="utf-8") as f:
        doc = json.load(f)
    items, legs = doc.get("items") or {}, doc.get("legs") or {}
    sys.stderr.write(f"  merged {len(got)} fragment(s): "
                     + ", ".join(os.path.basename(g) for g in got) + "\n"
                     f"  {dest}: {len(items)} item GUIDs, {len(legs)} leg(s), "
                     f"{len(fps)} fingerprint(s), top-level keys {sorted(doc)}\n")
    for guid, it in sorted(items.items(), key=lambda kv: (kv[1].get("role", ""),
                                                          kv[1].get("name", ""))):
        sys.stderr.write(f"    {it.get('role', '?'):<10} {it.get('name', '?'):<24} {guid}"
                         + (f"  ({it['engine']})" if it.get("engine") else "") + "\n")
    for engine, lg in sorted(legs.items()):
        sys.stderr.write(f"    leg {engine:<9} {lg.get('started', '?')} -> "
                         f"{lg.get('finished', '?')}  {lg.get('outcome', '?')}  "
                         f"compute={lg.get('compute')}\n")
    return dest


def main(argv: list[str]) -> int:
    if not argv:
        print("usage: record.py init | merge '<json>' | leg-start <engine> | "
              "leg-end <engine> <outcome> | combine <dir>... <dest> | "
              "finish <fragment dir> <parity dir|-> <dest>", file=sys.stderr)
        return 2
    cmd = argv[0]
    if cmd == "init":
        _init()
        print(path() or "(no RUN_RECORD set)")
    elif cmd == "merge":
        merge(json.loads(argv[1]))
        print(path() or "(no RUN_RECORD set)")
    elif cmd == "leg-start":
        leg(argv[1], started=now())
        print(path() or "(no RUN_RECORD set)")
    elif cmd == "leg-end":
        leg(argv[1], finished=now(), outcome=argv[2] if len(argv) > 2 else "unknown")
        print(path() or "(no RUN_RECORD set)")
    elif cmd == "combine":
        *srcs, dest = argv[1:]
        got = combine(srcs, dest)
        sys.stderr.write(f"merged {len(got)} fragment(s) into {dest}\n")
        print(dest)
    elif cmd == "finish":
        frag, parity, dest = argv[1], argv[2], argv[3]
        print(finish(frag, None if parity == "-" else parity, dest))
    else:
        print(f"unknown command {cmd}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
