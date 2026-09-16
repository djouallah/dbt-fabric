#!/usr/bin/env python3
"""Compare the gold-layer fingerprints the five engines emitted. The acceptance test.

Five adapters, one set of business logic, one gold table. This is what makes that a
checkable claim rather than an assertion in a README.

Each engine's CI leg runs, through its OWN adapter:

    dbt run-operation parity_fingerprint --target <engine> --profiles-dir . \
        | python .github/scripts/parity.py capture history/parity

and once the legs are done:

    python .github/scripts/parity.py compare history/parity

Exact-match fields (rows_total, duids, days, date_min, date_max) must be IDENTICAL: a
difference there is a real difference in what the pipeline produced. The money columns get
a relative tolerance, because two engines can compute the same thing and still disagree in
the last place:

  * DOUBLE -> DECIMAL tie-breaking is HALF_UP on Spark, HALF_EVEN on DuckDB and a third
    thing in T-SQL. A neutral reader cannot grade a writer's rounding.
  * Summing ~10^5 doubles in a different order gives a different last bit.

The tolerance is deliberately tight (1e-9 relative). It is there to absorb float
association, not to paper over a logic difference -- if a leg drifts past it, that is a
finding, not a threshold to raise.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

EXACT = ["rows_total", "duids", "days", "date_min", "date_max"]
APPROX = ["mw_sum", "price_sum"]
REL_TOL = 1e-9


def capture(out_dir: Path) -> int:
    """Read a `dbt run-operation parity_fingerprint` log on stdin, save the JSON object."""
    text = sys.stdin.read()
    m = re.search(r"\{\s*\"engine\".*?\n\}", text, re.S)
    if not m:
        print("no fingerprint JSON found in the run-operation output", file=sys.stderr)
        print(text[-2000:], file=sys.stderr)
        return 1
    fp = json.loads(m.group(0))
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{fp['engine']}.json"
    path.write_text(json.dumps(fp, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {path}: {fp['rows_total']:,} rows, {fp['duids']} DUIDs, {fp['days']} days")
    return 0


def compare(in_dir: Path) -> int:
    fps = {}
    for p in sorted(in_dir.glob("*.json")):
        fp = json.loads(p.read_text(encoding="utf-8"))
        fps[fp["engine"]] = fp

    if len(fps) < 2:
        print(f"only {len(fps)} fingerprint(s) in {in_dir}; need at least 2 to compare")
        return 0

    engines = sorted(fps)
    width = max(len(e) for e in engines)
    print(f"comparing {len(engines)} engines: {', '.join(engines)}\n")
    header = "field".ljust(14) + "".join(e.rjust(20) for e in engines)
    print(header)
    print("-" * len(header))
    for field in EXACT + APPROX:
        row = field.ljust(14)
        for e in engines:
            v = fps[e].get(field)
            row += (f"{v:,}" if isinstance(v, int) else f"{v:.6f}" if isinstance(v, float) else str(v)).rjust(20)
        print(row)
    print()

    base_name = engines[0]
    base = fps[base_name]
    failures: list[str] = []

    for e in engines[1:]:
        fp = fps[e]
        for field in EXACT:
            if fp.get(field) != base.get(field):
                failures.append(
                    f"{field}: {base_name}={base.get(field)!r} but {e}={fp.get(field)!r}"
                )
        for field in APPROX:
            a, b = float(base.get(field, 0)), float(fp.get(field, 0))
            scale = max(abs(a), abs(b), 1.0)
            if abs(a - b) / scale > REL_TOL:
                failures.append(
                    f"{field}: {base_name}={a!r} but {e}={b!r} "
                    f"(relative difference {abs(a - b) / scale:.3e} > {REL_TOL:.0e})"
                )

    if failures:
        print("PARITY FAILED — the engines did not produce the same gold layer:", file=sys.stderr)
        for f in failures:
            print(f"  {f}", file=sys.stderr)
        print(
            "\nBefore changing a tolerance, check the two usual causes:\n"
            "  * a string join key with a trailing space (T-SQL pads on comparison; the\n"
            "    others do not), which changes row counts, not just sums\n"
            "  * one engine's model drifting from the shared business logic — diff the\n"
            "    models/aemo/<engine>/ copies against each other",
            file=sys.stderr,
        )
        return 1

    print(f"PARITY OK — {len(engines)} engines agree on {base['model']} "
          f"({base['rows_total']:,} rows, {base['days']} days)")
    return 0


def main(argv: list[str]) -> int:
    if len(argv) < 2 or argv[1] not in ("capture", "compare"):
        print(__doc__)
        return 2
    d = Path(argv[2]) if len(argv) > 2 else Path("history/parity")
    return capture(d) if argv[1] == "capture" else compare(d)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
