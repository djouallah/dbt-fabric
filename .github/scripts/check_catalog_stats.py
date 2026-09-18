#!/usr/bin/env python3
"""Prove the published page actually says something, before it is published.

WHY THIS EXISTS: `dbt docs generate` is the same shape of failure as `dbt build` on a broken
gate -- it goes GREEN having documented nothing. A target name that stops matching a
models/aemo/<engine>/ folder enables no models, and dbt then writes a perfectly valid page
with an EMPTY DAG and exits 0. A catalog that came back without stats writes a page whose
Details panel is blank, and exits 0 too. Neither is visible from the run log, and a page is
the one artefact nobody diffs.

So the three things the page is FOR are asserted here, off the JSON dbt just wrote:

  * the DAG is the canonical eight models (check_gating.MODELS -- imported, not re-listed);
  * they are in the engine's own schemas, <prefix>duckrun_landing / <prefix>duckrun_mart, so
    a run pointed at a stale isolation schema cannot publish itself as production;
  * at least one model reports Delta stats -- num_rows, bytes and last_modified, all with
    include: true. This is duckrun's own issue #3, and the reason duckrun is the engine whose
    docs get published at all: dbt-fabric's catalog carries an approximate row count and
    nothing else.

Ported from duckrun's tests/tools/check_catalog_stats.py, which exists for the same reason.

    python .github/scripts/check_catalog_stats.py <catalog.json> <manifest.json> [engine]
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_gating import MODELS  # noqa: E402  the canonical eight, in one place

# The Delta log gives all three; a catalog carrying only some of them is the regression.
STATS = ("num_rows", "bytes", "last_modified")


def expected_schemas(engine: str) -> set[str]:
    """What macros/generate_schema_name.sql resolves to for this run.

    DBT_SCHEMA unset/'mart' -> '<engine>_<layer>'; anything else prefixes it, which is the
    isolation lever. Mirroring the macro here is the third copy of the schema rule
    (compact_iceberg.py and layout.py hold the other two) -- keep them in step.
    """
    schema = os.environ.get("DBT_SCHEMA", "mart")
    prefix = "" if schema == "mart" else f"{schema}_"
    return {f"{prefix}{engine}_landing", f"{prefix}{engine}_mart"}


def main(catalog_path: str, manifest_path: str, engine: str) -> int:
    catalog = json.loads(Path(catalog_path).read_text(encoding="utf-8"))
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))

    models = {n["name"]: n for uid, n in manifest["nodes"].items()
              if n.get("resource_type") == "model"}
    if set(models) != MODELS:
        missing = sorted(MODELS - set(models))
        extra = sorted(set(models) - MODELS)
        print(f"the DAG is not the canonical eight: missing {missing}, unexpected {extra}\n"
              f"an empty or short DAG here means the --target no longer matches a "
              f"models/aemo/<engine>/ folder -- dbt documents nothing and exits 0",
              file=sys.stderr)
        return 1

    wanted = expected_schemas(engine)
    wrong = {name: n["schema"] for name, n in models.items() if n["schema"] not in wanted}
    if wrong:
        print(f"models are not in {sorted(wanted)}: {wrong}\n"
              f"publishing these would document an isolation run as if it were production",
              file=sys.stderr)
        return 1

    statted = {uid: n["stats"] for uid, n in catalog.get("nodes", {}).items()
               if n.get("stats", {}).get("has_stats", {}).get("value")}
    full = {uid for uid, s in statted.items()
            if all(s.get(key, {}).get("include") is True for key in STATS)}
    if not full:
        print(f"no model reported the full Delta stats {STATS} in {catalog_path}\n"
              f"the page would show a DAG with an empty Details panel, which is most of "
              f"the reason duckrun is the engine published here",
              file=sys.stderr)
        return 1
    # NOT fatal, and deliberately so: stg_csv_archive_log is a VIEW on duckrun, and anything
    # that is not a Delta table has no log to read the three numbers out of. The regression
    # worth being red about is a catalog with no stats at all, not one model short of them.
    for uid in sorted(set(statted) - full):
        print(f"note: {uid} reports partial stats", file=sys.stderr)

    print(f"{len(models)} models, {len(full)} with Delta stats "
          f"({', '.join(STATS)}), in {sorted(wanted)}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) not in (3, 4):
        raise SystemExit("usage: check_catalog_stats.py <catalog.json> <manifest.json> [engine]")
    raise SystemExit(main(sys.argv[1], sys.argv[2],
                          sys.argv[3] if len(sys.argv) == 4 else "duckrun"))
