#!/usr/bin/env python3
"""What each engine WROTE: table layout and row-count parity over every engine's output, read
back through Delta with duckrun.get_stats() and merged into the run record under `layout`. Run
by the `layout` job of pipeline.yml. Ported from djouallah/direct-lake-parquet-layout's stats.py.

$GITHUB_STEP_SUMMARY gets ONE table -- `headline_table`, one row per engine. Every other table
here is printed to the job log and recorded as numbers in `layout`, never on the run page; see
write_outputs for why.

    BUILD_ENGINES=duckrun,dwh python .github/scripts/layout.py

ONE READER COVERS ALL FIVE. OneLake surfaces every table with a Delta log: duckrun writes Delta,
spark writes Delta, ducklake's `delta_export()` writes each table's `_delta_log` in place,
the Warehouse publishes one per table, and the Iceberg REST catalog's tables are virtualised as
Delta by OneLake. So the question "how many files, row groups, bytes, which encodings, is it
sorted" is asked the same way of every engine -- which is what makes the numbers comparable.
Every engine is best-effort: a table that cannot be read is ABSENT from the document ("not
measured"), never `{}`, and the job stays green; the first run tells whether iceberg's
virtualised log is listed.

TWO ITEMS, NOT FIVE. All the lakehouse engines share the `dbt` lakehouse and are separated by
schema (`<engine>_landing` / `<engine>_mart`, or `<DBT_SCHEMA>_<engine>_*`), so one
`duckrun.connect` over its Tables serves four engines; the Warehouse is the second connection.
The schema rule comes from deploy.mart_schema() so there is no third copy of it.

GLOB, NEVER A BARE `get_stats()`. A bare call sweeps every schema in the item -- each
`test_*` isolation run's tables, every footer over OneLake -- so each engine asks for
`<prefix>_*.*` and gets exactly its own landing and mart schemas: all eight models. The wide
facts live in landing, and that is where files and row groups are interesting.

THE `fct_summary` DEEP DIVE reads ONE `get_stats(detailed=True)` fetch per engine -- DuckDB's
raw `parquet_metadata()` over the mart's live files -- and answers three questions from it:
per-column ENCODINGS (what Power BI has to transcode), ROW-GROUP ORDERING (do the row groups
carve up each column's domain or all span it) and, from a bounded read of one sample file,
RUN LENGTHS in physical row order (was the writer's sort real). Spark's per-file `VORDER`
Delta tag is read off the log as the fourth. The source repo's README explains why each of
those is worth a number; here they are simply recorded.

HEAVY IMPORTS ARE LAZY. duckrun, provision (which reads FABRIC_WORKSPACE_ID at import) and
obstore are imported inside the functions that need them, so the pure functions below import
offline for tests_py/test_layout.py -- the same rule measure_cu.py follows.
"""
from __future__ import annotations

import json
import os
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote

REPO = Path(__file__).resolve().parents[2]
SCRIPTS = Path(__file__).resolve().parent
for p in (str(REPO), str(SCRIPTS)):
    if p not in sys.path:
        sys.path.insert(0, p)

ALL_ENGINES = ("duckrun", "iceberg", "ducklake", "dwh", "spark")
# The item each engine's tables live in: everything but dwh is in the shared lakehouse.
LAKEHOUSE_ENGINES = ("duckrun", "iceberg", "ducklake", "spark")

# The canonical eight, in pipeline order -- inputs first, mart last -- so a disagreement in the
# mart can be traced to its inputs on the rows above it.
TABLES = ("stg_csv_archive_log", "dim_calendar", "dim_duid", "fct_price", "fct_price_today",
          "fct_scada", "fct_scada_today", "fct_summary")
# The one table Power BI reads through Direct Lake, the one parity compares, and the one the
# deep dive profiles.
MART = "fct_summary"

# What actually wrote the parquet behind each engine's Delta log -- the interesting axis when
# two engines produce the same rows in a very different physical layout.
WRITER = {"duckrun": "delta-rs", "iceberg": "duckdb (iceberg)", "ducklake": "duckdb (ducklake)",
          "spark": "spark", "dwh": "warehouse"}

# Display only, and the SAME five labels as pipeline.yml's `plan` step and ci.yml's gating
# matrix: the headline table and the Actions graph should name a leg identically, because they
# are read side by side. `engine` remains the key everywhere else.
LABEL = {"duckrun": "🦆 duckdb · delta-rs", "iceberg": "🧊 duckdb · iceberg",
         "ducklake": "🌊 duckdb · ducklake", "dwh": "🏢 fabric · warehouse",
         "spark": "⚡ fabric · spark"}

# The headline heading counts the engines in words -- "Five engines, one gold layer" reads as a
# claim, "5 engine(s)" reads as a log line. A single-engine run still has to say something true.
NUMBER = {1: "One", 2: "Two", 3: "Three", 4: "Four", 5: "Five"}

# How many physical rows `run_lengths` reads from the sample file. A FIXED ROW BUDGET rather
# than "the first row group", because the engines' row-group sizes differ by orders of magnitude
# and the run count is a fraction OF THE ROWS READ -- the windows must be the same size for the
# numbers to be one measurement.
ORDERING_SAMPLE_ROWS = 4_000_000

# The get_stats() detail carried per table (see stats_for) and how each column is rendered.
# get_stats() column order: catalog, schema, table, total_rows, num_files, num_row_groups,
# avg_row_group, size_mb, vorder, compression.
DETAIL_KEYS = ("schema", "total_rows", "num_files", "num_row_groups",
               "avg_row_group", "size_mb", "vorder", "compression")
DETAIL_COLS = [("total_rows", "rows", "num"), ("num_files", "files", "num"),
               ("num_row_groups", "row groups", "num"), ("avg_row_group", "avg RG rows", "num"),
               ("size_mb", "size MB", "num"), ("vorder", "vorder", "bool"),
               ("compression", "compression", "left")]


def log(msg: str) -> None:
    sys.stderr.write(msg + "\n")


def build_engines() -> list[str]:
    """Only the engines this run built. Reading an engine nobody rebuilt would record an older
    generation's layout under this run's id."""
    want = [e.strip() for e in os.environ.get("BUILD_ENGINES", "").split(",") if e.strip()]
    unknown = [e for e in want if e not in ALL_ENGINES]
    if unknown:
        raise SystemExit(f"BUILD_ENGINES names unknown engine(s) {unknown}")
    return [e for e in ALL_ENGINES if not want or e in want]


def schema_prefix(engine: str) -> str:
    """`<engine>` or `<DBT_SCHEMA>_<engine>` -- generate_schema_name()'s rule, via deploy.py."""
    from deploy import mart_schema

    m = mart_schema(engine)
    if not m.endswith("_mart"):
        raise ValueError(f"unexpected mart schema {m!r} for {engine}")
    return m[: -len("_mart")]


# --------------------------------------------------------------------------------- readers

def reader(guid: str, name: str):
    """A read-only duckrun session over one item's Tables. The OneLake token comes from
    duckrun's own OIDC exchange on the runner, exactly as in the `land` job."""
    import duckrun
    import provision

    con = duckrun.connect(provision.abfss(guid, "Tables"), read_only=True, name=name)
    transport = os.environ.get("AZURE_TRANSPORT_OPTION_TYPE")
    if transport:
        try:
            con.con.sql(f"SET GLOBAL azure_transport_option_type='{transport}'")
        except Exception:  # noqa: BLE001
            pass
    return con


def stats_for(con, prefix: str) -> dict:
    """{table: {schema, total_rows, num_files, num_row_groups, avg_row_group, size_mb, vorder,
    compression}} for every table in `<prefix>_landing` and `<prefix>_mart`.

    `<prefix>_*.*` rather than a bare get_stats(): see the module docstring. A pattern that
    matches nothing raises, and that is reported as "not measured" by the caller.
    """
    rows = con.get_stats(f"{prefix}_*.*").fetchall()
    return {r[2]: dict(zip(DETAIL_KEYS, (r[1], r[3], r[4], r[5], r[6], r[7], r[8], r[9])))
            for r in rows}


def mart_chunks(con, table: str):
    """`(name -> index, rows)` of `get_stats(table, detailed=True)` -- ONE footer read for
    everything the deep dive asks. `table` MUST be schema-qualified (`duckrun_mart.fct_summary`):
    a bare name is looked up in the current schema and does not resolve.

    `(None, [])` on any failure: every consumer treats that as "not measured".
    """
    try:
        rel = con.get_stats(table, detailed=True)
        at = {name: i for i, name in enumerate(d[0] for d in rel.description)}
        return at, rel.fetchall()
    except Exception as e:  # noqa: BLE001 -- never fail the layout job
        log(f"  parquet metadata unavailable for {table} ({type(e).__name__}: {e})")
        return None, []


# ------------------------------------------------------------------------ pure aggregations

def encodings_from(at, rows) -> dict:
    """`{column: {encodings, type, dict_pages, chunks, mb}}` for one engine's MART parquet.

    Pure: aggregates the rows `mart_chunks` fetched and reads nothing itself. One row per
    COLUMN, never per chunk: the distinct encodings across every chunk (sorted, so two engines
    are string-comparable), how many chunks carry a dictionary page, and the compressed MB.
    Direct Lake remaps a dictionary-encoded chunk straight into VertiPaq's own dictionary and
    re-encodes a PLAIN one from raw values at load -- that is the whole reason this exists.
    """
    if at is None or not rows:
        return {}
    need = ("path_in_schema", "type", "encodings", "dictionary_page_offset", "total_compressed_size")
    if any(k not in at for k in need):
        log(f"  parquet_metadata is missing {[k for k in need if k not in at]}")
        return {}
    cols: dict = {}
    for r in rows:
        c = cols.setdefault(r[at["path_in_schema"]],
                            {"encodings": set(), "type": r[at["type"]], "dict_pages": 0,
                             "chunks": 0, "mb": 0.0})
        for enc in str(r[at["encodings"]] or "").split(","):
            if enc.strip():
                c["encodings"].add(enc.strip())
        c["dict_pages"] += 1 if r[at["dictionary_page_offset"]] else 0
        c["chunks"] += 1
        c["mb"] += (r[at["total_compressed_size"]] or 0) / 1048576
    return {name: {**c, "encodings": sorted(c["encodings"]), "mb": round(c["mb"], 2)}
            for name, c in sorted(cols.items())}


def _stat(v):
    """A row-group min/max coerced to something SORTABLE, tagged so mixed coercions compare.

    `parquet_metadata()` renders `stats_min_value`/`stats_max_value` as VARCHAR whatever the
    physical type, so comparing them raw is lexicographic -- and `"9" > "10000"` would report a
    perfectly ascending numeric column as fully overlapping. Numbers are cast; everything else
    stays a string, which is right for ISO dates and for DUIDs.
    """
    for cast in (int, float):
        try:
            return (0, cast(v))
        except (TypeError, ValueError):
            pass
    return (1, str(v))


def _file_rows(at, rows) -> dict:
    """`{file: rows}` from the chunk metadata -- row counts summed over DISTINCT row groups.
    The fetch is one row per (row group, COLUMN CHUNK), so summing blindly multiplies every
    file's row count by its column count."""
    seen, out = set(), {}
    for r in rows:
        key = (r[at["file_name"]], r[at["row_group_id"]])
        if key in seen:
            continue
        seen.add(key)
        out[key[0]] = out.get(key[0], 0) + int(r[at["row_group_num_rows"]] or 0)
    return out


def _columns(at, rows) -> list:
    """The mart's top-level column names, in file order. Nested paths (`a.b`) are skipped."""
    out = []
    for r in rows:
        c = r[at["path_in_schema"]]
        if c and "." not in c and c not in out:
            out.append(c)
    return out


def rg_ordering(at, rows) -> dict:
    """`{column: {rg_overlap_pct, rgs, [inexact]}}` -- do the row groups CARVE UP the domain or
    all span it? For each column: one [min, max] per row group, sorted by min, then the share of
    CONSECUTIVE pairs that overlap. 0% = the row groups partition the column's range (what a
    global sort produces); ~100% = every row group spans the whole domain (no order at all).

    THE COMPARISON IS STRICT (`min_i < max_{i-1}`), not a rounding choice: a row-group boundary
    almost never lands on a value boundary, so under a PERFECT sort the last row of one group and
    the first of the next hold the SAME value, and a touch-counts-as-overlap rule scores a
    flawlessly sorted column 100%. Sorted by min rather than file order so a descending sort
    does not look like a shuffle. NULL stats are dropped (parquet omits them for an all-NULL
    chunk); a column with fewer than two row groups is ABSENT; `inexact` marks a truncated
    string statistic, surfaced for the reader to discount.
    """
    need = ("file_name", "row_group_id", "path_in_schema", "stats_min_value", "stats_max_value")
    if at is None or not rows or any(k not in at for k in need):
        if rows:
            log(f"  parquet_metadata is missing "
                f"{[k for k in need if at is None or k not in at]} -- no rg ordering")
        return {}
    per: dict = {}
    for r in rows:
        lo, hi = r[at["stats_min_value"]], r[at["stats_max_value"]]
        if lo is None or hi is None:
            continue
        exact = not any(k in at and r[at[k]] is False for k in ("min_is_exact", "max_is_exact"))
        per.setdefault(r[at["path_in_schema"]], []).append((_stat(lo), _stat(hi), exact))
    out = {}
    for col, ent in per.items():
        if "." in col or len(ent) < 2:
            continue
        ent.sort(key=lambda e: (e[0], e[1]))
        overlaps = sum(1 for i in range(1, len(ent)) if ent[i][0] < ent[i - 1][1])
        d = {"rg_overlap_pct": round(100 * overlaps / (len(ent) - 1), 1), "rgs": len(ent)}
        if not all(e[2] for e in ent):
            d["inexact"] = True
        out[col] = d
    return out


def run_lengths(con, at, rows) -> dict:
    """`{file, rows, runs: {column: n}}` -- adjacent equal-value RUNS in physical row order,
    over the first ORDERING_SAMPLE_ROWS of the largest live file (ties by name, so two runs of
    the same layout sample the same place).

    This is the question `rg_ordering` cannot answer: whether the writer reordered rows INSIDE
    a row group. `runs` ~ `rows` means the values are interleaved; `runs` << `rows` means equal
    values were brought together, which is what makes RLE and dictionary encoding pay. Read it
    as a RATIO of the rows sampled; a near-unique column (`mw`, `price`) is the built-in control.
    ORDERED BY `file_row_number` explicitly, so the count does not depend on DuckDB's scan
    order or thread count. Best-effort: `{}` on any failure.
    """
    need = ("file_name", "row_group_id", "row_group_num_rows", "path_in_schema")
    if at is None or not rows or any(k not in at for k in need):
        return {}
    try:
        files = _file_rows(at, rows)
        cols = _columns(at, rows)
        if not files or not cols:
            return {}
        path = sorted(files, key=lambda f: (-files[f], f))[0]
        span = min(files[path], ORDERING_SAMPLE_ROWS)
        # `IS DISTINCT FROM` counts NULL == NULL as one run, and the first row's LAG is NULL and
        # so opens a run -- the sum is exactly the number of maximal equal-value spans.
        chg = ",\n           ".join(
            f'CASE WHEN "{c}" IS DISTINCT FROM LAG("{c}") OVER w THEN 1 ELSE 0 END AS chg_{i}'
            for i, c in enumerate(cols))
        sel = ", ".join(f"SUM(chg_{i}) AS runs_{i}" for i in range(len(cols)))
        sql = (f"SELECT COUNT(*) AS n, {sel} FROM (\n"
               f"    SELECT {chg}\n"
               f"    FROM read_parquet(['{path.replace(chr(39), chr(39) * 2)}'], "
               f"file_row_number=true)\n"
               f"    WHERE file_row_number < {int(span)}\n"
               f"    WINDOW w AS (ORDER BY file_row_number)\n) t")
        res = con.con.sql(sql).fetchone()
        if not res or not res[0]:
            return {}
        return {"file": path.rsplit("/", 1)[-1], "rows": int(res[0]),
                "runs": {c: int(res[i + 1] or 0) for i, c in enumerate(cols)}}
    except Exception as e:  # noqa: BLE001 -- never fail the layout job
        log(f"  run lengths unavailable ({type(e).__name__}: {e})")
        return {}


def _vorder_from_log(actions, live_files) -> dict:
    """`{tagged, files, unknown}` -- how many LIVE parquet files carry Fabric's `VORDER` add tag.

    Spark records V-Order per file as an `add.tags` entry, which duckrun's `vorder` column (a
    table PROPERTY nobody sets) cannot see -- so this reads the commit JSON itself. Pure, so
    the parsing is testable without a store. Last `add` per path wins and REMOVES ARE NOT
    REPLAYED: the live set comes from the file list `mart_chunks` already read, tombstones
    excluded. Matched on BASENAME (`file_name` is a full URI, `add.path` table-relative and
    URL-encoded; the names are GUID-bearing). `unknown` counts live files no JSON commit
    describes -- their add was folded into a checkpoint -- reported rather than guessed at.
    """
    live = {str(f).rsplit("/", 1)[-1] for f in live_files}
    tags = {}
    for a in actions:
        add = (a or {}).get("add")
        if not isinstance(add, dict) or not add.get("path"):
            continue
        tags[unquote(str(add["path"])).rsplit("/", 1)[-1]] = add.get("tags") or {}
    return {"tagged": sum(1 for f in live
                          if str((tags.get(f) or {}).get("VORDER", "")).lower() == "true"),
            "files": len(live),
            "unknown": sum(1 for f in live if f not in tags)}


def vorder_tags(con, guid: str, schema: str, table: str, live_files) -> dict:
    """`_vorder_from_log` over the table's `_delta_log/*.json`, read with obstore through the
    session's own storage options. Commit files are zero-padded, so a lexicographic sort IS
    commit order. ONLY MEANINGFUL FOR A SPARK-WRITTEN TABLE: the tag is the Fabric Spark
    writer's marker; the Warehouse V-Orders by default and stamps none, delta-rs and DuckDB
    never do. Best-effort: `{}` on anything at all."""
    try:
        import obstore
        import provision
        from dbt.adapters.duckrun import objectstore, secret

        base = f"{provision.abfss(guid, 'Tables')}/{schema}/{table}/_delta_log"
        store = objectstore.build_store(base, secret.refreshed(con.storage_options))
        keys = sorted(o["path"].rsplit("/", 1)[-1]
                      for batch in obstore.list(store) for o in batch
                      if str(o["path"]).endswith(".json"))
        actions = []
        for k in keys:
            body = bytes(obstore.get(store, k).bytes()).decode("utf-8")
            actions += [json.loads(line) for line in body.splitlines() if line.strip()]
        return _vorder_from_log(actions, live_files)
    except Exception as e:  # noqa: BLE001 -- never fail the layout job
        log(f"  vorder tags unavailable for {schema}.{table} ({type(e).__name__}: {e})")
        return {}


def ordering_for(con, guid: str, schema: str, at, rows, engine: str) -> dict:
    """DID THE WRITER PHYSICALLY REORDER THE ROWS? Three signals over the mart, one document:
    `columns[c].rg_overlap_pct` (across row groups, free), `columns[c].runs` (within a file,
    one bounded read) and, for spark only, `vorder_files` (Fabric's per-file tag). Each part
    fails on its own and is absent when not measured -- never a zero."""
    if at is None or not rows or not schema:
        return {}
    doc: dict = {"table": f"{schema}.{MART}"}
    cols = rg_ordering(at, rows)
    rl = run_lengths(con, at, rows)
    if rl:
        doc["sample"] = {"file": rl["file"], "rows": rl["rows"]}
        for c, n in rl["runs"].items():
            cols.setdefault(c, {})["runs"] = n
    if "file_name" in at and engine == "spark":
        vt = vorder_tags(con, guid, schema, MART, {r[at["file_name"]] for r in rows})
        if vt:
            doc["vorder_files"] = vt
    if cols:
        doc["columns"] = dict(sorted(cols.items()))
    return doc if len(doc) > 1 else {}


# --------------------------------------------------------------------------------- rendering

def fmt(v, kind: str) -> str:
    if v is None:
        return "—"
    if kind == "num":
        return f"{v:,.1f}".rstrip("0").rstrip(".") if isinstance(v, float) else f"{v:,}"
    if kind == "bool":
        return "✅" if v else "·"
    return f"`{v}`" if kind == "left" else str(v)


def engine_total(per_engine: dict, engine: str, key: str):
    """One engine's `key` summed over every table it was measured on, or None if it was not
    measured at all. None and 0 are different claims and must render differently."""
    if not per_engine.get(engine):
        return None
    return sum(d.get(key) or 0 for d in per_engine[engine].values())


def headline_table(per_engine: dict, engines: list[str], encodings: dict,
                   out: list[str]) -> None:
    """THE ONLY TABLE ON THE RUN PAGE: one row per engine, no per-table breakdown. The claim it
    has to make in one glance is the repo's whole claim -- the engines are interchangeable (same
    rows) and what comes out is a well-shaped parquet table. Everything underneath it (per
    table, per column, physical order) is in the run record's `layout` and in the job log; the
    summary page carries this and nothing else.

    EVERY COLUMN IS `fct_summary` -- the table Power BI reads through Direct Lake, the table
    parity compares, the one this repo is about. Nothing here is summed over the other seven:
    a row count that mixes tables is not the parity claim, and it was also wrong in a way that
    read as a real difference -- `stg_csv_archive_log` is a VIEW on four engines and leaves
    nothing to measure, so the two engines that DO materialize it looked like the outliers.
    ⚠️ marks an engine whose row count differs from the others.

    NO V-ORDER COLUMN. It cannot be stated in one cell without lying: only the Fabric Spark
    writer stamps `add.tags.VORDER`, the Warehouse V-Orders by DEFAULT and stamps nothing, and
    the DuckDB writers neither stamp nor V-Order -- so the same blank cell would mean three
    different things. `ordering_table` still reports the per-file tags where they exist."""
    out.append(f"## 🏁 {NUMBER.get(len(engines), str(len(engines)))} "
               f"{'engine' if len(engines) == 1 else 'engines'}, one gold layer\n")
    out.append(f"<sub>Every column is <code>{MART}</code>, the table Power BI reads through "
               f"Direct Lake. <b>encoding</b> counts its columns whose every chunk carries a "
               f"dictionary page — Direct Lake remaps those straight into VertiPaq's "
               f"dictionary and re-encodes a PLAIN one from raw values at load. ⚠️ = this "
               f"engine's row count differs from the others.</sub>\n")
    heads = ["engine", "writer", "rows", "size MB", "files", "row groups", "avg RG rows",
             "compression", "encoding"]
    out.append("| " + " | ".join(heads) + " |")
    out.append("| --- | --- | --: | --: | --: | --: | --: | --- | --- |")

    marts = {e: (per_engine.get(e) or {}).get(MART) or {} for e in engines}
    present = [m["total_rows"] for m in marts.values() if m.get("total_rows") is not None]
    # The majority row count, so the ⚠️ lands on the engine that disagrees rather than on all
    # of them. One engine measured, or all of them agreeing, flags nothing.
    agreed = Counter(present).most_common(1)[0][0] if present else None
    for e in engines:
        mart = marts[e]
        rows = mart.get("total_rows")
        cells = [fmt(rows, "num") + ("" if rows is None or rows == agreed else " ⚠️"),
                 fmt(None if mart.get("size_mb") is None else round(mart["size_mb"], 1), "num")]
        cells += [fmt(mart.get(k), kind)
                  for k, kind in (("num_files", "num"), ("num_row_groups", "num"),
                                  ("avg_row_group", "num"), ("compression", "left"))]
        cells.append(encoding_cell((encodings or {}).get(e)))
        out.append(f"| {LABEL.get(e, e)} | `{WRITER.get(e, e)}` | " + " | ".join(cells) + " |")
    out.append("")


def encoding_cell(encodings: dict | None) -> str:
    """`<n>/<total> dict` for one engine's MART columns, ⚠️ when any column is not fully
    dictionary-encoded. A column counts only when EVERY chunk carries a dictionary page
    (`dict_pages == chunks`): one PLAIN chunk is one chunk Direct Lake re-encodes from raw
    values at load, and a column that is dictionary-encoded in nine files out of ten is not a
    dictionary-encoded column. encoding_table has the per-column breakdown."""
    if not encodings:
        return "—"
    full = sum(1 for c in encodings.values() if c.get("chunks") and c["dict_pages"] == c["chunks"])
    return f"{full:,}/{len(encodings):,} dict" + ("" if full == len(encodings) else " ⚠️")


def parity_table(per_engine: dict, engines: list[str], out: list[str]) -> None:
    """Row counts side by side. ⚠️ = differs or missing across engines. The last two rows carry
    per-engine totals: rows must line up; MB legitimately differs by writer and compression."""
    out.append("## 🧮 Row-count parity\n")
    out.append("<sub>Every model, in pipeline order. ⚠️ = differs or missing across the engines "
               "this run built.</sub>\n")
    out.append("| table | " + " | ".join(engines) + " |")
    out.append("| --- | " + " | ".join("--:" for _ in engines) + " |")
    for t in TABLES:
        vals = [(per_engine.get(e) or {}).get(t, {}).get("total_rows") for e in engines]
        present = [v for v in vals if v is not None]
        match = len(present) == len(engines) and len(set(present)) == 1
        out.append(f"| `{t}`{'' if match else ' ⚠️'} | "
                   + " | ".join(fmt(v, "num") for v in vals) + " |")

    def total(e, key):
        return engine_total(per_engine, e, key)

    rows = [total(e, "total_rows") for e in engines]
    present = [v for v in rows if v is not None]
    match = len(present) == len(engines) and len(set(present)) == 1
    out.append(f"| **total rows**{'' if match else ' ⚠️'} | "
               + " | ".join(fmt(v, "num") for v in rows) + " |")
    mbs = [total(e, "size_mb") for e in engines]
    out.append("| **total MB** | "
               + " | ".join(fmt(None if v is None else round(v, 1), "num") for v in mbs) + " |")
    out.append("")


def detail_tables(per_engine: dict, engines: list[str], out: list[str]) -> None:
    """Full get_stats() detail as ONE flat table, rows grouped by table so the engines sit
    directly under each other -- the only layout in which "same rows, wildly different
    files/row-groups" is visible at a glance."""
    out.append("## 🔬 Physical layout\n")
    heads = ["table", "engine", "writer"] + [h for _, h, _ in DETAIL_COLS]
    aligns = ["---", "---", "---"] + ["--:" if k == "num" else "---" for _, _, k in DETAIL_COLS]
    out.append("| " + " | ".join(heads) + " |")
    out.append("| " + " | ".join(aligns) + " |")
    for t in TABLES:
        vals = [(per_engine.get(e) or {}).get(t) for e in engines]
        counts = [d.get("total_rows") for d in vals if d is not None]
        agree = len(counts) == len(engines) and len(set(counts)) == 1
        for i, (e, d) in enumerate(zip(engines, vals)):
            name = f"`{t}`{'' if agree else ' ⚠️'}" if i == 0 else ""
            cells = [fmt(None if d is None else d.get(key), kind) for key, _, kind in DETAIL_COLS]
            out.append(f"| {name} | {e} | `{WRITER.get(e, e)}` | " + " | ".join(cells) + " |")
    out.append("")


def encoding_table(encodings: dict, engines: list[str], out: list[str]) -> None:
    """`fct_summary`'s per-column parquet encoding, engines side by side -- whether two engines
    hand Power BI the same thing to transcode."""
    have = [e for e in engines if encodings.get(e)]
    if not have:
        return
    out.append(f"## 🔤 `{MART}` column encoding\n")
    out.append("| column | type | " + " | ".join(have) + " |")
    out.append("| --- | --- | " + " | ".join("---" for _ in have) + " |")
    for col in sorted({c for e in have for c in encodings[e]}):
        typ = next((encodings[e][col]["type"] for e in have if col in encodings[e]), "—")
        cells = []
        for e in have:
            c = encodings[e].get(col)
            cells.append("—" if not c else
                         f"`{'+'.join(c['encodings'])}`"
                         f"{'' if c['dict_pages'] else ' ⚠️ no dict'} · {c['mb']:,.1f} MB")
        out.append(f"| `{col}` | `{typ}` | " + " | ".join(cells) + " |")
    out.append("")


def ordering_table(ordering: dict, engines: list[str], out: list[str]) -> None:
    """`fct_summary`'s physical row order, engines side by side -- the V-Order reality check."""
    have = [e for e in engines if ordering.get(e)]
    if not have:
        return
    out.append(f"## 🔀 `{MART}` physical row order\n")
    out.append("<sub><b>RG overlap</b>: consecutive row-group [min,max] ranges, sorted by min, that "
               "overlap — 0% = the row groups partition the column's range, ~100% = every row "
               "group spans everything. <b>runs</b>: adjacent equal-value spans in the first rows "
               "of the largest file, in physical order — runs ≪ rows means equal values were "
               "brought together. A near-unique column (`mw`, `price`) is the control. "
               "<code>*</code> = a truncated string statistic. <b>V-Order files</b> is the "
               "per-file Delta <code>add.tags.VORDER</code>, which only the Fabric Spark writer "
               "stamps; the Warehouse V-Orders by default and writes no tag, so it reads "
               "<code>n/a</code>.</sub>\n")
    out.append("| engine | V-Order files | sample |")
    out.append("| --- | --- | --- |")
    for e in have:
        d = ordering[e]
        v, s = d.get("vorder_files") or {}, d.get("sample") or {}
        vc = ("n/a (warehouse)" if not v and e == "dwh"
              else "—" if not v else (f"{v['tagged']:,}/{v['files']:,}"
                                      + (f" +{v['unknown']:,}?" if v.get("unknown") else "")))
        sc = "—" if not s else f"`{s['file']}` · {s['rows']:,} rows"
        out.append(f"| {e} | {vc} | {sc} |")
    out.append("")
    out.append("| column | " + " | ".join(have) + " |")
    out.append("| --- | " + " | ".join("---" for _ in have) + " |")
    for col in sorted({c for e in have for c in (ordering[e].get("columns") or {})}):
        cells = []
        for e in have:
            c = (ordering[e].get("columns") or {}).get(col)
            if not c:
                cells.append("—")
                continue
            pct, runs = c.get("rg_overlap_pct"), c.get("runs")
            cells.append(
                ("—" if pct is None else f"{pct:,.0f}%{'*' if c.get('inexact') else ''} RG overlap")
                + (f" · {runs:,} runs" if runs is not None else ""))
        out.append(f"| `{col}` | " + " | ".join(cells) + " |")
    out.append("")


def build_doc(per_engine: dict, engines: list[str], items: dict, prefixes: dict,
              encodings: dict, ordering: dict) -> dict:
    """The layout document: run stamp, what the build ran on, per-engine item + GUID + schema
    prefix, per-table detail, and the mart deep dive. Absent rather than `{}` wherever nothing
    was measured -- "not measured" and "nothing there" are different claims."""
    doc = {
        "run": {"id": os.environ.get("GITHUB_RUN_ID"),
                "sha": os.environ.get("GITHUB_SHA"),
                "written": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")},
        "config": {"vcores": os.environ.get("FABRIC_CORES") or None,
                   **({"spark": {"resource_profile": os.environ.get("SPARK_RESOURCE_PROFILE") or None}}
                      if "spark" in engines else {})},
        "engines": {e: {"item": items[e][1], "kind": items[e][0], "guid": items[e][2],
                        "schema_prefix": prefixes.get(e), "writer": WRITER.get(e, e)}
                    for e in engines if e in items},
        "tables": list(TABLES),
        "detail_keys": list(DETAIL_KEYS),
        "stats": {e: per_engine[e] for e in engines if per_engine.get(e)},
        **({"encodings": {e: encodings[e] for e in engines if encodings.get(e)}}
           if any(encodings.get(e) for e in engines) else {}),
        **({"ordering": {e: ordering[e] for e in engines if ordering.get(e)}}
           if any(ordering.get(e) for e in engines) else {}),
    }
    return doc


# --------------------------------------------------------------------------------- driver

def fabric_items(engines: list[str]) -> dict:
    """{engine: (kind, name, guid)} -- the item each engine's tables live in, by display name."""
    import provision

    out = {}
    lake = [e for e in engines if e in LAKEHOUSE_ENGINES]
    if lake:
        guid = provision.find("lakehouses", provision.DATA_LAKEHOUSE)
        if guid:
            for e in lake:
                out[e] = ("Lakehouse", provision.DATA_LAKEHOUSE, guid)
        else:
            log(f"  lakehouse {provision.DATA_LAKEHOUSE} not found -- {lake} not measured")
    if "dwh" in engines:
        guid = provision.find("warehouses", provision.DWH_WAREHOUSE)
        if guid:
            out["dwh"] = ("Warehouse", provision.DWH_WAREHOUSE, guid)
        else:
            log(f"  warehouse {provision.DWH_WAREHOUSE} not found -- dwh not measured")
    return out


def one_item(guid: str, name: str, engines: list[str], prefixes: dict) -> dict:
    """{engine: (stats, encodings, ordering)} for every engine whose tables live in this item.
    ONE connection per item, one `mart_chunks` fetch per engine. Engines are independent: a
    failure on one is logged and that engine is simply absent."""
    con = reader(guid, name)
    out = {}
    for e in engines:
        try:
            st = stats_for(con, prefixes[e])
        except Exception as ex:  # noqa: BLE001
            log(f"  {e}: no tables under {prefixes[e]}_* in {name} ({type(ex).__name__}: {ex})")
            out[e] = ({}, {}, {})
            continue
        schema = (st.get(MART) or {}).get("schema")
        at, chunks = mart_chunks(con, f"{schema}.{MART}") if schema else (None, [])
        out[e] = (st, encodings_from(at, chunks), ordering_for(con, guid, schema, at, chunks, e))
    return out


def write_outputs(doc: dict, headline: str, detail: str) -> None:
    """Three sinks, ONE document, and deliberately not the same slice of it in each.

    $GITHUB_STEP_SUMMARY gets the HEADLINE ONLY. The run page is read by people who want to
    know whether the engines agree and what shape the parquet came out in; four more tables of
    per-table, per-column and per-row-group detail below it buries that. The detail is not lost
    -- it goes to stdout (the job log, where it is being read for a reason) and, as numbers
    rather than markdown, into the run record's `layout`, which is what outlives the run.
    LAYOUT_JSON optionally names a file too, for a by-hand run."""
    import record

    print(headline)
    print(detail)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(headline + "\n")
    record.merge({"layout": doc})
    path = os.environ.get("LAYOUT_JSON", "").strip()
    if path:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=1, sort_keys=True, default=str)
        log(f"  wrote {path}")


def main() -> int:
    engines = build_engines()
    items = fabric_items(engines)
    prefixes = {e: schema_prefix(e) for e in engines}
    log(f"layout: engines {engines}; schemas {prefixes}")

    # One worker per ITEM (the lakehouse, the warehouse): they are independent, and the
    # lakehouse alone can take many minutes over OneLake, so wall-clock = slowest item.
    by_item: dict = {}
    for e, (kind, name, guid) in items.items():
        by_item.setdefault((guid, name), []).append(e)
    per_engine, encodings, ordering = {}, {}, {}
    with ThreadPoolExecutor(max_workers=max(1, len(by_item))) as pool:
        futures = {key: pool.submit(one_item, key[0], key[1], engs, prefixes)
                   for key, engs in by_item.items()}
    for (guid, name), fut in futures.items():
        try:
            got = fut.result()
        except Exception as ex:  # noqa: BLE001
            log(f"  {name} ({guid}) FAILED: {type(ex).__name__}: {ex}")
            continue
        for e, (st, enc, ordr) in got.items():
            per_engine[e], encodings[e], ordering[e] = st, enc, ordr
            v = (ordr.get("vorder_files") or {}) if ordr else {}
            log(f"  {e} ({name}): {len(st)} table(s), "
                f"{sum(d.get('total_rows') or 0 for d in st.values()):,} rows total, "
                f"{len(enc)} {MART} column(s) profiled"
                + (f", {v['tagged']}/{v['files']} V-Ordered file(s)" if v else ""))

    # The run page gets `head` and nothing else; `rest` goes to the log and, as numbers, to the
    # run record. write_outputs.
    head: list[str] = []
    headline_table(per_engine, engines, encodings, head)
    rest: list[str] = []
    parity_table(per_engine, engines, rest)
    detail_tables(per_engine, engines, rest)
    encoding_table(encodings, engines, rest)
    ordering_table(ordering, engines, rest)
    doc = build_doc(per_engine, engines, items, prefixes, encodings, ordering)
    write_outputs(doc, "\n".join(head), "\n".join(rest))

    measured = [e for e in engines if per_engine.get(e)]
    log(f"layout: measured {measured}; not measured {[e for e in engines if e not in measured]}")
    # Best-effort per engine, but NOTHING readable means the reader is broken, and that must
    # show -- the job is continue-on-error, so red here costs nothing downstream.
    return 0 if measured else 1


if __name__ == "__main__":
    raise SystemExit(main())
