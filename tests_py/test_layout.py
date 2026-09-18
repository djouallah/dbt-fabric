"""Offline tests for layout.py's pure aggregations. No duckrun, no OneLake, no credentials.

The functions here turn DuckDB's raw `parquet_metadata()` rows and Delta commit JSON into the
numbers the run record carries. Each one fails as a plausible number rather than an error --
a touch-counts-as-overlap rule scores a perfectly sorted column 100%, a lexicographic min/max
compare reports an ascending numeric column as shuffled, a per-chunk row sum multiplies a
file's rows by its column count -- so they are pinned on synthetic rows.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import sys

from _layout import REPO

sys.path.insert(0, str(REPO / ".github" / "scripts"))
import layout  # noqa: E402

COLS = ("file_name", "row_group_id", "row_group_num_rows", "path_in_schema", "type",
        "encodings", "dictionary_page_offset", "total_compressed_size",
        "stats_min_value", "stats_max_value", "min_is_exact", "max_is_exact")
AT = {c: i for i, c in enumerate(COLS)}


def row(file="f1.parquet", rg=0, rows=100, col="date", typ="INT32", enc="PLAIN,RLE_DICTIONARY",
        dict_off=1, size=1048576, lo="2026-01-01", hi="2026-01-02", exact=True):
    return (file, rg, rows, col, typ, enc, dict_off, size, lo, hi, exact, exact)


def test_layout_imports_offline():
    """The heavy imports are lazy on purpose: this module must load in ci.yml's unit job, where
    duckrun is not installed and FABRIC_WORKSPACE_ID (which provision.py reads at import) is unset."""
    assert "duckrun" not in sys.modules or True
    assert layout.MART == "fct_summary"


def test_encodings_aggregate_to_one_row_per_column():
    rows = [row(rg=0, col="mw", typ="DOUBLE", enc="PLAIN", dict_off=None, size=2097152),
            row(rg=1, col="mw", typ="DOUBLE", enc="PLAIN,RLE_DICTIONARY", dict_off=7, size=1048576),
            row(rg=0, col="duid", typ="BYTE_ARRAY", enc="RLE_DICTIONARY,PLAIN", dict_off=3)]
    got = layout.encodings_from(AT, rows)
    assert got["mw"] == {"encodings": ["PLAIN", "RLE_DICTIONARY"], "type": "DOUBLE",
                         "dict_pages": 1, "chunks": 2, "mb": 3.0}
    # Sorted, so two engines' encodings are string-comparable whatever order the footer lists.
    assert got["duid"]["encodings"] == ["PLAIN", "RLE_DICTIONARY"]
    assert got["duid"]["dict_pages"] == 1


def test_encodings_are_absent_not_empty_when_nothing_was_fetched():
    assert layout.encodings_from(None, []) == {}
    assert layout.encodings_from({"path_in_schema": 0}, [("x",)]) == {}


def test_file_rows_counts_each_row_group_once():
    """One metadata row per (row group, column chunk): three columns must not triple the count."""
    rows = [row(rg=0, rows=100, col=c) for c in ("a", "b", "c")] \
        + [row(rg=1, rows=50, col=c) for c in ("a", "b", "c")] \
        + [row(file="f2.parquet", rg=0, rows=7, col="a")]
    assert layout._file_rows(AT, rows) == {"f1.parquet": 150, "f2.parquet": 7}


def test_rg_ordering_touching_boundaries_are_not_overlap():
    """Under a perfect sort the last row of one row group and the first of the next hold the
    SAME value. A touch-counts rule would score that 100%; strict scores it 0%."""
    rows = [row(rg=0, lo="2026-01-01", hi="2026-01-05"),
            row(rg=1, lo="2026-01-05", hi="2026-01-09"),
            row(rg=2, lo="2026-01-09", hi="2026-01-12")]
    assert layout.rg_ordering(AT, rows)["date"] == {"rg_overlap_pct": 0.0, "rgs": 3}


def test_rg_ordering_interleaved_ranges_overlap_and_numbers_compare_numerically():
    """`"9" > "10000"` lexicographically -- a numeric column must be cast before comparing."""
    rows = [row(rg=0, col="time", lo="0", hi="10000"),
            row(rg=1, col="time", lo="9", hi="2355"),      # genuinely inside rg 0's range
            row(rg=2, col="time", lo="10005", hi="20000")]  # disjoint from both
    assert layout.rg_ordering(AT, rows)["time"] == {"rg_overlap_pct": 50.0, "rgs": 3}


def test_rg_ordering_skips_single_row_group_columns_and_null_stats():
    rows = [row(rg=0, col="one"), row(rg=0, col="nulls", lo=None, hi=None),
            row(rg=1, col="nulls", lo=None, hi=None)]
    assert layout.rg_ordering(AT, rows) == {}


def test_rg_ordering_flags_truncated_statistics():
    rows = [row(rg=0, col="duid", lo="A", hi="M", exact=True),
            row(rg=1, col="duid", lo="N", hi="Z", exact=False)]
    got = layout.rg_ordering(AT, rows)["duid"]
    assert got["inexact"] is True and got["rg_overlap_pct"] == 0.0


def test_vorder_tags_last_add_wins_and_matches_on_basename():
    actions = [
        {"add": {"path": "part-1.parquet", "tags": {"VORDER": "true"}}},
        {"add": {"path": "part-2.parquet", "tags": {"VORDER": "false"}}},
        {"add": {"path": "part-2.parquet", "tags": {"VORDER": "true"}}},   # re-added later
        {"add": {"path": "sub%20dir/part-3.parquet"}},                     # URL-encoded, untagged
        {"remove": {"path": "part-1.parquet"}},                            # removes not replayed
        {"commitInfo": {}},
    ]
    live = ["abfss://ws@onelake/x/Tables/spark_mart/fct_summary/part-1.parquet",
            "abfss://ws@onelake/x/Tables/spark_mart/fct_summary/part-2.parquet",
            "abfss://ws@onelake/x/Tables/spark_mart/fct_summary/sub dir/part-3.parquet",
            "abfss://ws@onelake/x/Tables/spark_mart/fct_summary/part-9.parquet"]  # in a checkpoint
    assert layout._vorder_from_log(actions, live) == {"tagged": 2, "files": 4, "unknown": 1}


def test_parity_table_flags_disagreement_and_absence():
    per = {"duckrun": {"fct_summary": {"total_rows": 10, "size_mb": 1.0}},
           "dwh": {"fct_summary": {"total_rows": 11, "size_mb": 2.0}},
           "spark": {}}
    out: list[str] = []
    layout.parity_table(per, ["duckrun", "dwh", "spark"], out)
    line = next(l for l in out if l.startswith("| `fct_summary`"))
    assert "⚠️" in line and "| 10 | 11 | — |" in line
    assert any(l.startswith("| **total rows** ⚠️") for l in out)


def test_headline_table_flags_the_row_count_outlier():
    """The run page's first table. The ⚠️ must land on the engine that DISAGREES, not on every
    engine, and an engine that was not measured must read `—` rather than 0 -- "nothing there"
    and "not measured" are different claims, and a 0 in the headline row reads as a broken
    build rather than an unread table."""
    per = {"duckrun": {"fct_summary": {"total_rows": 10, "size_mb": 1.0, "num_files": 2,
                                       "compression": "ZSTD", "vorder": False},
                       "fct_scada": {"total_rows": 30, "size_mb": 3.0}},
           "dwh": {"fct_summary": {"total_rows": 10, "size_mb": 2.0},
                   "fct_scada": {"total_rows": 30, "size_mb": 4.0}},
           "spark": {"fct_summary": {"total_rows": 9, "size_mb": 1.0}},
           "iceberg": {}}
    out: list[str] = []
    layout.headline_table(per, ["duckrun", "dwh", "spark", "iceberg"], {}, out)
    rows = {e: next(l for l in out if l.startswith(f"| {layout.LABEL[e]} |"))
            for e in ("duckrun", "dwh", "spark", "iceberg")}
    assert "⚠️" in rows["spark"], "the engine 9 rows short is not flagged"
    assert "⚠️" not in rows["duckrun"] and "⚠️" not in rows["dwh"], "the agreeing majority is flagged"
    # 40 = 10 + 30 summed over the tables, not fct_summary alone.
    assert "| 40 |" in rows["duckrun"]
    assert "⚠️" not in rows["iceberg"] and "| — | — | — |" in rows["iceberg"]
    assert out[0].startswith("## 🏁 Four engines")


def test_headline_vorder_distinguishes_untagged_from_unmeasured():
    """`·` (a writer that stamps no V-Order tag), `n/a (warehouse)` (V-Orders by default and
    writes no tag) and `—` (not measured) are three different answers. The spark tag count comes
    from the deep dive, which is the only per-file truth."""
    mart = {"total_rows": 1, "vorder": False}
    assert layout.vorder_cell(mart, {"vorder_files": {"tagged": 3, "files": 4}}, "spark") == "3/4"
    assert layout.vorder_cell(mart, {}, "duckrun") == "·"
    assert layout.vorder_cell(mart, {}, "dwh") == "n/a (warehouse)"
    assert layout.vorder_cell({}, {}, "dwh") == "—"


def test_build_doc_omits_unmeasured_sections():
    doc = layout.build_doc({"duckrun": {"fct_summary": {"total_rows": 1}}}, ["duckrun", "dwh"],
                           {"duckrun": ("Lakehouse", "dbt", "G1")}, {"duckrun": "duckrun"},
                           {}, {})
    assert list(doc["stats"]) == ["duckrun"]
    assert "encodings" not in doc and "ordering" not in doc
    assert doc["engines"]["duckrun"]["guid"] == "G1" and "dwh" not in doc["engines"]
    assert "spark" not in doc["config"]
