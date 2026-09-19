"""Pin that every engine folds the same files, newest first.

`process_limit` caps how many unprocessed archive-log files a fact model ingests per run. The
CAP is fine on its own; the ORDER is what has to be identical across the five engines, and
getting it wrong is the quietest failure in this repo:

  * With no `ORDER BY` at all -- which is what the three DuckDB legs' pre-hooks carried until
    2026-09-19 -- the engine folds ANY n of the backlog, and a different n per engine. dwh and
    spark ordered theirs. So on any run whose process_limit was smaller than the backlog the
    five engines built their gold tables from DIFFERENT INPUTS, and parity.py reported that as
    a logic difference: the one measurement this repo exists to make, grading five engines on
    five different samples.
  * The direction matters on its own. Newest first means a partial load is RECENT data and the
    backlog is old data still queued, which is what lets the assert_all_*_files_processed_*
    tests be warnings rather than failures.

`archive_path` is '/<subfolder>/PUBLIC_*_YYYYMMDD*.CSV', so lexicographic DESC is
chronological newest-first.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import re

import pytest

from _layout import REPO, singular_tests_dir

# Where each engine decides which files to fold. The DuckDB family does it in a model pre-hook
# (one per fact model); dwh and spark share one macro each.
DUCKDB_MODELS = [
    REPO / "dbt1" / "models" / "aemo" / engine / "marts" / f"{model}.sql"
    for engine in ("duckrun", "iceberg", "ducklake")
    for model in ("fct_price", "fct_price_today", "fct_scada", "fct_scada_today")
]
MACROS = [
    REPO / "dbt1" / "macros" / "new_source_files.sql",    # dwh
    REPO / "dbt1" / "macros" / "spark_new_files.sql",     # spark
]


@pytest.mark.parametrize("path", DUCKDB_MODELS, ids=lambda p: f"{p.parts[-3]}/{p.name}")
def test_a_duckdb_pre_hook_orders_before_it_limits(path):
    """A bare `LIMIT process_limit` inside the pre-hook returns an arbitrary subset, and DuckDB
    is free to return a different one on the next run or on the next engine."""
    src = path.read_text(encoding="utf-8")
    assert "LIMIT {{ env_var('process_limit'" in src, (
        f"{path.name} no longer caps its file list with process_limit"
    )
    assert "ORDER BY archive_path DESC LIMIT {{ env_var('process_limit'" in src, (
        f"{path.name}'s pre-hook LIMITs without an ORDER BY archive_path DESC immediately "
        f"before it -- the files it folds are then whichever ones DuckDB happened to emit"
    )


@pytest.mark.parametrize("path", MACROS, ids=lambda p: p.name)
def test_a_macro_selection_is_ordered_newest_first(path):
    """dwh's TOP and spark's LIMIT both need the ORDER BY to mean anything, and both must point
    the same way as the DuckDB pre-hooks."""
    src = path.read_text(encoding="utf-8")
    orders = re.findall(r"ORDER BY (?:l\.)?archive_path(?: (\w+))?", src)
    assert orders, f"{path.name} selects files with no ORDER BY archive_path"
    assert all(d == "DESC" for d in orders), (
        f"{path.name} orders {orders} -- every engine must fold NEWEST first, or the engines "
        f"fold different subsets of the same backlog and parity.py calls it a logic difference"
    )


def test_the_downloader_lands_newest_first():
    """Same reason one layer up: a download_limit smaller than the backlog should land the
    recent end of the archive. The intraday candidate tables were already newest-first; the
    daily one is the nemweb listing with the GitHub-mirror backfill appended, unordered."""
    src = (REPO / "download_aemo.py").read_text(encoding="utf-8")
    m = re.search(r"def new_files\(table, source_type\):(.*?)\.fetchall\(\)", src, re.S)
    assert m, "new_files() is not where this test expects it"
    assert "ORDER BY filename DESC" in m.group(1), (
        "new_files() LIMITs an unordered candidate table, so which files land is incidental"
    )


@pytest.mark.parametrize("engine", ["duckrun", "iceberg", "ducklake", "dwh", "spark"])
def test_a_backlog_is_a_warning_on_every_engine(engine):
    """These five go red for files that have merely not been folded YET. That is not a defect
    -- the data is correct, just incomplete, and it converges run by run -- and a red build
    that is expected to be red teaches everyone to ignore the build. Relaxed per FILE, not in
    dbt_project.yml's data_tests section: that key is per engine and would also relax the grain
    and join assertions, which are real correctness checks."""
    backlog = [
        "assert_all_daily_files_processed_price",
        "assert_all_daily_files_processed_scada",
        "assert_all_today_files_processed_price",
        "assert_all_today_files_processed_scada",
        "assert_summary_covers_all_scada_days",
    ]
    for name in backlog:
        path = singular_tests_dir(engine) / f"{name}.sql"
        assert path.exists(), f"{engine} has no {name}.sql"
        assert "config(severity='warn')" in path.read_text(encoding="utf-8"), (
            f"{engine}/{name}.sql fails the build on a backlog instead of warning about it"
        )
