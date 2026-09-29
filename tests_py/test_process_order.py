"""Pin that every engine folds the same files, newest first.

`process_limit` caps how many unprocessed archive-log files a fact model ingests per run. The
CAP is fine on its own; the ORDER is what has to be identical across the engines, and
getting it wrong is the quietest failure in this repo:

  * With no `ORDER BY` at all the engine folds ANY n of the backlog, and a different n per
    engine. So on any run whose process_limit was smaller than the backlog the engines would
    build their gold tables from DIFFERENT INPUTS, and parity.py would report that as a logic
    difference: the one measurement this repo exists to make, grading engines on different
    samples.
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

# Where each engine decides which files to fold: one macro each.
MACROS = [
    REPO / "dbt1" / "macros" / "new_source_files.sql",    # dwh
    REPO / "dbt1" / "macros" / "spark_new_files.sql",     # spark
]


@pytest.mark.parametrize("path", MACROS, ids=lambda p: p.name)
def test_a_macro_selection_is_ordered_newest_first(path):
    """dwh's TOP and spark's LIMIT both need the ORDER BY to mean anything, and both must point
    the same way."""
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


@pytest.mark.parametrize("engine", ["dwh", "spark"])
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
