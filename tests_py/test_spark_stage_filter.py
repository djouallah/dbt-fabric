"""Pin the spark stage filter's numeric predicate to an explicit CAST.

The spark fact models read the landed CSVs through a temp view whose every column is STRING
(dbt1/macros/spark_read_csv.sql). Spark resolves `STRING != 0` by casting the STRING to the
literal's type, INT, and that cast truncates a decimal fraction -- so `'0.5' != 0` is FALSE.
A bare `SCADAVALUE != 0` in the stage filter therefore dropped every intraday SCADA row with
0 < |value| < 1 (12-16% of the non-zero rows) on spark alone, with every dbt test green.
Parity caught it (run 35188161001: fct_summary 1,547 rows short, all in the intraday tail).

There is no way to run Spark SQL offline, so this pins the rendered text: the nonzero branch
of spark_record_filter must wrap the column in CAST(... AS DOUBLE).

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import re

from _layout import REPO

MACRO = REPO / "dbt1" / "macros" / "spark_read_csv.sql"


def nonzero_branch() -> str:
    txt = MACRO.read_text(encoding="utf-8")
    m = re.search(r"\{%\s*macro spark_record_filter\b(.*?)\{%\s*endmacro\s*%\}", txt, re.S)
    assert m, "spark_record_filter macro not found"
    body = m.group(1)
    branch = re.search(r"\{%-?\s*if spec\['nonzero'\]\s*%\}(.*?)\{%\s*endif", body, re.S)
    assert branch, "spark_record_filter has no nonzero branch"
    return branch.group(1)


def test_nonzero_predicate_casts_to_double():
    branch = nonzero_branch()
    assert "CAST(" in branch and "AS DOUBLE)" in branch, (
        "spark_record_filter compares the nonzero column as a STRING. Spark casts the string "
        "to the INT literal's type and truncates, so 0 < |value| < 1 rows are dropped. Wrap it: "
        "CAST(<col> AS DOUBLE) != 0.\n" + branch
    )


def test_nonzero_predicate_is_not_a_bare_string_compare():
    branch = nonzero_branch()
    assert not re.search(r"spec\['nonzero'\]\s*~\s*'\s*!= 0'", branch), (
        "bare `<col> != 0` against the all-STRING csv view -- see the module docstring"
    )