"""Parse every singular test's SQL in its OWN dialect.

The singular tests are the same assertion written five ways, and a naive port across
dialects is the classic way to end up with a test that parses on one engine and is
nonsense on another (T-SQL has no GROUP BY ALL and no LIMIT; Spark and DuckDB have no TOP).
dbt parse only checks the Jinja, never the SQL, and there is no way to run the dwh or spark
SQL without a Fabric capacity -- so sqlglot is the only offline check these get.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
import sqlglot

REPO = Path(__file__).resolve().parents[1]

DIALECT = {
    "duckrun": "duckdb",
    "iceberg": "duckdb",
    "ducklake": "duckdb",
    "dwh": "tsql",
    "spark": "spark",
}


def strip_jinja(sql: str) -> str:
    """Replace dbt Jinja with something parseable, leaving the SQL shape intact."""
    sql = re.sub(r"\{\{\s*config\([^}]*?\)\s*\}\}", "", sql, flags=re.S)
    # {{ ref('x') }} -> a plain identifier
    sql = re.sub(r"\{\{\s*ref\('([^']+)'\)\s*\}\}", r"\1", sql)
    sql = re.sub(r"\{\{\s*this\s*\}\}", "this_table", sql)
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S)
    sql = re.sub(r"\{%.*?%\}", "", sql, flags=re.S)
    sql = re.sub(r"\{\{.*?\}\}", "1", sql, flags=re.S)
    return sql.strip()


def cases():
    for engine, dialect in DIALECT.items():
        for p in sorted((REPO / "tests" / "aemo" / engine).glob("*.sql")):
            yield pytest.param(p, dialect, id=f"{engine}/{p.stem}")


@pytest.mark.parametrize("path,dialect", list(cases()))
def test_singular_test_parses_in_its_dialect(path, dialect):
    sql = strip_jinja(path.read_text(encoding="utf-8"))
    assert sql, f"{path} rendered empty"
    try:
        sqlglot.parse_one(sql, dialect=dialect)
    except Exception as e:  # noqa: BLE001 - sqlglot raises several types
        pytest.fail(f"{path.relative_to(REPO)} is not valid {dialect}:\n{e}\n\n{sql}")


@pytest.mark.parametrize("path,dialect", list(cases()))
def test_no_wrong_dialect_idioms(path, dialect):
    """sqlglot is lenient about some cross-dialect idioms, so check the known traps by hand."""
    # Comments are prose and mention "date"/"time" freely; only look at executable SQL.
    sql = re.sub(r"--[^\n]*", "", path.read_text(encoding="utf-8"))
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S)
    if dialect == "tsql":
        assert "GROUP BY ALL" not in sql, "T-SQL has no GROUP BY ALL"
        assert not re.search(r"\bLIMIT\s+\d", sql), "T-SQL uses SELECT TOP n, not LIMIT"
    else:
        assert not re.search(r"\bSELECT\s+TOP\s+\d", sql, re.I), f"{dialect} has no SELECT TOP"
    if dialect == "spark":
        # `date` and `time` are reserved-ish in Spark SQL and must be backticked.
        for col in ("date", "time"):
            bare = re.search(r"(?<![`\w.])" + col + r"(?![`\w])", sql)
            assert not bare, f"bare `{col}` in Spark SQL must be backticked: {bare.group(0)!r}"


@pytest.mark.parametrize(
    "path",
    [pytest.param(p, id=f"dwh/{p.stem}") for p in sorted((REPO / "tests" / "aemo" / "dwh").glob("*.sql"))],
)
def test_tsql_singular_tests_have_no_top_level_with(path):
    """dbt-fabric nests a singular test's SQL inside a CTE of its own, and T-SQL does not
    allow a WITH clause in a CTE body. A test written with a leading `WITH` therefore fails
    at run time with "Invalid object name '<first cte>'" -- which reads like a missing
    table, not a syntax problem, and cost a full dwh leg to diagnose.

    Use nested derived tables instead. Note this applies to TESTS only: a plain table or
    incremental model with a leading WITH is fine, which is why dim_calendar.sql and
    fct_summary.sql still have one.
    """
    sql = re.sub(r"--[^\n]*", "", path.read_text(encoding="utf-8"))
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S).strip()
    assert not re.match(r"(?i)with\b", sql), (
        f"{path.relative_to(REPO)} starts with a top-level WITH. dbt-fabric wraps the test "
        f"in a CTE and T-SQL cannot nest one -- rewrite as nested derived tables."
    )
