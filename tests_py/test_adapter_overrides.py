"""Pin macros/iceberg_adapter_overrides.sql against the INSTALLED dbt-duckdb.

`iceberg` and `ducklake` are both `type: duckdb`, so a `duckdb__` override applies to BOTH.
The overrides therefore branch on target.name and reproduce dbt-duckdb's own body for the
non-iceberg path. If dbt-duckdb changes that body upstream, ducklake would silently keep
running our stale copy — these tests make that a loud failure instead.

Run: python -m pytest tests_py/ -q
"""
import pathlib
import re

import pytest

OVERRIDES = pathlib.Path(__file__).resolve().parents[1] / "macros" / "iceberg_adapter_overrides.sql"


def _installed_adapters_sql() -> str:
    import dbt.include.duckdb as inc

    return (pathlib.Path(inc.__file__).parent / "macros" / "adapters.sql").read_text(encoding="utf-8")


def _macro_body(text: str, name: str) -> str:
    m = re.search(r"\{%-?\s*macro\s+" + name + r"\b.*?\{%-?\s*endmacro\s*-?%\}", text, re.S)
    assert m, f"macro {name} not found"
    return m.group(0)


def _norm(sql: str) -> list[str]:
    """Comparable shape: drop Jinja comments, blank lines and indentation."""
    sql = re.sub(r"\{#.*?#\}", "", sql, flags=re.S)
    return [ln.strip() for ln in sql.splitlines() if ln.strip()]


@pytest.mark.parametrize("macro", ["duckdb__get_columns_in_relation", "duckdb__drop_relation"])
def test_override_is_guarded_by_target_name(macro):
    """Every override must branch on target.name, or it leaks onto ducklake."""
    body = _macro_body(OVERRIDES.read_text(encoding="utf-8"), macro)
    assert "target.name == 'iceberg'" in body, (
        f"{macro} has no target.name guard — it would apply to the ducklake target too"
    )


def test_get_columns_fallback_matches_upstream():
    """Our body, minus the iceberg-only `where` guard, must equal dbt-duckdb's."""
    ours = _macro_body(OVERRIDES.read_text(encoding="utf-8"), "duckdb__get_columns_in_relation")
    theirs = _macro_body(_installed_adapters_sql(), "duckdb__get_columns_in_relation")
    # Drop the iceberg-only guard block from ours before comparing.
    ours = re.sub(r"\{%-?\s*if target\.name == 'iceberg'.*?\{%-?\s*endif\s*-?%\}", "", ours, flags=re.S)
    assert _norm(ours) == _norm(theirs), (
        "dbt-duckdb's duckdb__get_columns_in_relation has changed upstream.\n"
        "Re-sync the fallback in macros/iceberg_adapter_overrides.sql, or ducklake keeps\n"
        "running the stale copy.\n\nUPSTREAM:\n" + theirs + "\n\nOURS (guard stripped):\n" + ours
    )


def test_drop_relation_fallback_keeps_upstream_ducklake_branch():
    """dbt-duckdb already omits CASCADE for DuckLake; our override must keep that."""
    ours = _macro_body(OVERRIDES.read_text(encoding="utf-8"), "duckdb__drop_relation")
    theirs = _macro_body(_installed_adapters_sql(), "duckdb__drop_relation")
    assert "adapter.is_ducklake(relation)" in theirs, (
        "dbt-duckdb no longer special-cases DuckLake in drop_relation; re-check the override"
    )
    assert "adapter.is_ducklake(relation)" in ours
    assert "cascade" in ours.lower()
