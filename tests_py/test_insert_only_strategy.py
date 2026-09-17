"""Render dbt2's `insert_only` incremental strategy and check the SQL it produces.

This macro is the whole reason the iceberg leg can write to the OneLake catalog at all: it
emits MERGE ... WHEN MATCHED THEN DO NOTHING / WHEN NOT MATCHED THEN INSERT BY NAME, which is
what the other four engines get from
merge_clauses={'when_matched': [{'action': 'do_nothing'}]} -- a key dbt 2's config schema does
not accept. See dbt2/macros/incremental_insert_only.sql.

WHY IT IS TESTED HERE RATHER THAN IN CI. Getting it wrong costs a Fabric notebook, not a
parse: `dbt parse` is perfectly happy with SQL that is malformed, so gating goes green and the
failure lands mid-build against the live catalog. It already did once --

    Parser Error: syntax error at or near "NOTHINGWHEN"

-- because `{#--` is Jinja's whitespace-TRIMMING comment form (`{#-` plus a dash) and `--#}`
trims on the way out, so a comment sitting between two SQL lines ate the newlines on both
sides and welded DO NOTHING onto WHEN. The repo hits this trap often enough that CLAUDE.md
lists it first under "Jinja traps"; this test is the only thing that catches it for free.

Rendering needs no dbt: the macro uses plain Jinja plus `{% do %}`, and the one dbt builtin it
touches (exceptions.raise_compiler_error) is only reached on the error path, which is stubbed.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import re

import pytest
from jinja2 import Environment

from _layout import REPO

MACRO = REPO / "dbt2" / "macros" / "incremental_insert_only.sql"


class _Exceptions:
    """Just enough of dbt's `exceptions` for the guard clause."""

    @staticmethod
    def raise_compiler_error(msg):
        raise RuntimeError(msg)


def render(unique_key, predicates=None):
    env = Environment(extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols"])
    tmpl = env.from_string(
        MACRO.read_text(encoding="utf-8")
        + "\n{{ get_incremental_insert_only_sql(arg_dict) }}"
    )
    return tmpl.render(
        exceptions=_Exceptions(),
        arg_dict={
            "target_relation": "onelake.iceberg_mart.fct_summary",
            "temp_relation": "fct_summary__dbt_tmp",
            "unique_key": unique_key,
            "dest_columns": [],
            "incremental_predicates": predicates,
        },
    )


def _squashed(sql: str) -> str:
    return re.sub(r"\s+", " ", sql).strip().upper()


def test_keywords_are_not_welded_together():
    """The regression that reached the live catalog: DO NOTHING glued onto WHEN NOT MATCHED."""
    sql = _squashed(render(["date", "time", "DUID"]))
    assert "NOTHINGWHEN" not in sql, f"whitespace was trimmed away between clauses:\n{sql}"
    for keywords in ("MERGE INTO", "USING", "WHEN MATCHED THEN DO NOTHING",
                     "WHEN NOT MATCHED THEN INSERT BY NAME"):
        assert keywords in sql, f"missing {keywords!r} in:\n{sql}"


def test_every_sql_token_is_separated():
    """Catch the same class of bug anywhere else in the statement, not just the one spot.

    Any two SQL keywords running together produce a token that is not a real word. Rather than
    enumerate the pairs, assert the rendered statement contains no suspiciously long all-caps
    run. Every keyword in this statement is at most ten letters; NOTHINGWHEN, the token that
    actually shipped, is eleven -- hence the threshold.
    """
    sql = _squashed(render(["date", "DUID"]))
    for token in re.findall(r"\b[A-Z]{11,}\b", sql):
        pytest.fail(f"{token!r} looks like two welded keywords in:\n{sql}")


def test_join_uses_every_key_qualified_on_both_sides():
    sql = render(["date", "time", "DUID"])
    for key in ("date", "time", "DUID"):
        assert f"DBT_INTERNAL_SOURCE.{key} = DBT_INTERNAL_DEST.{key}" in sql, (
            f"{key} is missing from the MERGE join condition -- a key that drops out of the "
            f"ON clause makes rows unmatched, and an insert-only merge then DUPLICATES them"
        )
    assert _squashed(sql).count(" AND ") >= 2


def test_a_single_string_key_still_works():
    sql = render("date")
    assert "DBT_INTERNAL_SOURCE.date = DBT_INTERNAL_DEST.date" in sql
    # A string must not be iterated character by character.
    assert "DBT_INTERNAL_SOURCE.d " not in sql


def test_incremental_predicates_are_anded_in():
    sql = _squashed(render(["DUID"], predicates=["DBT_INTERNAL_DEST.month_key >= 202601"]))
    assert "MONTH_KEY >= 202601" in sql


def test_no_unique_key_is_a_loud_error():
    """Without a join condition every row is unmatched and every run re-inserts the batch."""
    with pytest.raises(RuntimeError, match="unique_key"):
        render(None)


def test_no_matched_update_is_emitted():
    """A matched UPDATE is what the OneLake catalog rejects (BadRequest 400).

    DuckDB writes positional delete files for it, and the catalog refuses a commit carrying
    both data and delete files. The whole strategy exists to keep every commit a single
    append snapshot.
    """
    sql = _squashed(render(["DUID"]))
    assert "UPDATE" not in sql, f"an UPDATE branch would make the catalog reject the commit:\n{sql}"
    assert "DELETE" not in sql
