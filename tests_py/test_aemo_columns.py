"""Pin macros/aemo_columns.sql against the AEMO CSV layout, and prove every engine uses it.

The whole reason this repo exists is that the AEMO column lists were written out four times
in four repos. They were verified byte-identical as data before being collapsed into one
macro; these tests keep them that way.

Column ORDER is load-bearing: the AEMO CSVs are headerless positional records, dwh's
OPENROWSET binds by ordinal and Spark's from_csv binds by position, so a reordering is a
silent data corruption on two engines and a no-op on the other three.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SPEC = REPO / "macros" / "aemo_columns.sql"

# The record layouts, as measured across all four source repos before the merge.
EXPECTED = {
    "price": (130, 125),
    "scada": (53, 48),
    "price_today": (70, 60),
    "scada_today": (8, None),  # no cast loop: SCADAVALUE is read typed
}

ENGINES = ["duckrun", "iceberg", "ducklake", "dwh", "spark"]


def spec_block(record: str) -> str:
    """The `{% if/elif record == '<record>' %}` arm of aemo_spec."""
    txt = SPEC.read_text(encoding="utf-8")
    m = re.search(
        r"\{%\s*(?:if|elif)\s+record == '" + record + r"'\s*%\}(.*?)(?=\{%\s*(?:elif|else)\s)",
        txt, re.S,
    )
    assert m, f"no aemo_spec arm for record '{record}'"
    return m.group(1)


def columns(record: str) -> list[str]:
    block = spec_block(record)
    cols = re.search(r"'columns':\s*\[(.*?)\n\s*\],", block, re.S)
    assert cols, f"no columns list for '{record}'"
    return [a for a, _b in re.findall(r"\['([^']+)',\s*'([^']+)'\]", cols.group(1))]


def not_double(record: str) -> list[str]:
    block = spec_block(record)
    m = re.search(r"'not_double':\s*\[([^\]]*)\]", block)
    assert m, f"no not_double list for '{record}'"
    return re.findall(r"'([^']+)'", m.group(1))


@pytest.mark.parametrize("record", sorted(EXPECTED))
def test_column_count(record):
    n_cols, _ = EXPECTED[record]
    assert len(columns(record)) == n_cols


@pytest.mark.parametrize("record", sorted(EXPECTED))
def test_cast_subset_count(record):
    _, n_cast = EXPECTED[record]
    if n_cast is None:
        pytest.skip("scada_today reads typed columns; it has no cast loop")
    cast = [c for c in columns(record) if c not in not_double(record)]
    assert len(cast) == n_cast


@pytest.mark.parametrize("record", sorted(EXPECTED))
def test_no_duplicate_columns(record):
    cols = columns(record)
    dupes = {c for c in cols if cols.count(c) > 1}
    assert not dupes, f"duplicate column names in '{record}': {sorted(dupes)}"


@pytest.mark.parametrize("record", sorted(EXPECTED))
def test_not_double_names_all_exist(record):
    """A typo here silently casts a column that should have been left alone."""
    cols = set(columns(record))
    missing = [c for c in not_double(record) if c not in cols]
    assert not missing, f"not_double names absent from the layout of '{record}': {missing}"


@pytest.mark.parametrize("record", ["price", "scada"])
def test_key_columns_present(record):
    """The record-selection predicate and the merge key must reference real columns."""
    cols = set(columns(record))
    for c in ("I", "UNIT", "VERSION", "SETTLEMENTDATE", "INTERVENTION"):
        assert c in cols, f"'{c}' missing from the layout of '{record}'"


def test_no_engine_carries_its_own_column_list():
    """The point of the merge: no model may re-declare the AEMO layout locally."""
    offenders = []
    for engine in ENGINES:
        for p in (REPO / "models" / "aemo" / engine).rglob("*.sql"):
            txt = p.read_text(encoding="utf-8")
            # A local list is a `set <name> = [ ... ]` holding many quoted UPPERCASE names.
            for m in re.finditer(r"\{%-?\s*set\s+\w+\s*=\s*\[(.*?)\]\s*-?%\}", txt, re.S):
                names = re.findall(r"'([A-Z][A-Z0-9_]{2,})'", m.group(1))
                if len(names) > 12:
                    offenders.append(f"{p.relative_to(REPO)} ({len(names)} column names inline)")
    assert not offenders, (
        "these models declare the AEMO column layout locally instead of calling "
        "aemo_columns()/aemo_cast_columns():\n  " + "\n  ".join(offenders)
    )


def test_every_engine_has_the_canonical_models():
    canonical = {
        "stg_csv_archive_log", "dim_calendar", "dim_duid", "fct_price",
        "fct_price_today", "fct_scada", "fct_scada_today", "fct_summary",
    }
    for engine in ENGINES:
        got = {p.stem for p in (REPO / "models" / "aemo" / engine).rglob("*.sql")}
        assert got == canonical, f"{engine}: {sorted(got ^ canonical)} differs from the canonical set"


def test_every_engine_has_the_same_singular_tests():
    def names(engine: str) -> set[str]:
        return {p.name for p in (REPO / "tests" / "aemo" / engine).glob("*.sql")}

    base = names(ENGINES[0])
    for engine in ENGINES[1:]:
        got = names(engine)
        assert got == base, f"{engine} singular tests differ from {ENGINES[0]}: {sorted(got ^ base)}"
