"""Where each engine's files live, for the tests that walk the trees.

ONE dbt project, dbt1/, holding all five engines. It was briefly two -- dbt2/ carried the
iceberg engine on dbt OSS 2 between 2026-09-17 and 2026-09-18 -- and the mapping below is
what is left of that: a single dict rather than a path recomputed in every test, because a
test that globs a path no longer there matches NOTHING, and a parametrized test with no
cases PASSES.
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

# engine -> project directory. The key is the dbt target name, the models/aemo/<engine>/
# folder name and the schema prefix, all at once. Keep in step with
# .github/scripts/check_gating.py's ENGINES.
PROJECT_OF = {
    "duckrun": "dbt1",
    "iceberg": "dbt1",
    "ducklake": "dbt1",
    "dwh": "dbt1",
    "spark": "dbt1",
}

ENGINES = list(PROJECT_OF)

# The AEMO column layout and everything else the project reads from the REPO ROOT rather
# than from dbt1/macros. Kept separate because it is shared data, not engine logic.
SHARED_MACROS = REPO / "macros"


def project_dir(engine: str) -> Path:
    return REPO / PROJECT_OF[engine]


def models_dir(engine: str) -> Path:
    return project_dir(engine) / "models" / "aemo" / engine


def singular_tests_dir(engine: str) -> Path:
    return project_dir(engine) / "tests" / "aemo" / engine


def patch_dir(engine: str) -> Path:
    """The _staging/_dimensions/_marts.yml folder. ONE per project, above the engine trees --
    so every engine resolves to the same directory, and the argument is only there because
    callers name an engine rather than a project."""
    return project_dir(engine) / "models" / "aemo"
