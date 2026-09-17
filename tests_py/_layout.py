"""Where each engine's files live, for the tests that walk the trees.

The repo holds TWO dbt projects, split by dbt major version rather than by anything about
the data: dbt1/ is dbt-core 1.x (duckrun, ducklake, dwh, spark) and dbt2/ is dbt OSS 2
(iceberg). They cannot share a project root because catalogs.yml sits next to
dbt_project.yml and dbt 1.x aborts on one -- see dbt2/dbt_project.yml.

Everything comparing engines to each other has to cross that boundary, so the mapping lives
here once instead of being re-derived (or, worse, silently half-applied: a test that globs
the old flat models/aemo/<engine> path now matches NOTHING, and a parametrized test with no
cases PASSES).
"""
from __future__ import annotations

from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

# engine -> project directory. The key is the dbt target name, the models/aemo/<engine>/
# folder name and the schema prefix, all at once. Keep in step with
# .github/scripts/check_gating.py's ENGINES.
PROJECT_OF = {
    "duckrun": "dbt1",
    "ducklake": "dbt1",
    "dwh": "dbt1",
    "spark": "dbt1",
    "iceberg": "dbt2",
}

ENGINES = list(PROJECT_OF)

# The AEMO column layout and everything else both projects read. Shared on purpose: it is
# the one thing that must not drift between the two.
SHARED_MACROS = REPO / "macros"


def project_dir(engine: str) -> Path:
    return REPO / PROJECT_OF[engine]


def models_dir(engine: str) -> Path:
    return project_dir(engine) / "models" / "aemo" / engine


def singular_tests_dir(engine: str) -> Path:
    return project_dir(engine) / "tests" / "aemo" / engine


def patch_dir(engine: str) -> Path:
    """The _staging/_dimensions/_marts.yml folder for this engine's project."""
    return project_dir(engine) / "models" / "aemo"
