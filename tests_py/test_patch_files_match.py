"""The two projects' model patch files must stay byte-identical.

models/aemo/_staging.yml, _dimensions.yml and _marts.yml declare the 30 generic tests and
every column description. They exist TWICE -- once in dbt1/ and once in dbt2/ -- because a
patch file has to sit inside its own project's model-paths, and the two projects cannot
share a root (catalogs.yml; see dbt2/dbt_project.yml).

That duplication is the only unguarded copy in the repo, and it is exactly the shape of the
problem this repo was built to end: four source repos each holding their own copy of the
truth, drifting one fix at a time. So it is pinned mechanically instead of by discipline.
Edit one, run pytest, copy it across -- do not "fix" a difference by deciding which side is
right in your head.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import pytest

from _layout import patch_dir

PATCH_FILES = ["_staging.yml", "_dimensions.yml", "_marts.yml"]


@pytest.mark.parametrize("name", PATCH_FILES)
def test_patch_file_is_identical_in_both_projects(name):
    # patch_dir takes an ENGINE, so name one from each project. duckrun stands for dbt1 and
    # iceberg for dbt2; which engine is irrelevant, the file is per project.
    a = patch_dir("duckrun") / name
    b = patch_dir("iceberg") / name
    for p in (a, b):
        assert p.is_file(), f"{p} is missing -- both projects need their own copy"

    left = a.read_text(encoding="utf-8")
    right = b.read_text(encoding="utf-8")
    if left == right:
        return

    import difflib

    diff = "\n".join(difflib.unified_diff(
        left.splitlines(), right.splitlines(),
        fromfile=str(a), tofile=str(b), lineterm="",
    ))
    pytest.fail(
        f"{name} differs between the two projects. One patch file documents and tests "
        f"whichever engine tree is live, so a difference here means the engines are being "
        f"held to different assertions -- which is the drift this repo exists to "
        f"prevent.\n\n{diff}"
    )


def test_both_projects_declare_the_same_generic_test_count():
    """A cheap independent check on the same thing, in the units check_gating.py reports.

    If someone reconciles the files by deleting tests from one side rather than copying the
    new ones across, the byte comparison above goes green while GENERIC drops on one engine
    -- and a missing test is invisible in a passing run.
    """
    import yaml

    def generic_tests(engine: str) -> int:
        total = 0
        for name in PATCH_FILES:
            doc = yaml.safe_load((patch_dir(engine) / name).read_text(encoding="utf-8"))
            for model in doc.get("models", []):
                for col in model.get("columns", []):
                    total += len(col.get("tests", []) or [])
        return total

    dbt1, dbt2 = generic_tests("duckrun"), generic_tests("iceberg")
    assert dbt1 == dbt2, f"dbt1 declares {dbt1} generic tests, dbt2 declares {dbt2}"
    # check_gating.py asserts this number against a real dbt parse on every engine.
    assert dbt1 == 30, f"expected 30 generic tests, found {dbt1} -- update check_gating.GENERIC too"
