"""The parity job must actually be able to commit the fingerprints it produces.

history/parity/*.json is gitignored on purpose: a laptop run writing a fingerprint would
poison the cross-engine comparison, and only CI is allowed to record one. But `git add <dir>`
SKIPS ignored files without saying so -- it stages the directory's README and nothing else,
`git diff --cached` then reports nothing to commit, and the workflow goes green having
recorded NOTHING. history/parity/ held no fingerprints at all because of this.

The two settings are only correct together, and neither file hints at the other, so they are
pinned here.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import re

from _layout import REPO

GITIGNORE = REPO / ".gitignore"
PIPELINE = REPO / ".github" / "workflows" / "pipeline.yml"


def test_fingerprints_are_gitignored():
    """Guards the other half: without this, a local run's numbers reach the comparison."""
    assert re.search(r"^history/parity/\*\.json\s*$", GITIGNORE.read_text(encoding="utf-8"),
                     re.M), (
        "history/parity/*.json is no longer gitignored. If that is deliberate, drop this test "
        "and the -f in pipeline.yml together -- but a laptop run can now commit a fingerprint "
        "and the engines would be compared against numbers CI never produced."
    )


def test_the_record_step_forces_the_add():
    text = PIPELINE.read_text(encoding="utf-8")
    assert "git add -f history/parity" in text, (
        "pipeline.yml's Record step must use `git add -f history/parity`. A plain `git add` on "
        "that directory silently stages nothing, because the fingerprints are gitignored -- the "
        "commit is then empty and the run records no measurement while still going green."
    )
    assert not re.search(r"git add (?!-f )\S*history/parity", text), (
        "a non-forcing `git add` of history/parity is still present somewhere in pipeline.yml"
    )
