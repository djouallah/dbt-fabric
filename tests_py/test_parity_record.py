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

import yaml

from _layout import REPO

GITIGNORE = REPO / ".gitignore"
WORKFLOWS = REPO / ".github" / "workflows"
PIPELINE = WORKFLOWS / "pipeline.yml"
CAPACITY = WORKFLOWS / "capacity.yml"


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


def _triggers(path):
    """The `on:` block. PyYAML parses the bare key `on` as the boolean True."""
    doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    on = doc.get(True) if True in doc else doc.get("on")
    if isinstance(on, str):
        return {on}
    if isinstance(on, list):
        return set(on)
    return set(on or {})


def test_only_ci_runs_on_push():
    """pipeline.yml and capacity.yml COMMIT to history/. A push made with GITHUB_TOKEN triggers
    nothing, which is the only reason two committers are safe today; a `push:` trigger on
    either would let a PAT-authenticated commit start a paid build or loop the ledger."""
    for wf in sorted(WORKFLOWS.glob("*.yml")):
        if wf.name == "ci.yml":
            continue
        assert "push" not in _triggers(wf), f"{wf.name} must not run on push"


def test_capacity_checks_out_the_triggering_branch():
    """On a `workflow_run` event the default checkout is the TRIGGERING run's SHA, from before
    the record job pushed -- the read would then miss the very run that triggered it."""
    text = CAPACITY.read_text(encoding="utf-8")
    assert "workflow_run" in _triggers(CAPACITY)
    assert "ref: ${{ github.event.workflow_run.head_branch" in text
