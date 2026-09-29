"""The build leg is a matrix job inside pipeline.yml. Two things about it fail SILENTLY and GREEN:

  * The leg must NOT land. download_aemo.py rewrites csv_raw_archive_log.parquet in place, so
    legs landing at once race on that one file -- which is why the shared `land` job exists.
  * The display labels exist in three copies and must agree.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import yaml

from _layout import REPO

PIPELINE = REPO / ".github" / "workflows" / "pipeline.yml"


def jobs():
    return yaml.safe_load(PIPELINE.read_text(encoding="utf-8"))["jobs"]


def test_engine_labels_agree_across_the_three_places():
    """COSMETIC, and pinned anyway: the display labels exist in three copies -- this file's
    `plan` step, ci.yml's gating matrix and layout.py's LABEL -- and they are read side by side
    (the Actions graph next to the run page's first table). A failure here is a display drift,
    never a broken build; fix it by making the three match, not by deleting the test."""
    import json
    import sys

    sys.path.insert(0, str(REPO / ".github" / "scripts"))
    import layout  # noqa: PLC0415

    run = jobs()["plan"]["steps"][0]["run"]
    plan = {e["engine"]: e["label"] for e in json.loads(run.split("ALL='", 1)[1].split("'", 1)[0])}
    ci = yaml.safe_load((REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8"))
    gating = {e["engine"]: e["label"]
              for e in ci["jobs"]["gating"]["strategy"]["matrix"]["include"]}
    assert plan == gating, "pipeline.yml and ci.yml label the same engine differently"
    assert plan == {e: layout.LABEL[e] for e in plan}, "layout.LABEL has drifted from the workflows"


def test_the_legs_do_not_land():
    steps = jobs()["build"]["steps"]
    assert not any("download_aemo" in str(s.get("run", "")) for s in steps), (
        "a build leg runs download_aemo.py again -- the legs race on "
        "csv_raw_archive_log.parquet, and the engines would be compared on different inputs"
    )
