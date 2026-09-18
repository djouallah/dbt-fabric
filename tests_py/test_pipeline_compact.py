"""The leg used to be a reusable workflow (build.yml); it is now a matrix job inside
pipeline.yml. Three things that were structurally true there have to be asserted here, because
each one fails SILENTLY and each one fails GREEN.

  * The iceberg compact job takes the lakehouse GUID from the `land` job's output. It cannot come
    from `build`: `build` is a matrix, and matrix entries overwrite one another's outputs, so the
    value is whichever leg finished last -- or empty. compact_iceberg.py is guarded on an empty
    GUID and exits 0 with a warning, so a broken hand-off means iceberg is never compacted and
    every run stays green.
  * `layout` must wait for `compact`: compaction rewrites exactly the files layout.py measures,
    and a read mid-rewrite is a plausible-looking number rather than an error. The reusable
    workflow gave this for free (a called workflow's job is done when ALL its jobs are done).
  * The leg must NOT land. download_aemo.py rewrites csv_raw_archive_log.parquet with a
    delete-then-copy, so five legs landing at once race on that one file -- which is why the
    shared `land` job exists at all.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import yaml

from _layout import REPO

PIPELINE = REPO / ".github" / "workflows" / "pipeline.yml"


def jobs():
    return yaml.safe_load(PIPELINE.read_text(encoding="utf-8"))["jobs"]


def test_compact_takes_the_lakehouse_from_the_land_job():
    j = jobs()
    assert "data_lakehouse_id" in (j["land"].get("outputs") or {}), (
        "the land job no longer publishes data_lakehouse_id; the compact job then runs with an "
        "empty DATA_LAKEHOUSE_ID, warns, and exits 0 having compacted nothing"
    )
    got = (j["compact"].get("env") or {}).get("DATA_LAKEHOUSE_ID", "")
    assert "needs.land.outputs.data_lakehouse_id" in got, f"compact reads {got!r}"
    assert "needs.build.outputs" not in got, (
        "build is a MATRIX job -- its entries overwrite one another's outputs, so this would be "
        "whichever leg happened to finish last, or empty"
    )
    assert "land" in j["compact"]["needs"]


def test_layout_waits_for_compaction():
    j = jobs()
    assert "compact" in j["layout"]["needs"], (
        "layout.py would measure files and row groups while iceberg_rewrite_data_files is "
        "rewriting them"
    )
    # A run that builds no iceberg leg SKIPS compact, and a skipped `needs` skips everything
    # behind it unless the condition says otherwise.
    assert "!cancelled()" in str(j["layout"]["if"])


def test_engine_labels_agree_across_the_three_places():
    """COSMETIC, and pinned anyway: the five display labels exist in three copies -- this file's
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
        "a build leg runs download_aemo.py again -- five of them race on "
        "csv_raw_archive_log.parquet, and the engines would be compared on different inputs"
    )
