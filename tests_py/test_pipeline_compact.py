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


def test_local_runner_takes_duckrun_and_iceberg_off_fabric_but_not_ducklake():
    """`local_runner` is how the iceberg leg's credential vending gets proven on an `az`-minted
    token, and getting it wrong is SILENT: the leg builds in Fabric anyway, the run is green, and
    the question you dispatched it to answer is still unanswered.

    ducklake must stay remote whatever the toggle says -- only run_in_fabric.py carries the 40613
    loop that waits out its auto-paused catalog SQL DB, and the runner path has nothing of the
    sort. It is also why no DBT_ENV_SECRET_SQL_TOKEN has to be minted on the runner.
    """
    remote = jobs()["build"]["env"]["REMOTE"]
    assert "inputs.local_runner" in remote, (
        "the REMOTE expression ignores local_runner, so the toggle does nothing and the DuckDB "
        "legs still build inside Fabric"
    )
    assert "matrix.engine == 'ducklake'" in remote, (
        "ducklake is no longer pinned to Fabric in the REMOTE expression; a local ducklake leg "
        "dies at the DuckLake ATTACH on a paused catalog with no run_results.json to retry from"
    )


def test_fabric_cores_stays_a_number():
    """Why local_runner is its own input and not a `local` value on fabric_cores: remote_dbt.py
    reads FABRIC_CORES as int(), and the ducklake leg still launches a notebook with it on a
    local run. A non-numeric value there is a ValueError before anything starts."""
    cores = jobs()["build"]["env"]["FABRIC_CORES"]
    assert "local" not in cores, (
        f"build's FABRIC_CORES is {cores!r} -- remote_dbt.py does int() on it and ducklake still "
        f"uses it when local_runner is on"
    )
    opts = yaml.safe_load(PIPELINE.read_text(encoding="utf-8"))
    trig = opts.get("on") or opts[True]
    values = trig["workflow_dispatch"]["inputs"]["fabric_cores"]["options"]
    assert all(v.isdigit() for v in values), f"fabric_cores has a non-numeric option: {values}"


def test_a_local_iceberg_leg_mints_its_own_onelake_token():
    """Inside Fabric remote_dbt.py's setup hook mints ONELAKE_TOKEN from notebookutils, which is
    the whole reason a Fabric run cannot prove vending on a runner token. On the runner nothing
    mints it and the profile's env_var('ONELAKE_TOKEN') has NO default, so dbt dies at profile
    render -- loudly, but only once you have paid for the job."""
    steps = jobs()["build"]["steps"]
    mint = [s for s in steps if "ONELAKE_TOKEN=" in str(s.get("run", ""))]
    assert mint, "no build step mints ONELAKE_TOKEN; a local iceberg leg cannot render its profile"
    cond = str(mint[0].get("if", ""))
    assert "env.REMOTE != 'true'" in cond and "iceberg" in cond, (
        f"the ONELAKE_TOKEN step's condition is {cond!r}; it must fire only for a LOCAL iceberg "
        f"leg -- in Fabric the token must come from notebookutils and never travel"
    )
    assert "add-mask" in str(mint[0]["run"]), "the minted token is not masked in the log"
