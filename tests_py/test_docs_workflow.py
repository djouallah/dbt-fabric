"""The docs page is published unattended, and every way it can go wrong goes wrong GREEN.

`dbt docs generate` documents whatever the gate enabled. If the target name stops matching a
models/aemo/<engine>/ folder it enables NOTHING, dbt writes a valid page with an empty DAG and
exits 0 -- the same failure check_gating.py exists for, except that here the evidence is a
published web page rather than a build log. check_catalog_stats.py catches that at run time;
these pin the things that would stop it being called at all, plus the two permissions that
must not spread.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import re

import yaml

from _layout import REPO, models_dir

DOCS = REPO / ".github" / "workflows" / "docs.yml"
PIPELINE = REPO / ".github" / "workflows" / "pipeline.yml"
ENGINE = "duckrun"


def doc():
    return yaml.safe_load(DOCS.read_text(encoding="utf-8"))


def steps(job):
    return doc()["jobs"][job]["steps"]


def test_the_engine_is_a_real_model_tree():
    """The one thing that empties the page silently. The target name is also the folder name
    and the schema prefix; nothing downstream notices if it stops being all three."""
    run = next(s["run"] for s in steps("site") if "docs generate" in str(s.get("run", "")))
    assert f"--target {ENGINE}" in run, f"docs generate no longer targets {ENGINE}: {run!r}"
    assert models_dir(ENGINE).is_dir(), (
        f"docs.yml targets {ENGINE} but {models_dir(ENGINE)} does not exist -- dbt would "
        f"enable no models and publish an empty DAG, green"
    )
    step = next(s for s in steps("site") if "docs generate" in str(s.get("run", "")))
    assert step.get("working-directory") == "dbt1", (
        f"{ENGINE} is a dbt-core 1.x engine and lives in dbt1/; run from the wrong project "
        f"and dbt parses the other one"
    )


def test_the_page_is_the_static_single_file():
    """`--static` is what inlines the manifest and catalog. Without it the page is a shell
    that fetches manifest.json and catalog.json beside itself -- and only static_index.html
    is copied into site/, so the page would load and then show nothing."""
    run = next(s["run"] for s in steps("site") if "docs generate" in str(s.get("run", "")))
    assert "--static" in run
    assemble = " ".join(str(s.get("run", "")) for s in steps("site"))
    assert "static_index.html" in assemble


def test_the_catalog_is_checked_before_it_is_published():
    ordered = [str(s.get("run", "")) + str(s.get("uses", "")) for s in steps("site")]
    check = next(i for i, s in enumerate(ordered) if "check_catalog_stats.py" in s)
    upload = next(i for i, s in enumerate(ordered) if "upload-pages-artifact" in s)
    assert check < upload, (
        "the stats check must run BEFORE the artifact is uploaded, or a stat-less catalog "
        "is already on its way to the page"
    )


def test_the_uploaded_directory_is_the_one_assembled():
    d = doc()
    upload = next(s for s in steps("site") if "upload-pages-artifact" in str(s.get("uses", "")))
    path = upload["with"]["path"]
    assemble = " ".join(str(s.get("run", "")) for s in steps("site"))
    assert f"mkdir -p {path}" in assemble and f"{path}/index.html" in assemble, (
        f"upload-pages-artifact publishes {path!r}, which no step in the job fills"
    )
    assert d["jobs"]["publish"]["needs"] == "site" or "site" in d["jobs"]["publish"]["needs"], (
        "a generate that failed must not leave the previous job's artifact to be deployed"
    )


def test_the_write_permissions_do_not_spread():
    """`pages: write` deploys the site and `contents: write` could commit to the repo. This
    workflow needs the first, on one job, and never the second."""
    d = doc()
    assert d["permissions"] == {"contents": "read"}, d["permissions"]
    site = d["jobs"]["site"]["permissions"]
    assert site.get("contents") == "read" and "pages" not in site, site
    publish = d["jobs"]["publish"]["permissions"]
    assert publish.get("pages") == "write" and publish.get("id-token") == "write", publish
    assert publish.get("contents") != "write"


def test_it_chains_off_the_pipeline_and_checks_out_that_branch():
    """Same trap as capacity.yml's: on a `workflow_run` event the default checkout is the
    TRIGGERING run's SHA and `github.ref_name` is the default branch, so a topic-branch
    pipeline would publish main's models as if they were the ones that ran."""
    d = doc()
    on = d[True] if True in d else d["on"]
    assert set(on) == {"workflow_dispatch", "workflow_run"}, sorted(on)
    assert on["workflow_run"]["workflows"] == ["pipeline"]
    checkout = next(s for s in steps("site") if "actions/checkout" in str(s.get("uses", "")))
    assert "github.event.workflow_run.head_branch" in checkout["with"]["ref"]


def test_it_only_publishes_when_the_pipeline_built_duckrun():
    """The page is duckrun's CATALOG, so a pipeline run that built only iceberg (or dwh, or
    spark) leaves nothing on it to republish. A `workflow_run` trigger cannot be filtered on
    the triggering run's inputs -- the payload does not carry them -- so the gate reads the
    engine selection back off `display_title`, and a manual dispatch bypasses it."""
    gate = doc()["jobs"]["site"]["if"]
    assert "github.event.workflow_run.display_title" in gate, gate
    assert "github.event_name != 'workflow_run'" in gate, (
        "a manual dispatch has no triggering run to read an engine list off, and must still "
        "publish"
    )
    publish = doc()["jobs"]["publish"]
    assert "if" not in publish, (
        "publish must inherit the skip through `needs: site` -- an `if:` of its own would "
        "run it on a skipped generate and deploy the previous run's artifact"
    )


def test_the_gate_matches_what_pipeline_run_name_actually_says():
    """THE COUPLING, and the reason it is worth a test. `display_title` is pipeline.yml's
    `run-name`, which is documented THERE as display only -- so this renders that template
    for each of the six engine selections and checks the gate's verdict on the title it
    produces. Reword the run-name and the gate stops matching: the docs job would skip every
    time, green, and the page would freeze at whatever it last published."""
    run_name = yaml.safe_load(PIPELINE.read_text(encoding="utf-8"))["run-name"]
    gate = doc()["jobs"]["site"]["if"]

    # The one expression in the run-name, and the label it substitutes for `all`. Pulling the
    # label out rather than hard-coding it is what lets the run-name be reworded freely as
    # long as the gate is reworded with it.
    expr = re.search(r"\$\{\{(.+?)\}\}", run_name)
    assert expr, f"pipeline.yml's run-name no longer computes anything: {run_name!r}"
    branch = re.search(
        r"inputs\.engines\s*==\s*'all'\s*&&\s*'([^']+)'\s*\|\|\s*inputs\.engines", expr.group(1)
    )
    assert branch, (
        f"run-name no longer renders `all` as a label and every other selection as the raw "
        f"engine name; the gate cannot read the selection off it: {expr.group(1)!r}"
    )
    head, tail = run_name.split("${{", 1)[0], run_name.split("}}", 1)[1]
    tokens = re.findall(r"display_title,\s*'([^']+)'\)", gate)
    assert tokens, f"the gate matches nothing against display_title: {gate!r}"

    def publishes(engines):
        title = head + (branch.group(1) if engines == "all" else engines) + tail
        return any(t in title for t in tokens)

    assert publishes("all"), (
        f"a five-engine run renders {head + branch.group(1) + tail!r}, which the gate does "
        f"not match -- the page would never be published again"
    )
    assert publishes(ENGINE), f"an engines: {ENGINE} run does not publish the page"
    for other in ("iceberg", "ducklake", "dwh", "spark"):
        assert not publishes(other), (
            f"engines: {other} builds none of duckrun's tables, but the gate matches the "
            f"title it renders -- the page would be republished unchanged"
        )
