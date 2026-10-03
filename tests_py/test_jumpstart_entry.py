"""Pin jumpstart/, the Fabric Jumpstart catalog entry, against the install and the catalog.

The entry is a second copy of what install_jumpstart.INSTALL says: a folder, an entry point
and the item types to install. Nothing checks the copy until the catalog's CI does, on a PR
upstream, or a user's install does, in their workspace. The catalog's schema rules are copied
here as constants (src/fabric_jumpstart/tests/schemas.py and fabric_jumpstart/constants.py in
microsoft/fabric-jumpstart) rather than installed: tests_py needs no package beyond its own.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import re
import sys
from datetime import datetime

import yaml

from _layout import REPO

JUMPSTART = REPO / "jumpstart"

sys.path.insert(0, str(REPO / ".github" / "scripts"))
import install_jumpstart  # noqa: E402

INSTALL = install_jumpstart.INSTALL
ENTRY = JUMPSTART / f"{INSTALL['logical_id']}.yml"
CONTENT = JUMPSTART / "content" / INSTALL["logical_id"]

# The catalog's schema (extra="forbid": a key outside this set fails its CI).
FIELDS = {"id", "logical_id", "name", "description", "date_added", "include_in_listing",
          "workload_tags", "scenario_tags", "type", "core", "source", "items_in_scope",
          "feature_flags", "jumpstart_docs_uri", "entry_point", "test_suite", "owner_email",
          "minutes_to_complete_jumpstart", "minutes_to_deploy", "video_url", "difficulty",
          "last_updated", "mermaid_diagram"}
SOURCE_FIELDS = {"workspace_path", "repo_url", "repo_ref", "files_source_path",
                 "files_destination_lakehouse", "files_destination_path"}
WORKLOAD_TAGS = {"Data Engineering", "Data Warehouse", "Data Science", "Real-Time Intelligence",
                 "Data Factory", "SQL Database", "Power BI", "Ontology"}
SCENARIO_TAGS = {"Streaming", "Modeling", "Monitoring", "Data Integration", "Batch Processing"}
TYPES = {"Accelerator", "Tutorial", "Demo"}
DIFFICULTIES = {"Beginner", "Intermediate", "Advanced"}
# The docs say 250; their code allows 350. Held to the stricter one.
DESCRIPTION_MAX = 250


def _entry() -> dict:
    return yaml.safe_load(ENTRY.read_text(encoding="utf-8"))


def test_the_entry_installs_what_the_install_installs():
    e = _entry()
    assert e["logical_id"] == INSTALL["logical_id"] == ENTRY.stem
    assert e["source"]["workspace_path"] == INSTALL["workspace_path"]
    assert e["source"]["repo_url"].removesuffix(".git") == INSTALL["repo_url"]
    assert e["entry_point"] == INSTALL["entry_point"]
    assert e["items_in_scope"] == INSTALL["items_in_scope"]


def test_the_entry_passes_the_catalog_schema():
    e = _entry()
    assert set(e) <= FIELDS - {"core"}, "core is set by the folder the entry is in, not by it"
    assert set(e["source"]) <= SOURCE_FIELDS
    assert isinstance(e["id"], int) and e["id"] > 0
    assert re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", e["logical_id"])
    assert len(e["description"]) <= DESCRIPTION_MAX, len(e["description"])
    assert not e["description"].lower().startswith(e["name"].lower())
    datetime.strptime(e["date_added"], "%m/%d/%Y")
    assert e["workload_tags"] and set(e["workload_tags"]) <= WORKLOAD_TAGS
    assert e["scenario_tags"] and set(e["scenario_tags"]) <= SCENARIO_TAGS
    assert e["type"] in TYPES
    assert e["difficulty"] in DIFFICULTIES
    assert re.fullmatch(r"[^@\s]+@[^@\s.]+\.[^@\s]+", e["owner_email"])
    assert e["mermaid_diagram"].lstrip().startswith("graph LR")


def test_the_entry_installs_a_tag():
    """The catalog installs at repo_ref and requires a tag. A branch would move under every
    install; see jumpstart/README.md for why the tag has to point at itself."""
    ref = _entry()["source"]["repo_ref"]
    assert re.fullmatch(r"v\d+\.\d+\.\d+", ref), ref


def test_the_diagram_has_both_themes_from_the_catalogs_generator():
    """The catalog needs a light AND a dark SVG for an entry with a mermaid_diagram, rendered
    from that mermaid by Jumpstart's own generator (`tools/render-diagrams.ts` in
    microsoft/fabric-jumpstart, or its /tools/diagram-generator page) so they carry the
    catalog's theme. Re-render both whenever the mermaid changes: every node is in both."""
    labels = re.findall(r"\[([^\]]+)\]:::", _entry()["mermaid_diagram"])
    assert labels
    for variant in ("light", "dark"):
        svg = (JUMPSTART / f"{INSTALL['logical_id']}_{variant}.svg").read_text(encoding="utf-8")
        missing = [label for label in labels if f">{label}<" not in svg]
        assert not missing, f"re-render the {variant} SVG: {missing}"


def test_the_docs_page_shows_the_light_drawing():
    """Light is the catalog site's default theme. The page's picture is the repo's own
    drawing, docs/medallion-fabric-dbt.svg, an Excalidraw dark-mode export, without its
    inverting filter."""
    source = (REPO / "docs" / "medallion-fabric-dbt.svg").read_text(encoding="utf-8")
    invert = ' filter="invert(93%) hue-rotate(180deg)"'
    assert source.count(invert) == 1
    page = (CONTENT / "images" / "medallion-fabric-dbt.svg").read_text(encoding="utf-8")
    assert page == source.replace(invert, ""), "re-make the page's light SVG from docs/"


def test_the_docs_page_meets_the_content_contract():
    assert {p.name for p in CONTENT.iterdir()} <= {"index.md", "images"}
    page = (CONTENT / "index.md").read_text(encoding="utf-8")
    front = yaml.safe_load(page.split("---")[1])
    assert str(front.get("title", "")).strip()
    for image in re.findall(r"!\[[^\]]*\]\((?!https?://)([^)]+)\)", page):
        assert (CONTENT / image).is_file(), image
