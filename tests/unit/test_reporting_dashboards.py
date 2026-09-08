"""The committed dashboard JSON, checked for the things a `terraform plan` cannot see.

`terraform plan` proves the resources exist; it never opens the JSON. Rendering
is proven only by applying it in Task 10's attended window -- the Lakeview widget
schema is not publicly documented, so nothing here claims the visualisations are
correct. What *is* checkable offline is checked: every widget points at a dataset
that exists, no identity column reaches a displayed field, and the two panels
that must say they are unavailable still say so.
"""

import json
import re
from pathlib import Path
from typing import Any

import pytest

DASHBOARDS = Path(__file__).resolve().parents[2] / "dashboards"

# Columns that identify a natural person. A dashboard may aggregate over these
# (contributor concentration does) but must never render one (docs/pseudonymization.md).
IDENTITY_COLUMNS = ("author_login", "actor_login")

PAGES = sorted(DASHBOARDS.glob("*.json"))


def load(path: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(path.read_text())
    return loaded


def widgets(dashboard: dict[str, Any]) -> list[dict[str, Any]]:
    return [entry["widget"] for page in dashboard["pages"] for entry in page["layout"]]


def test_all_three_pages_are_committed() -> None:
    """§7 asks for three pages; §4.7 made page 3 a dashboard like the other two."""
    assert [p.name for p in PAGES] == [
        "01-review-sla-risk.json",
        "02-model-platform-health.json",
        "03-developer-engagement.json",
    ]


@pytest.mark.parametrize("path", PAGES, ids=lambda p: p.stem)
def test_every_widget_query_names_a_dataset_that_exists(path: Path) -> None:
    """A widget pointing at a missing dataset renders empty and raises nothing."""
    dashboard = load(path)
    declared = {dataset["name"] for dataset in dashboard["datasets"]}

    referenced = {
        query["query"]["datasetName"]
        for widget in widgets(dashboard)
        for query in widget.get("queries", [])
    }

    assert referenced <= declared, f"{path.name}: undefined datasets {referenced - declared}"


@pytest.mark.parametrize("path", PAGES, ids=lambda p: p.stem)
def test_every_declared_dataset_is_used(path: Path) -> None:
    """An unused dataset is a query that still runs and still costs warehouse time."""
    dashboard = load(path)
    referenced = {
        query["query"]["datasetName"]
        for widget in widgets(dashboard)
        for query in widget.get("queries", [])
    }

    unused = {dataset["name"] for dataset in dashboard["datasets"]} - referenced

    assert not unused, f"{path.name}: datasets declared but never rendered: {unused}"


@pytest.mark.parametrize("path", PAGES, ids=lambda p: p.stem)
def test_no_identity_column_reaches_a_displayed_field(path: Path) -> None:
    """The rule Task 8 wrote down, enforced where it is easiest to break it.

    `author_login` may appear inside a dataset's SQL -- concentration is computed
    by grouping on it -- but it must never be a field a widget renders.
    """
    rendered = [
        field["name"]
        for widget in widgets(load(path))
        for query in widget.get("queries", [])
        for field in query["query"]["fields"]
    ]

    leaked = [name for name in rendered if name in IDENTITY_COLUMNS]

    assert not leaked, f"{path.name}: identity rendered in a widget: {leaked}"


def test_page_two_still_says_which_panels_are_unavailable() -> None:
    """Marked, not dropped: an absent panel a reader cannot see is
    indistinguishable from a panel nobody thought of.
    """
    text = json.dumps(load(DASHBOARDS / "02-model-platform-health.json"))

    assert "feature freshness" in text.lower()
    assert "training/serving skew" in text.lower()


def test_page_three_carries_the_non_negotiable_limitations_panel() -> None:
    """§7 calls this the cheapest high-signal element in the project."""
    text = json.dumps(load(DASHBOARDS / "03-developer-engagement.json")).lower()

    for claim in ("stars are gross", "bots are excluded", "capped at 20", "percentile"):
        assert claim in text, f"limitations panel lost: {claim!r}"


@pytest.mark.parametrize("path", PAGES, ids=lambda p: p.stem)
def test_no_widget_overlaps_another_on_the_canvas(path: Path) -> None:
    """Two widgets on the same cells is a layout bug that only shows up visually,
    which is exactly the kind that survives to a demo.
    """
    boxes = [entry["position"] for page in load(path)["pages"] for entry in page["layout"]]

    for i, a in enumerate(boxes):
        for b in boxes[i + 1 :]:
            overlaps_x = a["x"] < b["x"] + b["width"] and b["x"] < a["x"] + a["width"]
            overlaps_y = a["y"] < b["y"] + b["height"] and b["y"] < a["y"] + a["height"]
            assert not (overlaps_x and overlaps_y), f"{path.name}: {a} overlaps {b}"


# Every object the dashboards may query, verified against the live workspace on
# 2026-09-08 with `databricks tables list`. The first draft of these pages
# referenced four that do not exist -- `silver.events_clean` (the table is
# `silver.events`), `bronze.events` (there is no bronze schema; Bronze is an
# ADLS path) and `features.pr_breach_predictions` before the scoring job
# registered it. `terraform plan` cannot see any of that, and a dashboard
# querying a missing table renders an error panel rather than failing loudly.
KNOWN_OBJECTS = {
    "almanac_dbx.silver.events",
    "almanac_dbx.silver.events_quarantine",
    "almanac_dbx.gold.agg_repo_daily",
    "almanac_dbx.gold.dim_repo",
    "almanac_dbx.gold.fact_pull_request",
    "almanac_dbx.gold.int_pr_events",
    "almanac_dbx.serving_logs.pr_review_sla_risk_payload",
    # Created and registered by the scoring job (Task 9a) before the window.
    "almanac_dbx.features.pr_breach_predictions",
    "system.lakeflow.job_run_timeline",
}

_QUALIFIED = re.compile(r"\b(almanac_dbx|system)\.[a-z_]+\.[a-z_]+\b")


@pytest.mark.parametrize("path", PAGES, ids=lambda p: p.stem)
def test_every_queried_object_is_one_that_exists(path: Path) -> None:
    sql = "".join(line for dataset in load(path)["datasets"] for line in dataset["queryLines"])

    names = {m.group(0) for m in _QUALIFIED.finditer(sql)}

    assert names <= KNOWN_OBJECTS, f"{path.name}: unknown objects {sorted(names - KNOWN_OBJECTS)}"
