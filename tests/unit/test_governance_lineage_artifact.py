"""The committed lineage artifact must keep describing a whole pipeline.

`make lineage` needs a workspace, so CI cannot regenerate this. What CI *can*
do is refuse an artifact that has quietly lost a tier -- a broken extraction
produces a smaller graph, not an error, which is the failure this guards.
"""

import json
from pathlib import Path
from typing import Any

import pytest

ARTIFACT = Path(__file__).resolve().parents[2] / "docs" / "lineage" / "column-lineage.json"

# The path a row actually travels, end to end. Losing any of these means the
# artifact stopped describing the platform.
REQUIRED_HOPS = [
    ("landing", "bronze"),
    ("bronze", "silver"),
    ("silver", "gold"),
    ("silver", "features"),
]


@pytest.fixture(scope="module")
def artifact() -> dict[str, Any]:
    if not ARTIFACT.exists():
        pytest.fail(f"{ARTIFACT} is missing; regenerate with `make lineage`")
    loaded: dict[str, Any] = json.loads(ARTIFACT.read_text())
    return loaded


def test_the_artifact_records_where_it_came_from(artifact: dict[str, Any]) -> None:
    assert artifact["source"] == "system.access.column_lineage"
    assert artifact["generated"], "an artifact with no generation time cannot be aged"


@pytest.mark.parametrize(("source", "target"), REQUIRED_HOPS)
def test_the_critical_path_is_present(artifact: dict[str, Any], source: str, target: str) -> None:
    """A hop vanishing is the symptom of a broken extraction, and it is silent."""
    hops = {(e["source_tier"], e["target_tier"]): e["column_edges"] for e in artifact["tier_edges"]}

    assert (source, target) in hops, f"{source} -> {target} is gone; the graph lost a tier"
    assert hops[(source, target)] > 0


def test_the_excluded_experiment_schemas_stay_excluded(artifact: dict[str, Any]) -> None:
    """Phase 2's A/B harness is not part of the platform and must not present as tiers."""
    tiers = {e["source_tier"] for e in artifact["tier_edges"]} | {
        e["target_tier"] for e in artifact["tier_edges"]
    }

    assert not [t for t in tiers if t.startswith("ab_")], f"A/B schemas leaked in: {tiers}"


def test_every_tier_in_the_graph_is_one_the_artifact_declares(artifact: dict[str, Any]) -> None:
    """A tier outside the declared scope means the filter and the scope disagree."""
    declared = set(artifact["scope"])
    seen = {e["source_tier"] for e in artifact["tier_edges"]} | {
        e["target_tier"] for e in artifact["tier_edges"]
    }

    assert seen <= declared, f"undeclared tiers present: {seen - declared}"


def test_the_blind_spots_ship_with_the_graph(artifact: dict[str, Any]) -> None:
    """A lineage graph reads as complete unless it says otherwise; this one is not."""
    spots = " ".join(artifact["blind_spots"]).lower()

    assert len(artifact["blind_spots"]) >= 4
    assert "local spark" in spots, "the invisible-test-suite caveat is the load-bearing one"
    assert "1-year" in spots or "rolling" in spots
