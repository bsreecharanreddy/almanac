"""The architecture walkthrough's node/edge graph. Pure data -- every claim
links to where it was actually found; nothing here is restated from there.
See docs/design/2026-09-11-almanac-architecture-walkthrough-design.md.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ArchNode:
    id: str
    label: str
    summary: str
    links: tuple[str, ...]


@dataclass(frozen=True)
class ArchEdge:
    source: str
    target: str


NODES: tuple[ArchNode, ...] = (
    ArchNode(
        "bronze",
        "Bronze",
        "Raw payload, untransformed. A new event type cannot break ingestion.",
        ("docs/limitations.md", "docs/findings/2026-09-02-bronze-is-single-threaded.md"),
    ),
    ArchNode(
        "silver",
        "Silver",
        "Per-type parsing and quality rules. Bad records are quarantined, never dropped.",
        ("docs/findings/2026-09-08-first-real-quarantine.md",),
    ),
    ArchNode(
        "gold",
        "Gold",
        "dbt-built facts and dimensions. A merge defect here was found the hard way.",
        (
            "docs/findings/2026-09-04-gold-is-a-metastore-table-not-a-path.md",
            "docs/findings/2026-09-08-pr-opened-spine-fanout.md",
        ),
    ),
    ArchNode(
        "features",
        "Feature Platform",
        "As-of joins enforce point-in-time correctness -- the governing invariant.",
        ("docs/adr/0001-hand-rolled-as-of-join.md",),
    ),
    ArchNode(
        "model",
        "Model",
        "A baseline shipped first. The registered champion had a leakage bug, found and fixed.",
        (
            "docs/findings/2026-09-08-champion-rescored-temporal-split.md",
            "docs/decision-memo.md",
        ),
    ),
    ArchNode(
        "serving",
        "Serving",
        "A live endpoint, measured for skew against offline scoring.",
        (
            "docs/findings/2026-09-04-serving-endpoint-measured.md",
            "docs/findings/2026-09-08-training-serving-skew-measured.md",
        ),
    ),
    ArchNode(
        "streaming",
        "Streaming",
        "A live poller and an online store. A watermark silently dropped real data once.",
        (
            "docs/postmortem-watermark-data-loss.md",
            "docs/adr/0004-dedup-on-write-not-watermark.md",
        ),
    ),
    ArchNode(
        "agent",
        "Agent layer",
        "Four read-only tools over MCP, bounded, and audited.",
        ("docs/findings/2026-09-11-agent-layer-window-cost.md",),
    ),
    ArchNode(
        "grounding",
        "Grounding verifier",
        "Checks the relationship a claim makes, not only that its number is real.",
        ("CHANGELOG.md",),
    ),
)

EDGES: tuple[ArchEdge, ...] = (
    ArchEdge("bronze", "silver"),
    ArchEdge("silver", "gold"),
    ArchEdge("gold", "features"),
    ArchEdge("gold", "streaming"),
    ArchEdge("features", "model"),
    ArchEdge("model", "serving"),
    ArchEdge("model", "agent"),
    ArchEdge("agent", "grounding"),
)


def missing_links(repo_root: Path) -> dict[str, list[str]]:
    """Node id -> its links that do not resolve to a real file. Empty if clean."""
    result: dict[str, list[str]] = {}
    for node in NODES:
        gone = [link for link in node.links if not (repo_root / link).exists()]
        if gone:
            result[node.id] = gone
    return result
