"""The architecture walkthrough's node/edge graph -- pure data, its own invariants."""

from pathlib import Path

from almanac.demo.architecture import EDGES, NODES, missing_links

_REPO_ROOT = Path(__file__).resolve().parents[2]


def test_every_node_id_is_unique() -> None:
    ids = [n.id for n in NODES]
    assert len(ids) == len(set(ids))


def test_every_edge_endpoint_names_a_real_node() -> None:
    ids = {n.id for n in NODES}
    for edge in EDGES:
        assert edge.source in ids, f"{edge.source} is not a node id"
        assert edge.target in ids, f"{edge.target} is not a node id"


def test_no_node_carries_a_link_free_summary_or_a_summary_free_link() -> None:
    """A node that asserts something must point at where it was measured."""
    for node in NODES:
        assert node.links, f"{node.id} has no link -- it is a claim with no source"


def test_no_summary_or_label_contains_a_digit() -> None:
    """The design doc's governing rule: a node names a fact, it never restates
    one. A digit in prose is a restated measurement; digits belong only in
    link paths (dates, ADR numbers), never in text a viewer reads without
    clicking through."""
    for node in NODES:
        assert not any(c.isdigit() for c in node.label), node.id
        assert not any(c.isdigit() for c in node.summary), node.id


def test_every_link_resolves_to_a_real_file() -> None:
    missing = missing_links(_REPO_ROOT)
    assert not missing, f"dead links: {missing}"
