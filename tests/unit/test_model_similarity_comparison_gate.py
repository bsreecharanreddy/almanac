"""The champion gate constant must not drift from the finding that measured it.

`CHAMPION_AVERAGE_PRECISION` states a measured fact that is also written
down in `docs/findings/`. That is the same fact in two places, and on
2026-09-08 the code copy went stale: the champion was re-scored on a
temporal split and the constant kept the retracted random-split 0.612,
silently holding the gate at a bar 31% too high. This pins them together.
"""

from pathlib import Path

from almanac.model.similarity_comparison import CHAMPION_AVERAGE_PRECISION

FINDING = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "findings"
    / "2026-09-08-champion-rescored-temporal-split.md"
)


def test_the_gate_matches_the_finding_that_measured_it() -> None:
    """A re-score that updates the doc and forgets the constant fails here."""
    assert f"{CHAMPION_AVERAGE_PRECISION}" in FINDING.read_text()


def test_the_retracted_random_split_figure_is_not_the_gate() -> None:
    """0.612 came from a random split; both sides of the gate are temporal now."""
    assert CHAMPION_AVERAGE_PRECISION != 0.612
