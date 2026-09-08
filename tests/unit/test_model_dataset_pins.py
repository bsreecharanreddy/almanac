"""`feature_pins`: the refusal that stops a training frame mixing points in time.

No SparkSession, so this runs in the fast gate -- which is where a guard against
a silent point-in-time defect belongs, rather than only in the 20-minute suite
that found it.
"""

import pytest

from almanac.features.runner import FEATURE_TABLES
from almanac.model.dataset import FEATURE_TABLE_NAMES, feature_pins


def test_no_pin_reads_every_table_live() -> None:
    assert feature_pins(None) == dict.fromkeys(FEATURE_TABLE_NAMES)


def test_a_full_pin_is_returned_per_table() -> None:
    """The versions differ on purpose: one shared number was the defect."""
    pinned = {"author_activity": 10, "repo_activity": 11, "pr_static": 8}

    assert feature_pins(pinned) == pinned


def test_a_partial_pin_is_refused_rather_than_read_live() -> None:
    """The silent failure this exists to end: pinning two tables and reading the
    third live mixes points in time and raises nothing.
    """
    with pytest.raises(KeyError, match="pr_static"):
        feature_pins({"author_activity": 0, "repo_activity": 0})


def test_an_unknown_table_is_refused_rather_than_ignored() -> None:
    """A typo would otherwise pin nothing at all, quietly."""
    with pytest.raises(KeyError, match="author_activty"):
        feature_pins(
            {"author_activty": 0, "author_activity": 0, "repo_activity": 0, "pr_static": 0}
        )


def test_the_pinnable_tables_are_the_tables_the_runner_writes() -> None:
    """FEATURE_TABLE_NAMES is stated in dataset.py rather than imported from
    features.runner, which already imports this module through
    similarity_runner. This is what keeps the two from drifting.
    """
    assert tuple(spec.name for spec in FEATURE_TABLES) == FEATURE_TABLE_NAMES
