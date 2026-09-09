"""Drift between the training window and a scoring window.

Section 10 asks for "drift and training/serving skew monitored and visible";
line 1911 maps it to the MLOps claim and line 1922's resume bullet already
asserts it. Design doc 4.8 decision 5 splits it: this is the offline half.

The numbers below are measured, not invented -- Q3 2025 against the
2026 window characterized in Task 5.
"""

import math

import pytest
from pyspark.sql import SparkSession

from almanac.model.drift import (
    MAJOR_PSI,
    FeatureSummary,
    compare,
    population_stability_index,
    summarize,
)


def test_an_identical_distribution_has_zero_psi() -> None:
    bins = {"low": 0.5, "high": 0.5}
    assert population_stability_index(bins, bins) == pytest.approx(0.0)


def test_psi_grows_with_the_size_of_the_shift() -> None:
    reference = {"low": 0.5, "high": 0.5}
    small = population_stability_index(reference, {"low": 0.55, "high": 0.45})
    large = population_stability_index(reference, {"low": 0.9, "high": 0.1})

    assert small < large
    assert large > MAJOR_PSI


def test_an_empty_bin_does_not_divide_by_zero() -> None:
    """A category present in training and absent in scoring is the common
    case, and log(0) would make the whole report unreadable rather than
    flagging one feature.
    """
    psi = population_stability_index({"a": 0.5, "b": 0.5}, {"a": 1.0, "b": 0.0})

    assert psi > MAJOR_PSI
    assert not math.isnan(psi)


def test_a_feature_that_goes_wholly_null_is_schema_drift_not_covariate() -> None:
    """`is_draft` is present on every rich-era PR and absent from every
    reduced-era one -- measured 13,301 in the 2025 hour, 0 in all five 2026
    hours. That is not a distribution moving; it is the field being gone,
    and the two must not be reported the same way.
    """
    reference = [FeatureSummary("is_draft", null_share=0.0, bins={"true": 0.2, "false": 0.8})]
    current = [FeatureSummary("is_draft", null_share=1.0, bins={})]

    (finding,) = compare(reference, current)

    assert finding.feature == "is_draft"
    assert finding.kind == "schema"
    assert "cannot score" in finding.response


def test_a_moved_distribution_is_covariate_drift() -> None:
    reference = [FeatureSummary("opened_hour", null_share=0.0, bins={"day": 0.7, "night": 0.3})]
    current = [FeatureSummary("opened_hour", null_share=0.0, bins={"day": 0.2, "night": 0.8})]

    (finding,) = compare(reference, current)

    assert finding.kind == "covariate"
    assert finding.psi is not None and finding.psi > MAJOR_PSI


def test_a_stable_feature_is_not_reported() -> None:
    """A report that lists every feature every run is one nobody reads."""
    stable = [FeatureSummary("is_bot_author", null_share=0.0, bins={"true": 0.3, "false": 0.7})]
    current = [FeatureSummary("is_bot_author", null_share=0.0, bins={"true": 0.31, "false": 0.69})]

    assert compare(stable, current) == []


def test_a_feature_missing_from_the_scoring_window_is_schema_drift() -> None:
    """Absent entirely is not the same as present-and-null, but it has the
    same consequence for a model that reads it.
    """
    reference = [FeatureSummary("is_draft", null_share=0.0, bins={"true": 0.2, "false": 0.8})]

    (finding,) = compare(reference, [])

    assert finding.kind == "schema"


def test_the_measured_2025_to_2026_case_fires_on_both_kinds() -> None:
    """The real comparison: the schema break and the population shift are
    both present between the training window and any 2026 window.
    """
    reference = [
        FeatureSummary("is_draft", null_share=0.0, bins={"true": 0.13, "false": 0.87}),
        FeatureSummary("is_bot_author", null_share=0.0, bins={"true": 0.35, "false": 0.65}),
    ]
    current = [
        FeatureSummary("is_draft", null_share=1.0, bins={}),
        FeatureSummary("is_bot_author", null_share=0.0, bins={"true": 0.80, "false": 0.20}),
    ]

    kinds = {f.feature: f.kind for f in compare(reference, current)}

    assert kinds == {"is_draft": "schema", "is_bot_author": "covariate"}


def test_a_vanished_category_scores_the_same_as_an_explicit_zero() -> None:
    """A category can disappear two ways: an explicit 0.0 share, or the key
    simply not being there. They mean the same thing and must score the same.

    Added after a mutation survived: replacing the bin union with an
    intersection changed nothing, because every existing test spelled the
    empty bin as `0.0` rather than omitting it. Intersecting silently drops
    the vanished category -- scoring the worst possible drift as if it were
    milder than a small shift.
    """
    reference = {"x": 0.5, "y": 0.5}
    explicit_zero = population_stability_index(reference, {"x": 1.0, "y": 0.0})
    absent_key = population_stability_index(reference, {"x": 1.0})

    assert explicit_zero == pytest.approx(absent_key)
    assert absent_key > MAJOR_PSI


@pytest.mark.spark
def test_summarize_reads_null_share_and_bins_from_a_real_frame(spark: "SparkSession") -> None:
    """The Spark edge. `is_draft` null on every row is what the reduced era
    actually looks like, so the summary must report null_share 1.0 rather
    than an empty distribution that reads as "no data".
    """
    frame = spark.createDataFrame(
        [(True, None), (False, None), (False, None), (False, None)],
        "is_bot_author boolean, is_draft boolean",
    )

    summaries = {s.name: s for s in summarize(frame, ["is_bot_author", "is_draft"])}

    assert summaries["is_draft"].null_share == 1.0
    assert summaries["is_draft"].bins == {}
    assert summaries["is_bot_author"].null_share == 0.0
    assert summaries["is_bot_author"].bins == pytest.approx({"true": 0.25, "false": 0.75})


@pytest.mark.spark
def test_summarize_bins_a_numeric_feature_by_quantile_edges(spark: "SparkSession") -> None:
    """Shares must sum to 1 over non-null rows, or PSI is meaningless."""
    frame = spark.createDataFrame([(float(i),) for i in range(100)], "prior_pr_count double")

    (summary,) = summarize(frame, ["prior_pr_count"])

    assert sum(summary.bins.values()) == pytest.approx(1.0)
    assert len(summary.bins) > 1
