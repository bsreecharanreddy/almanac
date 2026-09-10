"""Drift between the window the champion was trained on and a scoring window.

Section 10's "drift and training/serving skew monitored and visible"; design
doc 4.8 decision 5 splits it, and this is the offline half -- no cloud, no
billable window.
"""

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

# The conventional PSI reading, not a measurement from this project: under
# 0.1 is no meaningful shift, 0.1-0.25 moderate, above 0.25 major. Stated as
# convention on purpose -- this repo's rule is that a quoted number names
# where it came from, and this one comes from practice, not from Almanac.
MODERATE_PSI = 0.1
MAJOR_PSI = 0.25

# A share small enough to stand in for "absent" without taking log(0). Well
# below any bin this project reports on, so it changes no real verdict.
_FLOOR = 1e-6

# Above this, a feature is treated as gone rather than merely sparse.
_NULL_IS_SCHEMA_DRIFT = 0.99

# Shared by `compare`'s schema-drift branch and `missing_for` below, so the
# response a caller acts on is stated once -- two copies of this sentence
# is exactly the kind of duplication that goes stale silently (CLAUDE.md).
_SCHEMA_DRIFT_RESPONSE = (
    "The champion reads this feature, so it cannot score this window. "
    "Refuse to serve rather than re-baselining."
)


@dataclass(frozen=True)
class FeatureSummary:
    """One feature's distribution, reduced to what drift needs.

    Summaries rather than DataFrames so the comparison is pure and testable
    without a SparkSession; the Spark edge that builds these lives at the
    caller.
    """

    name: str
    null_share: float
    bins: dict[str, float]


@dataclass(frozen=True)
class DriftFinding:
    """One drifted feature, and what a caller should actually do about it."""

    feature: str
    kind: str
    psi: float | None
    detail: str
    response: str


def population_stability_index(reference: dict[str, float], current: dict[str, float]) -> float:
    """PSI between two binned distributions.

    Bins absent from either side are floored rather than skipped: a category
    that disappears entirely is the strongest drift signal there is, and
    dropping it would score that case as *less* drifted than a mild shift.
    """
    total = 0.0
    for label in set(reference) | set(current):
        expected = max(reference.get(label, 0.0), _FLOOR)
        actual = max(current.get(label, 0.0), _FLOOR)
        total += (actual - expected) * math.log(actual / expected)
    return total


def compare(
    reference: Sequence[FeatureSummary], current: Sequence[FeatureSummary]
) -> list[DriftFinding]:
    """Every feature that drifted, and nothing that did not.

    Only drifted features are returned: a report listing all of them every
    run is one nobody reads, and an alarm nobody reads is not a monitor.
    """
    by_name = {f.name: f for f in current}
    findings: list[DriftFinding] = []

    for ref in reference:
        now = by_name.get(ref.name)

        # Schema drift is checked first and reported instead of covariate
        # drift, never alongside it: when a field is gone its remaining
        # distribution is an artefact, and a PSI over it would be noise
        # dressed as a measurement.
        if now is None or now.null_share >= _NULL_IS_SCHEMA_DRIFT:
            was = "absent from the scoring window" if now is None else "null on every row"
            findings.append(
                DriftFinding(
                    feature=ref.name,
                    kind="schema",
                    psi=None,
                    detail=f"{ref.name} is {was}; it was populated in training",
                    response=_SCHEMA_DRIFT_RESPONSE,
                )
            )
            continue

        psi = population_stability_index(ref.bins, now.bins)
        if psi >= MODERATE_PSI:
            severity = "major" if psi >= MAJOR_PSI else "moderate"
            findings.append(
                DriftFinding(
                    feature=ref.name,
                    kind="covariate",
                    psi=psi,
                    detail=f"{ref.name} PSI {psi:.3f} ({severity})",
                    response=(
                        "Scores stay computable but their calibration is not "
                        "carried over. Re-score against held-out outcomes from "
                        "this window before trusting the ranking."
                    ),
                )
            )

    return findings


def missing_for(features: Mapping[str, float | None]) -> list[DriftFinding]:
    """The schema-drift question `compare` asks of a whole window, asked of
    one already-fetched row instead: not "has this window's distribution
    shifted", but "is the value this row needs here at all".

    No reference distribution and no PSI: a null value needs no training-time
    baseline to know it cannot be scored, and a covariate-drift comparison is
    not meaningful at n=1 -- concentrating a reference's mass onto whichever
    single bin one row happens to fall in reads as drift regardless of
    whether anything really shifted. `predict` (agent/tools.py) is this
    function's caller; `compare` above stays the window-level check.
    """
    return [
        DriftFinding(
            feature=name,
            kind="schema",
            psi=None,
            detail=f"{name} is null for this row",
            response=_SCHEMA_DRIFT_RESPONSE,
        )
        for name, value in sorted(features.items())
        if value is None
    ]


# Enough resolution to see a shift, few enough that each bin holds real mass
# at this project's row counts. Deciles are the usual choice and match the
# calibration curve section 7's page 1 already draws.
_QUANTILES = 10


def _numeric_bins(frame: DataFrame, column: str, non_null: int) -> dict[str, float]:
    edges = sorted(
        set(frame.stat.approxQuantile(column, [i / _QUANTILES for i in range(1, _QUANTILES)], 0.01))
    )
    bucket = F.lit(0)
    for edge in edges:
        bucket = bucket + F.when(F.col(column) > F.lit(edge), 1).otherwise(0)
    counts = (
        frame.where(F.col(column).isNotNull())
        .select(bucket.alias("_b"))
        .groupBy("_b")
        .count()
        .collect()
    )
    return {f"q{r['_b']}": r["count"] / non_null for r in counts}


def summarize(frame: DataFrame, columns: Sequence[str]) -> list[FeatureSummary]:
    """Reduce a feature frame to one summary per column. The Spark edge.

    A column that is null on every row reports ``null_share`` 1.0 and an
    empty distribution -- distinguishable from "no rows at all", which is
    the distinction `compare` needs to call schema drift.
    """
    total = frame.count()
    summaries: list[FeatureSummary] = []

    for column in columns:
        non_null = frame.where(F.col(column).isNotNull()).count()
        null_share = 1.0 - (non_null / total) if total else 1.0

        if not non_null:
            summaries.append(FeatureSummary(column, null_share=null_share, bins={}))
            continue

        if isinstance(frame.schema[column].dataType, NumericType):
            bins = _numeric_bins(frame, column, non_null)
        else:
            rows = frame.where(F.col(column).isNotNull()).groupBy(column).count().collect()
            bins = {str(r[column]).lower(): r["count"] / non_null for r in rows}

        summaries.append(FeatureSummary(column, null_share=null_share, bins=bins))

    return summaries
