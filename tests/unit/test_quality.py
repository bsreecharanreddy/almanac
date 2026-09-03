from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from almanac.pipeline.eras import normalize_events
from almanac.pipeline.quality import apply_rules, split
from almanac.pipeline.source import QualityRule, Severity, SourceConfig
from tests.helpers import one, raw

pytestmark = pytest.mark.spark

RULES = [
    QualityRule(name="id_present", expression="event_id IS NOT NULL", severity=Severity.REJECT),
    QualityRule(name="repo_present", expression="repo_id IS NOT NULL", severity=Severity.REJECT),
    QualityRule(name="actor_present", expression="actor_login IS NOT NULL", severity=Severity.WARN),
]
SCHEMA = "event_id string, repo_id long, actor_login string"


def test_passing_record_has_empty_failed_rules(spark: SparkSession) -> None:
    df = spark.createDataFrame([("e1", 1, "a")], SCHEMA)
    assert one(apply_rules(df, RULES))["_failed_rules"] == []


def test_failed_rule_is_named_not_just_flagged(spark: SparkSession) -> None:
    # A boolean tells you a record is bad. The rule name tells you why,
    # which is what makes data quality analyzable rather than a bin.
    df = spark.createDataFrame([(None, 1, "a")], SCHEMA)
    assert one(apply_rules(df, RULES))["_failed_rules"] == ["id_present"]


def test_multiple_failures_all_recorded(spark: SparkSession) -> None:
    df = spark.createDataFrame([(None, None, "a")], SCHEMA)
    got = one(apply_rules(df, RULES))["_failed_rules"]
    assert set(got) == {"id_present", "repo_present"}


def test_null_in_predicate_counts_as_failure(spark: SparkSession) -> None:
    """The three-valued-logic trap, wrapped in coalesce(cond, False)."""
    rule = [QualityRule(name="positive", expression="repo_id > 0", severity=Severity.REJECT)]
    df = spark.createDataFrame([("e1", None, "a")], SCHEMA)
    assert one(apply_rules(df, rule))["_failed_rules"] == ["positive"]


def test_warn_severity_does_not_quarantine(spark: SparkSession) -> None:
    df = spark.createDataFrame([("e1", 1, None)], SCHEMA)
    clean, quarantined = split(apply_rules(df, RULES))
    assert clean.count() == 1
    assert quarantined.count() == 0


def test_warn_is_still_recorded_on_the_clean_row(spark: SparkSession) -> None:
    df = spark.createDataFrame([("e1", 1, None)], SCHEMA)
    clean, _ = split(apply_rules(df, RULES))
    assert one(clean)["_failed_rules"] == ["actor_present"]


def test_reject_severity_quarantines(spark: SparkSession) -> None:
    df = spark.createDataFrame([(None, 1, "a")], SCHEMA)
    clean, quarantined = split(apply_rules(df, RULES))
    assert clean.count() == 0
    assert quarantined.count() == 1


def test_split_conserves_every_record(spark: SparkSession) -> None:
    """Records must not vanish between layers. Asserted, not assumed."""
    df = spark.createDataFrame(
        [("e1", 1, "a"), (None, 1, "a"), ("e3", None, None), ("e4", 4, None)],
        SCHEMA,
    )
    applied = apply_rules(df, RULES)
    clean, quarantined = split(applied)
    assert clean.count() + quarantined.count() == applied.count() == 4


def test_every_declared_rule_resolves_against_the_silver_shape(spark: SparkSession) -> None:
    """A rule naming a column the pipeline drops cannot run at all."""
    silver = normalize_events(
        raw(
            spark,
            (
                "2025-08-13T14:00:00Z",
                "a",
                1,
                "o/r",
                "PushEvent",
                "9",
                None,
                datetime(2026, 9, 2, 12, 0, tzinfo=UTC),
            ),
        )
    )
    for rule in SourceConfig.load(Path("conf/sources/gharchive.yml")).quality_rules:
        apply_rules(silver, [rule]).schema  # noqa: B018 -- forces analysis


# Everything above proves the *mechanism* quarantines, using rules written for
# the test. Nothing proved a rule the pipeline actually ships ever fires, so
# the Tier 3 backfill's 0 quarantined of 341,060,851 rows was unreadable: clean
# data and an inert ruleset produce the same number.
SHIPPED = SourceConfig.load(Path("conf/sources/gharchive.yml")).quality_rules

# Case -> the column to corrupt, the corruption, and every shipped rule it must
# trip. Built lazily; F.lit needs a live SparkContext.
type Violation = tuple[str, Column, set[str]]

_CASES = (
    "event_id_null",
    "event_id_empty",
    "created_at_null",
    "created_at_future",
    "repo_id_null",
    "event_type_null",
)


def _violations() -> dict[str, Violation]:
    return {
        "event_id_null": ("event_id", F.lit(None).cast("string"), {"event_id_present"}),
        "event_id_empty": ("event_id", F.lit(""), {"event_id_present"}),
        # A null timestamp trips the comparison too: NULL <= x is NULL, which
        # coalesce(_, False) turns into a failure rather than a silent pass.
        "created_at_null": (
            "created_at",
            F.lit(None).cast("timestamp"),
            {"created_at_present", "created_at_not_future"},
        ),
        # Corruption, not staleness. Backfilled rows are already a year older
        # than ingested_at, so on the only mode this pipeline has run in, this
        # rule fires on a future-dated row or not at all.
        "created_at_future": (
            "created_at",
            F.col("ingested_at") + F.expr("INTERVAL 1 DAY"),
            {"created_at_not_future"},
        ),
        "repo_id_null": ("repo_id", F.lit(None).cast("long"), {"repo_id_present"}),
        "event_type_null": ("event_type", F.lit(None).cast("string"), {"event_type_present"}),
    }


def _passing_silver(spark: SparkSession) -> DataFrame:
    """A Silver row passing every shipped rule, so an injected failure is the only one."""
    return normalize_events(
        raw(
            spark,
            (
                "2025-08-13T14:00:00Z",
                "a",
                1,
                "o/r",
                "PushEvent",
                "9",
                None,
                datetime(2026, 9, 2, 12, 0, tzinfo=UTC),
            ),
        )
    )


def test_the_baseline_row_passes_every_shipped_rule(spark: SparkSession) -> None:
    """Without this the violation cases below could pass for the wrong reason."""
    clean, quarantined = split(apply_rules(_passing_silver(spark), SHIPPED))
    assert quarantined.count() == 0
    assert one(clean)["_failed_rules"] == []


@pytest.mark.parametrize("case", _CASES)
def test_each_shipped_reject_rule_quarantines_what_it_names(spark: SparkSession, case: str) -> None:
    column, corruption, expected = _violations()[case]
    df = _passing_silver(spark).withColumn(column, corruption)
    clean, quarantined = split(apply_rules(df, SHIPPED))
    assert clean.count() == 0
    assert set(one(quarantined)["_failed_rules"]) == expected


def test_every_shipped_reject_rule_has_a_case_proving_it_fires(spark: SparkSession) -> None:
    """A new reject rule in the YAML must arrive with a case, or 0 quarantined means nothing."""
    violations = _violations()
    assert set(_CASES) == set(violations)
    covered: set[str] = set().union(*(expected for _, _, expected in violations.values()))
    rejects = {rule.name for rule in SHIPPED if rule.severity is Severity.REJECT}
    assert rejects <= covered
