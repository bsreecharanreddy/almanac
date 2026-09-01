from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from almanac.pipeline.eras import normalize_events
from almanac.pipeline.quality import apply_rules, split
from almanac.pipeline.source import QualityRule, Severity, SourceConfig
from tests.helpers import one

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
    """The three-valued-logic trap, wrapped in coalesce(cond, False).

    `NULL > 5` is NULL, not False. An un-coalesced rule would let the row
    through as if it had passed.
    """
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
    """A rule naming a column the pipeline drops cannot run at all.

    `created_at_not_future` compares against `ingested_at`, which the Silver
    contract originally omitted -- so that rule was unrunnable, and nothing
    said so until a real file was pushed through. Each rule is applied on
    its own here, so a failure names the rule instead of failing the whole
    pipeline with one unresolved column.
    """
    silver = normalize_events(
        spark.createDataFrame(
            [
                (
                    "2025-08-13T14:00:00Z",
                    "a",
                    1,
                    "o/r",
                    "PushEvent",
                    "9",
                    None,
                    datetime(2026, 9, 2, 12, 0, tzinfo=UTC),
                )
            ],
            "created_at_raw string, actor_raw string, repo_id long, repo_name string, "
            "event_type string, id string, event_url string, ingested_at timestamp",
        )
    )
    for rule in SourceConfig.load(Path("conf/sources/gharchive.yml")).quality_rules:
        apply_rules(silver, [rule]).schema  # noqa: B018 -- forces analysis
