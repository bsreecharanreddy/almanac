"""registration.py: SQL-string building for the Unity Catalog feature-
table constraints, plus the one round trip that runs on local Delta.

primary_key_sql and not_null_key_sql are string-only here -- the local
Derby metastore rejects a TIMESERIES constraint, and open-source Delta
rejects `ALTER COLUMN ... SET NOT NULL` on a populated table. Both run
for real against Unity Catalog in Phase 6's cloud burn (design §4.4a,
§4.6). change_data_feed_sql does round-trip locally, so it gets a real one.
"""

from pathlib import Path

import pytest
from pyspark.sql import Row, SparkSession

from almanac.features.registration import (
    change_data_feed_sql,
    not_null_key_sql,
    primary_key_sql,
    register_feature_table,
)


def test_a_temporal_table_gets_a_timeseries_primary_key() -> None:
    drop_sql, add_sql = primary_key_sql(
        schema="features",
        table="author_activity",
        entity_cols=["author_login"],
        event_time_col="event_time",
    )
    assert (
        drop_sql
        == "ALTER TABLE features.author_activity DROP CONSTRAINT IF EXISTS author_activity_pk"
    )
    assert add_sql == (
        "ALTER TABLE features.author_activity ADD CONSTRAINT author_activity_pk "
        "PRIMARY KEY (author_login, event_time TIMESERIES)"
    )


def test_a_non_temporal_table_gets_a_plain_composite_primary_key() -> None:
    """pr_static carries no event_time -- registering it as TIMESERIES
    would misrepresent it as a temporal lookup table it is not.
    """
    _, add_sql = primary_key_sql(
        schema="features", table="pr_static", entity_cols=["repo_id", "pr_number"]
    )
    expected = (
        "ALTER TABLE features.pr_static ADD CONSTRAINT pr_static_pk "
        "PRIMARY KEY (repo_id, pr_number)"
    )
    assert add_sql == expected


def test_timeseries_pk_unchanged() -> None:
    """Regression guard: primary_key_sql and not_null_key_sql now share
    _key_columns. §4.4a's constraint DDL is already correct against real
    UC -- routing it through the shared helper must not have shifted it.
    """
    drop_sql, add_sql = primary_key_sql(
        schema="features",
        table="repo_activity",
        entity_cols=["repo_id"],
        event_time_col="event_time",
    )
    assert (
        drop_sql == "ALTER TABLE features.repo_activity DROP CONSTRAINT IF EXISTS repo_activity_pk"
    )
    assert add_sql == (
        "ALTER TABLE features.repo_activity ADD CONSTRAINT repo_activity_pk "
        "PRIMARY KEY (repo_id, event_time TIMESERIES)"
    )


def test_sets_pk_columns_not_null() -> None:
    """One statement per key column, event-time included, and never with
    the TIMESERIES word -- that belongs only in the constraint.
    """
    stmts = not_null_key_sql(
        schema="features",
        table="author_activity",
        entity_cols=["author_login"],
        event_time_col="event_time",
    )
    assert stmts == [
        "ALTER TABLE features.author_activity ALTER COLUMN author_login SET NOT NULL",
        "ALTER TABLE features.author_activity ALTER COLUMN event_time SET NOT NULL",
    ]


def test_sets_pk_columns_not_null_omits_event_time_for_a_static_table() -> None:
    stmts = not_null_key_sql(
        schema="features", table="pr_static", entity_cols=["repo_id", "pr_number"]
    )
    assert stmts == [
        "ALTER TABLE features.pr_static ALTER COLUMN repo_id SET NOT NULL",
        "ALTER TABLE features.pr_static ALTER COLUMN pr_number SET NOT NULL",
    ]


@pytest.mark.spark
def test_enables_cdf_on_feature_table(spark: SparkSession, tmp_path: Path) -> None:
    path = str(tmp_path / "repo_activity")
    spark.createDataFrame(
        [Row(repo_id=1, event_time="2025-11-03T14:00:00", events_prior_1h=3)]
    ).write.format("delta").mode("overwrite").save(path)
    # A schema unique to this test: the fixture's metastore is one
    # session-wide Derby instance, and a reused schema.table would keep a
    # stale LOCATION from an earlier test (the AuditChainTest isolation
    # hazard, same as test_embed_pipeline).
    schema = f"features_{tmp_path.name}"
    register_feature_table(spark, table="repo_activity", path=path, schema=schema)

    spark.sql(change_data_feed_sql(schema=schema, table="repo_activity"))

    props = {
        r["key"]: r["value"]
        for r in spark.sql(f"SHOW TBLPROPERTIES {schema}.repo_activity").collect()
    }
    assert props["delta.enableChangeDataFeed"] == "true"
