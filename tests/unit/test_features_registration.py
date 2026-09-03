"""primary_key_sql: pure SQL-string building for the UC TIMESERIES
constraint. Execution against a real Unity Catalog metastore is verified
live during Phase 3's cloud burn (design doc §4.4a) -- the local Derby
metastore this repo's other tests run against does not support
TIMESERIES, so nothing here calls spark.sql.
"""

from almanac.features.registration import primary_key_sql


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
