"""online_store: the contract this wrapper adds over FeatureEngineeringClient
-- a smallest-capacity default, a CDF precondition the service would otherwise
fail on only after the store started billing, and an idempotent delete the real
client does not provide. `load_client` builds the real client and is
deliberately not exercised, the same "verified only against the real service"
precedent as `embed.query`'s VectorSearchIndex.
"""

import inspect
from pathlib import Path
from typing import Any

import pytest
from databricks.feature_engineering import FeatureEngineeringClient
from databricks.sdk.errors import NotFound
from pyspark.sql import Row, SparkSession

from almanac.features.registration import change_data_feed_sql, register_feature_table
from almanac.stream.online_store import (
    create_store,
    delete_store,
    publish_feature_table,
    require_store,
)


class _FakeClient:
    """Records what the real client would have been asked to do. Keyword-only
    throughout, matching the real 0.17.1 signatures -- a double that accepted
    positionals would let a call pass here and fail against the service.
    """

    def __init__(self) -> None:
        self.created: list[tuple[str, str]] = []
        self.published: list[dict[str, Any]] = []
        self.deleted: list[str] = []
        self._live: set[str] = set()

    def create_online_store(self, *, name: str, capacity: str) -> Any:
        self.created.append((name, capacity))
        self._live.add(name)
        return {"name": name, "capacity": capacity}

    def get_online_store(self, *, name: str) -> Any:
        # Returns None for an absent store rather than raising -- the opposite
        # of delete_online_store below, and the real client's own behaviour.
        return {"name": name} if name in self._live else None

    def publish_table(
        self,
        *,
        online_store: Any,
        source_table_name: str,
        online_table_name: str,
        publish_mode: str,
    ) -> Any:
        self.published.append(
            {"source": source_table_name, "online": online_table_name, "mode": publish_mode}
        )
        return {"source": source_table_name}

    def delete_online_store(self, *, name: str) -> None:
        # The real client raises rather than no-opping (read from 0.17.1's own
        # source) -- if the fake silently succeeded, the idempotency test below
        # would pass against a wrapper that does nothing.
        if name not in self._live:
            raise NotFound(f"Online store with name '{name}' not found")
        self._live.remove(name)
        self.deleted.append(name)


def _feature_table(spark: SparkSession, tmp_path: Path, *, cdf: bool) -> str:
    """A real local Delta table, registered, optionally with CDF enabled the
    same way `run_features --register` enables it."""
    path = str(tmp_path / "repo_activity")
    spark.createDataFrame(
        [Row(repo_id=1, event_time="2025-11-03T14:00:00", events_prior_1h=3)]
    ).write.format("delta").mode("overwrite").save(path)
    # A schema unique to this test: the fixture's metastore is one session-wide
    # Derby instance, so a reused schema.table would keep a stale LOCATION.
    schema = f"online_{tmp_path.name}"
    register_feature_table(spark, table="repo_activity", path=path, schema=schema)
    if cdf:
        spark.sql(change_data_feed_sql(schema=schema, table="repo_activity"))
    return f"{schema}.repo_activity"


def test_the_real_client_still_matches_the_protocol() -> None:
    """`_FakeClient` is only worth anything if the real client agrees with it.
    The dependency floor is `>=0.17.1` with no ceiling, so a later release could
    move these signatures out from under `OnlineStoreClient` and every test here
    would still pass. Introspection only -- no client is constructed, so this
    needs no credentials and makes no call.
    """
    expected_keywords = {
        "create_online_store": {"name", "capacity"},
        "publish_table": {
            "online_store",
            "source_table_name",
            "online_table_name",
            "publish_mode",
        },
        "get_online_store": {"name"},
        "delete_online_store": {"name"},
    }

    for method, keywords in expected_keywords.items():
        params = inspect.signature(getattr(FeatureEngineeringClient, method)).parameters
        keyword_only = {n for n, p in params.items() if p.kind is p.KEYWORD_ONLY}
        assert keywords <= keyword_only, f"{method} dropped {keywords - keyword_only}"


def test_require_store_refuses_to_invent_a_store_that_does_not_exist() -> None:
    """Terraform owns the instance's lifecycle. A wrapper that quietly created
    one on a name typo would bill for it -- Lakebase does not scale to zero.
    """
    client = _FakeClient()

    with pytest.raises(ValueError, match="does not exist"):
        require_store(client, name="almanac-online")

    assert client.created == []


def test_require_store_returns_the_store_once_it_exists() -> None:
    client = _FakeClient()
    create_store(client, name="almanac-online")

    assert require_store(client, name="almanac-online") == {"name": "almanac-online"}


def test_capacity_defaults_to_smallest() -> None:
    client = _FakeClient()

    create_store(client, name="almanac-online")

    assert client.created == [("almanac-online", "CU_1")]


def test_capacity_is_still_caller_controlled() -> None:
    client = _FakeClient()

    create_store(client, name="almanac-online", capacity="CU_4")

    assert client.created == [("almanac-online", "CU_4")]


def test_delete_is_idempotent() -> None:
    """A teardown script gets re-run; the real client raises NotFound the
    second time."""
    client = _FakeClient()
    store_name = "almanac-online"
    create_store(client, name=store_name)

    delete_store(client, name=store_name)
    delete_store(client, name=store_name)

    assert client.deleted == [store_name]


@pytest.mark.spark
def test_publish_requires_cdf_source(spark: SparkSession, tmp_path: Path) -> None:
    client = _FakeClient()
    source = _feature_table(spark, tmp_path, cdf=False)
    store = create_store(client, name="almanac-online")

    with pytest.raises(ValueError, match="enableChangeDataFeed"):
        publish_feature_table(client, spark, store=store, source=source, online="online.repos")

    assert client.published == []


@pytest.mark.spark
def test_publish_proceeds_once_cdf_is_enabled(spark: SparkSession, tmp_path: Path) -> None:
    client = _FakeClient()
    source = _feature_table(spark, tmp_path, cdf=True)
    store = create_store(client, name="almanac-online")

    publish_feature_table(client, spark, store=store, source=source, online="online.repos")

    assert client.published == [{"source": source, "online": "online.repos", "mode": "TRIGGERED"}]


@pytest.mark.spark
def test_snapshot_mode_does_not_require_cdf(spark: SparkSession, tmp_path: Path) -> None:
    """SNAPSHOT is a one-time full copy and reads no feed, so requiring CDF for
    it would refuse a publish the service accepts."""
    client = _FakeClient()
    source = _feature_table(spark, tmp_path, cdf=False)
    store = create_store(client, name="almanac-online")

    publish_feature_table(
        client, spark, store=store, source=source, online="online.repos", mode="SNAPSHOT"
    )

    assert client.published == [{"source": source, "online": "online.repos", "mode": "SNAPSHOT"}]
