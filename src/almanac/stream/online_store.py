"""Lakebase online store: create, publish, and the teardown that ships with them."""

from __future__ import annotations

import contextlib
from typing import Any, Literal, Protocol, cast

from databricks.feature_engineering import FeatureEngineeringClient
from databricks.sdk.errors import NotFound
from pyspark.sql import SparkSession

Capacity = Literal["CU_1", "CU_2", "CU_4", "CU_8"]
PublishMode = Literal["TRIGGERED", "CONTINUOUS", "SNAPSHOT"]

# The vendor's DatabricksOnlineStore, and publish_table's
# StreamingQuery | PublishedTable | None. Opaque on purpose: this module
# hands the store handle straight back to publish_table and never reads a
# field off either, so naming the real types would buy nothing and tie this
# signature to an untyped package.
OnlineStore = Any
PublishResult = Any

CHANGE_DATA_FEED_PROPERTY = "delta.enableChangeDataFeed"

# CDF is a prerequisite for these two modes only -- SNAPSHOT is a one-time
# full copy and reads no feed (Databricks online-feature-store docs, checked
# live 2026-09-06).
_MODES_NEEDING_CDF: frozenset[str] = frozenset({"TRIGGERED", "CONTINUOUS"})


class OnlineStoreClient(Protocol):
    """The three FeatureEngineeringClient methods this module calls. All are
    keyword-only in the real client (0.17.1, introspected), so a test double
    that accepts positionals would pass here and fail against the service.
    """

    def create_online_store(self, *, name: str, capacity: str) -> OnlineStore: ...

    # Returns None rather than raising when the store is absent (0.17.1, read
    # from its source) -- the opposite of delete_online_store below, so the
    # two cannot share one error convention.
    def get_online_store(self, *, name: str) -> OnlineStore | None: ...

    def publish_table(
        self,
        *,
        online_store: OnlineStore,
        source_table_name: str,
        online_table_name: str,
        publish_mode: str,
    ) -> PublishResult: ...

    def delete_online_store(self, *, name: str) -> None: ...


def load_client() -> OnlineStoreClient:
    """Credentials come from the ambient Databricks auth context -- a job's own
    token, or the CLI profile locally -- as with every other client here. The
    one untyped call in this module; everything above it is the Protocol.
    """
    return cast(OnlineStoreClient, FeatureEngineeringClient())


def create_store(
    client: OnlineStoreClient, *, name: str, capacity: Capacity = "CU_1"
) -> OnlineStore:
    """`capacity` is required by the real API; defaulting it to the smallest
    unit is a cost decision, not a stylistic one. Lakebase does not scale to
    zero (§4.6), so an oversized store bills for every idle hour. Databricks'
    own docs suggest CU_2 as a testing starting point; this project runs a
    bounded window against a finite credit, and `update_online_store` raises
    capacity later if CU_1 proves short. The service caps `name` at 63 bytes.
    """
    return client.create_online_store(name=name, capacity=capacity)


def has_change_data_feed(spark: SparkSession, table: str) -> bool:
    """Reads back the property `features.registration.change_data_feed_sql` sets."""
    rows = spark.sql(f"SHOW TBLPROPERTIES {table}").collect()
    return any(r["key"] == CHANGE_DATA_FEED_PROPERTY and r["value"] == "true" for r in rows)


def publish_feature_table(
    client: OnlineStoreClient,
    spark: SparkSession,
    *,
    store: OnlineStore,
    source: str,
    online: str,
    mode: PublishMode = "TRIGGERED",
) -> PublishResult:
    """Publish an offline feature table into `store`, refusing a source that
    cannot sync.

    Without CDF the failure surfaces inside the service, after the store has
    been created and is already billing by the hour -- so the precondition is
    checked here, against the table this repo's own `--register` path sets.
    """
    if mode in _MODES_NEEDING_CDF and not has_change_data_feed(spark, source):
        raise ValueError(
            f"{source} has no {CHANGE_DATA_FEED_PROPERTY}=true, which publish_mode"
            f" {mode} requires. Run features/runner.py --register first."
        )
    return client.publish_table(
        online_store=store,
        source_table_name=source,
        online_table_name=online,
        publish_mode=mode,
    )


def require_store(client: OnlineStoreClient, *, name: str) -> OnlineStore:
    """The store Terraform created, or a clear failure naming it.

    Look up, never create: `databricks_database_instance` in
    infra/terraform/streaming.tf owns this object's lifecycle, and a wrapper
    that quietly created a second one on a name typo would bill for it.
    """
    store = client.get_online_store(name=name)
    if store is None:
        raise ValueError(f"online store {name!r} does not exist; terraform apply creates it")
    return store


def delete_store(client: OnlineStoreClient, *, name: str) -> None:
    """Teardown ships with provisioning (§11), and is idempotent here because
    the real client is not: `delete_online_store` raises `NotFound` on a store
    that is already gone (0.17.1, read from its source), which would turn a
    re-run of a teardown script into a failure at exactly the wrong moment.

    An online store left up bills every hour whether or not anything reads it
    -- the shape Vector Search was measured at, 4 DBU/hour idle. Delete it
    when the window closes.
    """
    with contextlib.suppress(NotFound):
        client.delete_online_store(name=name)
