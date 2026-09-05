"""§6's whole `POST /similar-prs` contract, as a plain callable -- demoed
via a script/test, never wrapped in a service. §5.2's own precedent: no
custom API service exists yet, and this phase's concrete reason is the
index, not a service (design doc §8.3a).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from databricks.ai_search.client import AISearchClient
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from almanac.features.similarity import Neighbor, SimilarityIndex, query_similar


@dataclass
class VectorSearchIndex:
    """Wraps Task 3's real databricks_vector_search_index. Deliberately not
    unit-tested beyond construction -- like embed.pipeline's load_encoder,
    the real query path is only verified against the real endpoint (Task 7).
    """

    endpoint_name: str
    index_name: str
    client: AISearchClient

    def query(self, vector: list[float], *, as_of: datetime, k: int) -> list[Neighbor]:
        index = self.client.get_index(endpoint_name=self.endpoint_name, index_name=self.index_name)
        result = index.similarity_search(
            columns=["entity_key", "event_time"],
            query_vector=vector,
            # Confirmed live 2026-09-04 (STATUS.md, design doc §8.3a): a
            # trailing operator on the filter key expresses a range
            # predicate against a TIMESTAMP metadata column.
            filters={"event_time <": as_of.isoformat()},
            num_results=k,
        )
        rows = result.get("result", {}).get("data_array", [])
        return [
            Neighbor(
                entity_key=row[0],
                event_time=datetime.fromisoformat(row[1]),
                score=row[-1],
            )
            for row in rows
        ]


def load_index(*, endpoint_name: str, index_name: str) -> VectorSearchIndex:
    """Credentials come from the ambient Databricks auth context (a job's
    own token, or the CLI profile locally) -- AISearchClient() with no
    arguments picks that up the same way every other Databricks SDK client
    in this project does."""
    return VectorSearchIndex(
        endpoint_name=endpoint_name, index_name=index_name, client=AISearchClient()
    )


def similar_prs(
    repo_id: int,
    pr_number: int,
    as_of: datetime,
    *,
    index: SimilarityIndex,
    embeddings: DataFrame,
    k: int = 10,
) -> list[dict[str, str | float]]:
    """This PR's own embedding (Task 2's table), looked up by entity_key,
    then query_similar's k point-in-time-correct nearest neighbors -- the
    whole of §6's contract, minus the HTTP layer §5.2 leaves unbuilt.
    """
    entity_key = f"pr:{repo_id}:{pr_number}"
    row = embeddings.where(F.col("entity_key") == entity_key).select("embedding").first()
    if row is None or row["embedding"] is None:
        return []
    neighbors = query_similar(index, row["embedding"], as_of.astimezone(UTC), k=k)
    return [
        {"entity_key": n.entity_key, "event_time": n.event_time.isoformat(), "score": n.score}
        for n in neighbors
    ]
