"""Schema-era classification and field diffing. Pure; no I/O."""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

# Two measured boundaries. 2015-01-01 is §12 trap 7, confirmed. The reduced
# era (payload.pull_request cut 48 -> 5 fields) was found by measurement,
# placed at the first date observed reduced, known to within a week
# (docs/findings/2026-09-01-third-schema-era.md).
ERA_BOUNDARY_MODERN = datetime(2015, 1, 1, tzinfo=UTC)
ERA_BOUNDARY_REDUCED = datetime(2025, 10, 15, tzinfo=UTC)


class SchemaEra(StrEnum):
    LEGACY_V1 = "legacy_v1"
    MODERN_V2 = "modern_v2"
    REDUCED_V3 = "reduced_v3"


def era_for(created_at: datetime) -> SchemaEra:
    """Which schema era an event belongs to, by its event time.

    ``REDUCED_V3`` events are ingestible but not modelable -- no merge
    outcome, no PR author, no PR text, so §5.1's label is uncomputable.
    """
    if created_at >= ERA_BOUNDARY_REDUCED:
        return SchemaEra.REDUCED_V3
    if created_at >= ERA_BOUNDARY_MODERN:
        return SchemaEra.MODERN_V2
    return SchemaEra.LEGACY_V1


def field_paths(event: dict[str, Any]) -> set[str]:
    """Dotted paths, two levels deep -- deeper explodes across payload variants."""
    paths: set[str] = set()
    for key, value in event.items():
        if isinstance(value, dict):
            paths.update(f"{key}.{sub}" for sub in value)
        else:
            paths.add(key)
    return paths


def diff_field_paths(a: set[str], b: set[str]) -> tuple[set[str], set[str], set[str]]:
    return a - b, b - a, a & b
