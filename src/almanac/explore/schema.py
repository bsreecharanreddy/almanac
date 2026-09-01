"""Schema-era classification and field diffing. Pure; no I/O."""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

# The 2015-01-01 boundary is the design doc's stated hypothesis (§12,
# trap 7). This module's findings document is where it gets confirmed or
# corrected against real data.
ERA_BOUNDARY = datetime(2015, 1, 1, tzinfo=UTC)


class SchemaEra(StrEnum):
    LEGACY_V1 = "legacy_v1"
    MODERN_V2 = "modern_v2"


def era_for(created_at: datetime) -> SchemaEra:
    return SchemaEra.MODERN_V2 if created_at >= ERA_BOUNDARY else SchemaEra.LEGACY_V1


def field_paths(event: dict[str, Any]) -> set[str]:
    """Dotted paths, two levels deep.

    Two levels is deliberate: deeper paths explode combinatorially across
    payload variants and bury the structural differences this diff exists
    to surface.
    """
    paths: set[str] = set()
    for key, value in event.items():
        if isinstance(value, dict):
            paths.update(f"{key}.{sub}" for sub in value)
        else:
            paths.add(key)
    return paths


def diff_field_paths(a: set[str], b: set[str]) -> tuple[set[str], set[str], set[str]]:
    return a - b, b - a, a & b
