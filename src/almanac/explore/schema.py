"""Schema-era classification and field diffing. Pure; no I/O."""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

# Two measured era boundaries, not one.
#
# 2015-01-01 was the design doc's stated hypothesis (§12 trap 7) and is
# confirmed. The second was found by measurement on 2026-09-01 and is
# documented nowhere upstream: between 2025-10-08 and 2025-10-15 the
# `payload.pull_request` object was cut from 48 fields to 5, removing
# `merged`, `user`, `draft`, `created_at`, `title`, `body`, and every size
# field. See docs/findings/2026-09-01-third-schema-era.md.
#
# The boundary is placed at 2025-10-15, the first date observed reduced.
# It is known only to within a week; sampling was one hour per date.
ERA_BOUNDARY_MODERN = datetime(2015, 1, 1, tzinfo=UTC)
ERA_BOUNDARY_REDUCED = datetime(2025, 10, 15, tzinfo=UTC)


class SchemaEra(StrEnum):
    LEGACY_V1 = "legacy_v1"
    MODERN_V2 = "modern_v2"
    REDUCED_V3 = "reduced_v3"


def era_for(created_at: datetime) -> SchemaEra:
    """Which schema era an event belongs to, by its event time.

    `REDUCED_V3` events are ingestible but **not modelable**: they carry no
    merge outcome, no PR author, and no PR text, so the label defined in
    design doc §5.1 cannot be computed for them at all.
    """
    if created_at >= ERA_BOUNDARY_REDUCED:
        return SchemaEra.REDUCED_V3
    if created_at >= ERA_BOUNDARY_MODERN:
        return SchemaEra.MODERN_V2
    return SchemaEra.LEGACY_V1


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
