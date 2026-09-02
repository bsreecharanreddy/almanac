"""Per-day resume markers for the Tier 3 backfill, and which days remain."""

from __future__ import annotations

import json
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Self


def pending_days(start: date, end: date, done: Collection[date]) -> list[date]:
    """Days in [start, end] not already in done, in order."""
    if end < start:
        raise ValueError(f"end {end} must not be before start {start}")
    done_set = set(done)
    span = (end - start).days + 1
    return [d for n in range(span) if (d := start + timedelta(days=n)) not in done_set]


@dataclass(frozen=True)
class BackfillCheckpoint:
    """One JSON marker file per finished day; root is a FUSE-mounted lake path on the cluster."""

    root: Path

    def completed(self) -> set[date]:
        if not self.root.exists():
            return set()
        return {d for p in self.root.glob("*.json") if (d := _iso_or_none(p.stem)) is not None}

    def record(self, day: date, stats: Mapping[str, object]) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        target = self.root / f"{day.isoformat()}.json"
        # .part then rename: a crash never leaves a half-written marker.
        tmp = target.parent / f"{target.name}.part"
        tmp.write_text(json.dumps({"day": day.isoformat(), **stats}, indent=2, default=str))
        tmp.replace(target)

    @classmethod
    def under(cls, root: Path) -> Self:
        return cls(root=root / "checkpoints")


def _iso_or_none(stem: str) -> date | None:
    try:
        return date.fromisoformat(stem)
    except ValueError:
        return None
