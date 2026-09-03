"""Everything a burn run needs besides the day it is processing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Self

import httpx

from almanac.config import Settings
from almanac.pipeline.source import SourceConfig


@dataclass(frozen=True)
class LakePaths:
    # bronze/silver are strings, not Path: on the cluster they are abfss:// URIs.
    bronze: str
    silver: str
    staging: Path

    @classmethod
    def under(cls, root: Path) -> Self:
        """Conventional layout under one root, for local runs and tests."""
        return cls(
            bronze=str(root / "bronze"),
            silver=str(root / "silver"),
            staging=root / "staging",
        )


@dataclass(frozen=True)
class BurnContext:
    paths: LakePaths
    config: SourceConfig
    client: httpx.Client
    settings: Settings
    ingested_at: datetime | None = None
