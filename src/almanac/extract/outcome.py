"""How a fetch turned out. ABSENT/EMPTY/FAILED are distinct by design (§12
trap 5): completeness reporting needs "never published" apart from "download
broke"."""

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class FetchStatus(StrEnum):
    OK = "ok"
    ABSENT = "absent"  # 404 -- never published, not an error
    EMPTY = "empty"  # published, zero bytes -- a real collector gap
    FAILED = "failed"  # transport/5xx -- retryable


@dataclass(frozen=True)
class FetchResult:
    url: str
    status: FetchStatus
    path: Path | None
    bytes_downloaded: int
    attempts: int
    error: str | None = None
