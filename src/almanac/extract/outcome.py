"""The vocabulary for how a fetch turned out.

Distinguishing ABSENT from EMPTY from FAILED is a design requirement, not
a nicety (design doc §12, trap 5): the collector has genuinely failed at
points across fourteen years, and an ingestion layer that cannot tell
"this hour was never published" from "the download broke" cannot report
completeness honestly.
"""

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class FetchStatus(StrEnum):
    OK = "ok"
    ABSENT = "absent"  # 404 -- the hour was never published. Not an error.
    EMPTY = "empty"  # published, zero bytes. A real collector gap.
    FAILED = "failed"  # transport/5xx. Retryable; may succeed later.


@dataclass(frozen=True)
class FetchResult:
    url: str
    status: FetchStatus
    path: Path | None
    bytes_downloaded: int
    attempts: int
    error: str | None = None
