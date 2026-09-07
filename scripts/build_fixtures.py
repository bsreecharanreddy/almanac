"""Build committed test fixtures by sub-sampling real archive hours.

``make fixtures``, never CI. Every Nth line, not a random sample, so a
fixture is reproducible from its source hour.
"""

import gzip
import sys
from datetime import date
from pathlib import Path

import httpx

from almanac.config import Settings
from almanac.extract.archive import fetch_hour
from almanac.extract.outcome import FetchStatus

TARGET_EVENTS = 2_000
MAX_FIXTURE_MB = 5.0
SAMPLES: list[tuple[str, date, int]] = [
    ("modern", date(2025, 8, 13), 14),
    ("legacy", date(2014, 6, 12), 14),
    # Post-2025-10-15 payload reduction (SchemaEra.REDUCED_V3) -- Task 4's
    # replay harness needs real reduced-era data, and neither prior sample
    # predates the boundary far enough: this one is the only committed
    # fixture streaming's own era guard can accept.
    ("reduced", date(2025, 11, 3), 14),
]


def build(name: str, day: date, hour: int, settings: Settings) -> Path:
    with httpx.Client(follow_redirects=True) as client:
        result = fetch_hour(
            day,
            hour,
            client=client,
            dest_dir=settings.data_dir / "raw",
            settings=settings,
        )
    if result.status is not FetchStatus.OK or result.path is None:
        raise RuntimeError(f"{name}: fetch returned {result.status}")

    with gzip.open(result.path, "rt", encoding="utf-8") as fh:
        lines = fh.readlines()

    stride = max(1, len(lines) // TARGET_EVENTS)
    sampled = lines[::stride][:TARGET_EVENTS]

    out = settings.fixture_dir / f"{name}-{day:%Y-%m-%d}-{hour}.jsonl.gz"
    out.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(out, "wt", encoding="utf-8") as fh:
        fh.writelines(sampled)

    size_mb = out.stat().st_size / 1_048_576
    print(f"{name}: {len(lines)} events -> {len(sampled)} sampled, {size_mb:.2f} MB")
    if size_mb > MAX_FIXTURE_MB:
        raise RuntimeError(f"{out} is {size_mb:.1f} MB; keep fixtures under {MAX_FIXTURE_MB} MB")
    return out


def main() -> int:
    settings = Settings()
    for name, day, hour in SAMPLES:
        build(name, day, hour, settings)
    return 0


if __name__ == "__main__":
    sys.exit(main())
