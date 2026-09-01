"""Build committed test fixtures by sub-sampling real archive hours.

Run manually via ``make fixtures``, never in CI. The output is committed;
the raw downloads are not (see .gitignore).

Sampling takes every Nth line rather than a random sample so the fixture
is reproducible from the same source hour.
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
