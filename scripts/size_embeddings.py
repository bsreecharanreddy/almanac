"""Measure embedding throughput and project it to Tier 4 scale -- a sizing
spike that decides Phase 5's scope.

    uv run --with sentence-transformers python scripts/size_embeddings.py
"""

import gzip
import json
import sys
import time

from almanac.config import Settings

MODEL = "sentence-transformers/all-MiniLM-L6-v2"
MAX_CHARS = 2_000
# Measured 2026-09-01, design doc §5.1. Used only for projection.
PRS_PER_HOUR = 6_352


def collect_texts(settings: Settings) -> list[str]:
    """PR titles+bodies from a full downloaded hour if present, else the fixture."""
    raw = sorted((settings.data_dir / "raw").glob("2025-*.json.gz"))
    source = raw[0] if raw else sorted(settings.fixture_dir.glob("modern-*.jsonl.gz"))[0]
    print(f"source: {source.name}")

    texts: list[str] = []
    with gzip.open(source, "rt", encoding="utf-8") as fh:
        for line in fh:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            payload = event.get("payload") or {}
            pr = payload.get("pull_request") or {}
            issue = payload.get("issue") or {}
            body = pr or issue
            text = " ".join(filter(None, [body.get("title"), body.get("body")]))
            if text.strip():
                texts.append(text[:MAX_CHARS])
    return texts


def main() -> int:
    # In-function: sentence-transformers is injected via `uv run --with`, not
    # a project dependency, so a top-level import would break collection and mypy.
    from sentence_transformers import SentenceTransformer  # noqa: PLC0415

    settings = Settings()
    texts = collect_texts(settings)
    if not texts:
        print("no PR/issue text found; widen the sample before sizing")
        return 1

    model = SentenceTransformer(MODEL)
    model.encode(texts[:32])  # warm up; excluded from timing

    start = time.perf_counter()
    vectors = model.encode(texts, batch_size=64, show_progress_bar=False)
    elapsed = time.perf_counter() - start
    per_sec = len(texts) / elapsed

    print(f"model            : {MODEL}")
    print(f"texts embedded   : {len(texts):,}")
    print(f"dimensions       : {vectors.shape[1]}")
    print(f"mean chars       : {sum(map(len, texts)) / len(texts):.0f}")
    print(f"elapsed          : {elapsed:.1f}s")
    print(f"THROUGHPUT       : {per_sec:.1f} texts/sec")
    print()
    print("projected to a 30-day Tier 4 slice:")
    for pct in (5, 25, 100):
        prs = PRS_PER_HOUR * 24 * 30 * pct / 100
        hours = prs / per_sec / 3600
        print(f"  {pct:3d}% repo sample: {prs:>12,.0f} texts -> {hours:7.1f} h CPU")
    return 0


if __name__ == "__main__":
    sys.exit(main())
