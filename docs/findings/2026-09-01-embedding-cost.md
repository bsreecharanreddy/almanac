# Findings — embedding throughput and Phase 5 scope

**Date:** 2026-09-01
**Model:** `sentence-transformers/all-MiniLM-L6-v2` (384 dimensions)
**Hardware:** local CPU, Apple Silicon
**Corpus:** 25,878 PR and issue titles+bodies from one real hour
(`2025-01-08-0`), mean 752 characters, truncated at 2,000

| | |
|---|---|
| Elapsed | 171.3 s |
| **Throughput** | **151.1 texts/sec** |

## Projection to a 30-day Tier 4 slice

Using the measured 6,352 PRs/hour from design doc §5.1:

| Repo sample | Texts | CPU time |
|---|---|---|
| **5%** (planned) | 228,672 | **0.4 h** |
| 25% | 1,143,360 | 2.1 h |
| 100% | 4,573,440 | 8.4 h |

## Decision

**Phase 5 proceeds as designed, embedding locally on CPU.** At the planned
5% repo sample the whole slice embeds in under half an hour — an order of
magnitude below the ~4 h threshold that would have forced a redesign. No
GPU, no paid embedding API, no narrowing of scope.

There is also comfortable headroom: even a **100% sample** finishes in 8.4
hours of CPU, so raising the sample rate later is a scheduling question
rather than an architectural one.

## Caveats

- Single-threaded, single-process, one model. Batching or multiprocessing
  would improve on this; no tuning was attempted because the answer was
  already decisive.
- Throughput is text-length dependent. Mean here is 752 characters; a
  corpus of longer bodies would be slower.
- **The corpus itself is era-bound.** PR `title` and `body` do not exist
  after 2025-10-15 (see `2026-09-01-third-schema-era.md`), so this
  measurement, and Phase 5 generally, applies only to modern-era data.
