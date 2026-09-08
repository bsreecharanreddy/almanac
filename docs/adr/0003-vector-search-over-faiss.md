# ADR-0003: Databricks Vector Search over a self-hosted FAISS index

**Status:** Accepted 2026-09-04. Endpoint since torn down (Phase 5 exit),
decision unchanged.

## Context

Phase 5 treats retrieval as **feature infrastructure**, not a chatbot —
its success metric is downstream model lift, never citation
groundedness. §8.3's decision rule required the real corpus size and the
real SKU rate to be measured *before* choosing an index.

## What was measured first

The corpus, against the real 92-day Bronze table rather than a sample:

| | Opened events | With non-null title + body |
|---|---|---|
| PRs | 13,179,648 | **10,162,741** |
| Issues | 5,161,766 | **4,761,832** |
| **Candidate texts** | | **14,924,573** |

This **supersedes §8.3's "~4.6M"**, which was a 30-day, one-hour-rate
projection made before Tier 3 ever ran — the real quarter carries ~3.2x
that. Restated per the design-decision gate: the old number was not
mis-measured, it was extrapolated, which is a different failure.

## Decision

Databricks Vector Search, `STANDARD` endpoint, `DELTA_SYNC` /
`TRIGGERED` / `HYBRID`, sized for the real corpus.

## Alternatives considered

**A self-hosted FAISS index.** Not rejected on preference — §8.3a's
fallback clause was deliberately written to fire on *a real measured
problem*, not on cost being merely unknown in advance. Region
availability and account entitlement were both live positive signals, so
the cost half was answered by an actual `terraform apply` checked against
billing, rather than assumed favourable because nothing had ruled it out.

## Consequences

**Vector Search does not scale to zero, and that had to be measured to be
believed.** The endpoint drew a flat **4.000 DBU/hour, every hour** —
identical in the window where ~12,400 real queries ran and in windows
where nothing touched it. That is **96 DBU/day ≈ $6.72/day ≈ $202/month
idle**.

The trap worth carrying forward: the Phase 4 model-serving endpoint bills
under the **same SKU** and drew DBUs only during its live window, then
zero. So "serverless" says nothing about idle cost here — two resources on
one SKU, opposite behaviours.

Torn down accordingly. Recreation is `terraform apply` plus one sync: the
1,859,551-row embeddings Delta table survives, so it is never a re-embed.

**Evidence:** `docs/findings/2026-09-04-phase-5-corpus-and-index-choice.md`,
`docs/findings/2026-09-06-vector-search-live-state-and-teardown.md`.
