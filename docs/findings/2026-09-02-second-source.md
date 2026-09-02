# Findings — the second source, and "zero new Python"

**Date:** 2026-09-02
**Question:** design doc §4.5a claims the config-driven framework onboards
a new source via **YAML alone, zero new Python**. Phase 2's gate is to
test that claim against the GitHub REST API, not assume it.
**Outcome:** the claim is **false for this source, and the shape of the
failure is the interesting part.** A second gzip-file mirror would have
passed it untouched. A paginated, authenticated, rate-limited API needed
**~123 lines of new Python** — and every one of them is Python a file
fetch structurally never has.

---

## 1. What the YAML expressed

`conf/sources/github_rest.yml` — 13 non-comment lines. It names the
detail and list URL templates, the enrichment fields, the auth scheme and
token env var, and the rate-limit budget and headers. As a *description*
of the source it is complete. Nothing in the existing framework reads it.

## 2. What the YAML could not express — the accounting

| Need | Why YAML can't | New Python |
|---|---|---|
| **The config shape itself** | `SourceConfig` is `extra="forbid"` and its fields (`format`, `partition_by`, `quality_rules`) don't describe an API. `auth`, `rate_limit`, `pagination` are new concepts. | `AuthConfig`, `RateLimitConfig`, `RestSourceConfig` — **29 lines** in `pipeline/source.py`. `test_the_file_source_config_rejects_the_rest_config` pins that the two shapes genuinely do not overlap. |
| **Fetching by `(owner, repo, number)`** | The framework's only fetch path is `archive_url(day, hour)` → one gzip file. `url_template` was a declared field that **nothing consumed**. | `RestSession` + `fetch_pr` — the request, the `Bearer` header, the enrichment extraction. |
| **Rate-limit backoff** | 5,000 req/hr with `X-RateLimit-Remaining`/`-Reset` headers and 403/429 responses. A file mirror has none of this. | `_is_exhausted`, `_reset_wait`, `_wait_if_exhausted` — wait *until the header's own reset*, never a fixed guess. `test_fetch_pr_backs_off_when_the_budget_is_exhausted`. |
| **Pagination** | The list endpoint returns 100 PRs and an RFC 8288 `Link` header; there is no "page count" to declare up front. | `_next_link` + the `while url:` loop in `list_pr_numbers`, a generator. `test_list_pr_numbers_follows_the_link_header`. |

**Total: ~123 non-comment lines** — 29 in `source.py`, ~94 in the new
`extract/rest.py` — plus a committed fixture and 6 tests. `httpx` is the
only I/O; `RestSession` carries injectable `sleep`/`now` so the two
behaviours a file fetch never needs are exercised without waiting or
reaching the network.

## 3. The honest statement for §8.2's register

> The config-driven framework onboards a **file** source (a second GH
> Archive mirror, a different bucket) by YAML alone — `quality_rules` is
> the only field the Spark path consumes and it is format-agnostic. A
> **paginated, authenticated, rate-limited API** needs ~120 lines of
> Python: a config-model extension, a REST client, rate-limit backoff and
> `Link`-header pagination. "Zero new Python" held for the case it was
> easy for and broke on the case that matters.

This is a more credible claim than an unfalsifiable "fully generic
framework", and it is the claim the code now supports.

## 4. The bound on the enrichment set — measured, not guessed

The REST API repairs what the October 2025 firehose reduction dropped
(§12 trap 12): `merged`, `draft`, `title`, `additions`/`deletions`/
`changed_files`. The model is scoped to the rich era, so this is not
training data — it is what makes the label **re-runnable on today's
data**.

From Phase 0's measurements (STATUS 2026-09-01): a 2026 hour carries PR
events at **−97%** against 2025's ~6,618 PRs opened/hour → **~200 opened
PRs/hour** on current data. At one detail request per PR against a
**5,000 req/hr** ceiling:

```
one 2026 hour of opens   ~200 requests    ~2.4 min
one 2026 day             ~4,800 requests  ~1 hr (at the ceiling)
one 2026 month           ~144,000         ~29 hr of wall clock
```

So the bound: enrich a **repo-sampled slice sized to fit one rate-limit
window**. A 5% repo sample of one week of 2026 opens is **~1,680
requests** — ~20 minutes, a third of one window's budget. That is the
"bounded repo set, chosen to fit, measured not guessed" of Step 4.

## 5. What is deliberately not done

- **Not run against live GitHub.** Every test uses `httpx.MockTransport`;
  the client has never made a real request. Running it needs a token and
  the network, and Step 5 forbids tests touching either.
- **Not landed in Delta or joined to `fact_pull_request`.** The enrichment
  is a `PrEnrichment` dataclass, not a table. Wiring it into Gold for the
  reduced/legacy eras is Phase 3+ work — the model is rich-era-only by
  design, so nothing in Phase 2 needs the join.
