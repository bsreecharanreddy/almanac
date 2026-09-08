# Limitations

Every objection a careful reader would raise, written down before they
raise it. Nothing here is hedging: each item is either a measured
property of the data, a bounded choice with its reason, or a defect this
project found in itself and has not yet fixed.

Numbers are measured. Where something is an estimate or a single
observation, it says so and gives the `n`.

---

## 1. The data is GitHub's public firehose, and it lies in specific ways

These are properties of GH Archive, not of this pipeline. Each one is a
place a plausible-looking metric would be wrong.

| Trap | What breaks if you miss it |
|---|---|
| **`WatchEvent` means *star*, not watch.** GitHub renamed the feature in 2012 and never renamed the event. | Actual watching is not in this dataset at all. Any "watchers" metric is a fabrication. |
| **There are no un-star or un-fork events.** | The only honest metric is "stars **gained**". "Total stars" cannot be derived, and labelling it that way is a factual error, not a rounding one. |
| **`PushEvent.payload.commits` is capped at 20.** | `size(commits)` silently under-counts large pushes. Use `payload.size` / `distinct_size` — and note `size` is inflated by force-pushes, so `distinct_size` is the safer of the two. |
| **Duplicate event ids cross hour-file boundaries.** | Deduplication is functionally necessary, not decorative. Measured at last on the real quarter: **62 duplicates in 341,060,851 rows** — 1 in 5.5M, across 2,208 consecutive files. Rare, but not zero, and Phase 0's sample could not have found it because it deliberately used non-adjacent hours. |
| **gzip is not splittable.** | Parallelism is bounded by *file count*, not file size. This is why Bronze is 63.8% of execution time and single-threaded on decompression. |
| **Deleted users and repos** appear as nulls or placeholders in later events. | A join that assumes referential integrity across time will drop or mis-attribute rows. |
| **`language` is absent from most modern events**, nested in PR payloads only. | And the reverse holds for legacy: `repository.language` is populated on **1,712 / 2,000 (85.6%)** of legacy events. Pre-2015 language coverage is *better*, not worse — the opposite of the intuition. |

## 2. There are three schema eras, and one of them is undocumented

**The 2015 break** is worse than "a rename":

- `repository` → `repo`, as expected
- **`actor` is a bare string in legacy, an object in modern** — a type
  change, not a rename. Legacy carries detail in `actor_attributes` and
  has **no numeric actor id at all**, so cross-era actor identity rests
  on a mutable login.
- **Legacy events have no `id` field — 0 of 2,000.** Event identity has
  to be synthesised, which is why `event_id_source` exists as a column.
- **Legacy `created_at` carries a `-07:00` offset, not `Z`** — 2,000 of
  2,000.
- `repo_id` **is** stable across both eras (1,997/2,000 legacy), so the
  SCD2 natural key survives.

**The 2025-10 break is not documented anywhere upstream.** Between
**2025-10-08 and 2025-10-15**, `payload.pull_request` was cut from **48
fields to 5**, losing `merged`, `user`, `draft`, `created_at`, `title`,
`body` and every size field.

**What collapsed is pull-request activity specifically**, not the
firehose: roughly **25–50× fewer PR events** and **~30–40× fewer opened
PRs** (≈130–265/hr against 6,618/hr), with reviews falling comparably.
**Total volume is essentially unchanged** — ~155–162K events/hour in 2026
against 167K in 2025, with push and create events continuing normally.

**The consequence for anyone extending this project:** the primary label
survives only because facts are built *event-natively* rather than from
the PR payload. A reimplementation that reads `payload.pull_request.merged`
will work on 2025 data and silently return nulls on 2026 data.

**A correction worth carrying, because this document found it.** An
earlier claim said total volume fell **~76%**, generalised from a single
hour (`2026-08-28-14`, 54,232 events). Four more hours falsified it. Two
of the six sampled hours — 54,232 and 3,511 — turn out to be **truncated
captures**, i.e. a live instance of trap 5 above, which is why any single
hour is a bad basis for a claim.

That correction was written into the finding on **2026-09-01, the same
day**. It was never copied into §12 of the design doc, so the
authoritative traps list carried a retracted number for a week — found
only while writing this file, which exists to state §12 plainly and could
not do so while §12 disagreed with its own source. §12 is now corrected
in place and marked. **Two places recording one fact is two records, and
one of them goes stale**; this is the third time that shape has been
caught in this repo.

## 3. Repos get renamed, and the labels move with them

**5,757 renames** across 1,234,736 distinct repos in a 24-hour sample
spanning the candidate quarter — ownership transfers, user renames,
project renames, typo fixes, and at least one repo renamed **twice**
inside the window.

`repo.id` is stable; `repo.name` is not. Any dimension keyed on name is
wrong. This is why `dim_repo` is SCD2 and why every comparison in it uses
null-safe equality (`<=>`) — plain `<>` misses null-to-value transitions,
which is exactly what a newly populated `language` column looks like.

## 4. The label is a duration, and most rows do not have one

Time-to-first-response is undefined for most pull requests, for four
distinct reasons that must not be collapsed into one:

| `label_exclusion` | Rows | Why it has no label |
|---|---|---|
| `author_unobserved` | 7,055,396 | `author_login` is null, so "first response by someone **other than** the author" cannot be evaluated |
| `right_censored` | 5,255,463 | Open, no response **yet** — the response may still arrive |
| `closed_no_response` | 4,363,603 | Closed having never received a non-author response — a real outcome, but not a *latency* |
| `open_unobserved` | — | `opened_at` is null; the duration has no start point |

**The distinction that matters is `right_censored` vs
`closed_no_response`.** The first is a measurement in progress; the
second is a finished one with no duration. Collapsing them corrupts the
trainable population.

**The naive move was checked and was wrong for 74% of the category.**
Labelling every `closed_no_response` row a breach would have overstated
the positive class by 3.2M rows: of 4,363,603, only **1,130,833** were
open ≥1,487 s before closing. The other 3,232,770 closed *faster* than
the SLA threshold, so the window never had a chance to be exceeded.

Trainable population: **7,320,121** rows. Measured breach rate **25.55%**
overall, and **bots breach more than humans — 32.16% vs 21.97%**, which
is the opposite of the pre-measurement intuition and is stated here
because it corrected an assumption.

## 5. Bots dominate, and the obvious classifier is badly wrong

**29.0% of events carry the `[bot]` suffix alone**, and the share is
**day-of-week dependent** — 18.2% on a Saturday hour against 29.0% across
three Wednesdays — because scheduled automation runs on weekday cadences
and humans do not. Any unclassified metric is misleading, and any metric
compared across days of the week is misleading in a *varying* way.

**The heuristic needed fixing, and the documented failure modes were the
wrong ones.** A bare `ci$` clause matched 1,002 distinct logins of which
**869 (86.7%) were human surnames** — Turkish `Yazici`, `Akinci`, `Avci`;
Italian `Federici`, `Falcucci`. The fix requires a separator.

**The methodological lesson generalises beyond this project:** inspecting
top-N matches *by volume* is structurally blind to this, because bots are
high-volume by definition. Error rates for a rule over a power-law
population must be computed **per distinct entity**, not per event.

**Accepted recall cost, pinned by a test:** logins that *embed* a CI
service rather than suffixing it — `cw-circleci`, `seek-oss-circleci` —
are now missed, because "circleci" has an `e` before the final `ci`, not
a separator. Recoverable via a curated list if it ever matters. Recorded
rather than papered over.

## 6. The live feed captures ~7%, and that is a choice

The streaming poller captures **~7.1–7.4%** of the public event stream —
measured over **n = 30 polls across ~50 minutes**, and reproduced
independently on a later re-run (n=2, 7.1–7.5%). This supersedes an
earlier **~11%** figure that came from a single-poll probe.

**The measurement cannot count what was missed, and says so.** You cannot
observe what you never received. The capture fraction is a ratio of two
*independently sourced* rates — polled here, archive-measured in §12 — so
it inherits the archive figure's uncertainty rather than being a direct
count.

**The sharper finding is the overlap: zero, on all 30 polls, without
exception.** Not one event id survived from any poll to the next. The
retrievable window turns over completely inside 60 seconds, so everything
produced between two polls is unrecoverable. **The API exposes a sample,
not a firehose** — which is a property of the source, not of the poller.

**This is a politeness ceiling, not a rate limit, and the distinction is
the whole point.** At one poll per minute the poller uses **~180
requests/hour — under 4% of the authenticated budget**. Polling faster
would stay comfortably inside the rate limit and capture substantially
more. The ceiling is GitHub's requested `x-poll-interval` header, which
this project honours.

A demo that ignored a documented politeness header would be demonstrating
the wrong thing. That is a defensible engineering position, not a
limitation of the code — but it *is* the direct cause of the 7% number,
so it is stated rather than left implicit.

**A second live-feed limitation:** because the current firehose era is
`REDUCED_V3`, the payload no longer carries the fields the label needs.
**Live label computation is not possible** on the streaming path. The
streaming demonstration is about exactly-once, watermarks, late arrival
and online serving — not about producing fresh labels.

## 7. What this platform cannot currently do

Open defects and gaps, each found and recorded rather than discovered by
a reader.

**The served endpoint returns a class, not a probability.** `train.py`
logs the champion with a signature inferred from `model.predict()`, so
every row in the inference table is a boolean. Ranking an intervention
queue needs a score, so `almanac.model.score` produces one **offline**
from the same registered champion. Offline and online therefore return
different *types* for the same model — a training/serving skew of the
most basic kind. The fix is re-logging the signature; it is deferred
because it would invalidate the inference table, which is the one
artifact here that cannot be re-derived at any price (only traffic after
capture was switched on can ever be logged).

**The feature spine multiplies pull requests that were opened twice.**
25 PRs in the firehose carry two distinct `PullRequestEvent`/`opened`
events with different `event_id`s. Silver's dedup is correct to keep both
— they are different events — but `build_pr_opened_spine` and
`compute_pr_static` each emit one row per *event* rather than per PR, so
their equi-join produces 4 rows per affected PR. The scored table holds
**7,320,196 rows against 7,320,121 distinct keys: 75 duplicates,
0.001%**, and it violates its own declared key. The registered champion
was trained through the identical builder, so those 25 PRs carried 4×
weight. **This is not a leakage bug** — every duplicate row's features
still precede its own `as_of`. Unfixed; it needs its own failing test, a
re-score, and a decision about retraining.

**Two dashboard panels have no recoverable source.** Feature-freshness
lag and training/serving skew both require Phase 6's online feature
store, which was torn down at a measured idle rate of $12.06/day once its
exit gate was demonstrated. The panels ship **visibly marked as
unavailable** rather than dropped, because an absent panel a reader
cannot see is indistinguishable from a panel nobody thought of.

**The job-runs panel shows `result_state` as `null` for some runs.**
`system.lakeflow.job_run_timeline` emits one row per *period*, so a
multi-period run has no terminal state on every row. The panel renders
raw timeline rows and should aggregate per `run_id`. It is not wrong, but
a reader would reasonably misread `null` as "failed to record".

**The AI/BI widget schema is undocumented and unvalidated by the API.**
The REST API accepts an invalid widget spec without complaint; the
failure appears only when a human opens the page. A regression test now
pins the known-good shape, but there is no schema to validate against.

**`terraform apply` does not lock a dashboard.** An open browser editor
is a second writer, and last write wins — a stale editor tab overwrote
correct deployed config twice, with the apply reporting success both
times.

## 8. What the lineage graph cannot see

Stated on the artifact itself as well, because a lineage graph reads as
complete unless it says otherwise.

1. **Local Spark runs are invisible.** Unity Catalog records lineage for
   work executed on Databricks; this repo's entire test suite runs
   locally and contributes nothing. **An edge's absence is not evidence
   it does not exist in code.**
2. **A rolling one-year retention window.** This artifact is the cheap
   view, not the archival one.
3. **Columns written from a literal produce no edge at all** — lineage is
   emitted only where it can be inferred.
4. **Phase 2's Photon A/B schemas are excluded on purpose**, so four
   experiment tiers are not presented as part of the platform.
5. **`external` is a residual bucket, not a place.** An `x -> external`
   edge says only that something was written somewhere the taxonomy does
   not name. Do not read it as an export.

A sixth, found by the graph on its first real run: **CLAUDE.md states
§3.1 absolutely** ("never a read of `fact_pull_request`") **while the
built system carries one deliberate, documented exception** —
`similar_prior_breach_rate` needs prior PRs' labels, and the label lives
in Gold. A gap between a stated rule and reality, which is exactly the
job a lineage artifact exists to do.

## 9. Scope and scale ceilings, each with its upgrade path

None of these are limits of the design; they are bounded choices made
against a real credit and a real deadline.

| Ceiling | Why | Upgrade path |
|---|---|---|
| **One quarter — Q3 2025, 92 days, 341,060,851 rows** | The full medallion ran for $11.96, Gold over it for $0.78 | More days. The runner is date-parameterised and checkpointed per day. |
| **The embedding index is scoped** to ~1.73M texts via `--since-date 2025-09-20`, against **14,924,573** real candidates | Every Phase 5 capability demonstrates identically at that size | One parameter and a bigger cluster |
| **Coverage is measured on transformation and feature logic only** — 88%, gated at 85 | §10 asks for a scoped figure; quoting a flattering repo-wide number would be the dishonest version | — |
| **The cloud stack is torn down between sessions** | Vector Search does not scale to zero (4.000 DBU/hour flat, ≈$6.72/day); Lakebase idles at $12.06/day | `terraform apply`. Recreation never re-embeds — the embeddings Delta table survives. |

## 10. Cost accounting is more than DBUs

Worth stating because it is a common mis-estimate: **Databricks DBUs are
only 53% of real spend** on this project.

| Component | Spend | Share |
|---|---|---|
| Azure Databricks (DBUs) | $65.19 | 53% |
| Virtual Machines | $30.65 | 25% |
| NAT Gateway | $14.97 | 12% |
| Storage | $11.05 | 9% |
| Virtual Network | $0.54 | <1% |

A cost model built from the DBU meter alone would understate this
project's bill by roughly half. The NAT Gateway line in particular is
pure egress overhead that no compute-sizing exercise would predict.

**One measurement constraint that shapes every cost number here:**
`system.billing.usage` lags roughly a day and **cannot be queried for the
window it is measuring**. Every cost figure in this repo was therefore
read *after* the fact, which is why some are reported a day late rather
than at the end of the run that produced them.
