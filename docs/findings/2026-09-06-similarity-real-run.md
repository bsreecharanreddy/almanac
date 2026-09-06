# Findings — Task 4/6's real run: three bugs the live index surfaced

**Date:** 2026-09-06
**Source:** `databricks_job.pr_similarity` runs `705047433065245`
(crashed), `647656767930289` (ran, ~0 neighbours) and `26796189966426`
(scoped, still ~0 neighbours), plus read-only stats on
`almanac_dbx.embeddings.pr_issue_embeddings` (one measurement run in a
second session, cross-checked here) and a live 6-variant filter probe and
25-row neighbour probe against the deployed index.
**What it changes:** `extract_texts`'s issue-number path;
`_parse_entity_key` becomes total (returns `None`, never raises);
`build_similarity_spine` / the `pr_similarity` job gain `--since-date`;
`compute_pr_similarity` reads the spine exactly once;
`VectorSearchIndex` caches its index handle.

Each bug hid the next: the crash masked the empty output, and the empty
output masked the lost join. Only the third one was visible without a
real run against real data.

---

## Bug 1 — every issue embedding got a degenerate `issue:<repo_id>` key

Task 4's first real run crashed in `_parse_entity_key`:
`ValueError: not enough values to unpack (expected 3, got 2)`. The index
returned a neighbour whose `entity_key` had two segments, not three.

`extract_texts` built the key as
`concat_ws(":", event_type, $.repo.id, $.payload.number)`. `$.payload.number`
is correct for a **PullRequestEvent** (the number sits at payload top
level) but wrong for an **IssuesEvent**, which carries it at
`$.payload.issue.number`. `concat_ws` silently drops the null, so every
issue became `issue:<repo_id>`.

Measured on the 1,859,551-row table:

| kind | segments | rows | distinct keys |
|---|---|---|---|
| `issue` | 2 | 718,086 | 123,905 |
| `pr` | 3 | 1,141,465 | 1,141,458 |

`123,905 + 1,141,458 = 1,265,363` — exactly the index's
`indexed_row_count`. So the index is **1,141,458 PR vectors (complete)**
plus **123,905 issue vectors — one per repo**; **594,181 issue embeddings
are lost to the entity_key collapse**. Zero `pr:` keys are malformed
(this corpus ends 2025-09-30, before the 2025-10-08 payload cut, so every
PR event still carries `$.payload.number`).

**Fix, forward:** `extract_texts` now reads
`$.payload.{payload_key}.number` (valid for both event types) and drops
rows with a null repo id or number. **Fix, defensive:**
`_parse_entity_key` is now total — a key that isn't `<kind>:<int>:<int>`
returns `None`, and `_neighbor_rows` treats it like a non-PR neighbour
(counts toward `similar_neighbor_count`, never joins `resolutions`).

**Not fixed this session — the deployed index keeps the degenerate issue
keys.** Task 4/6 measure PR-to-PR similarity, where a PR's nearest
neighbours are overwhelmingly other PRs, so the missing issue vectors
don't change the deliverable. Recovery, when wanted, is a **re-key in
place, not a re-embed**: the 718,086 rows have intact
`text_hash` / `event_time` / `embedding` (0 nulls, 0 wrong-dimension), and
joining Bronze back on `(text_hash, event_time, repo_id)` — repo_id is
already in the degenerate key — resolves 717,353 of them (99.90%); the
733 residual are same-repo/same-text/same-timestamp duplicate issue
events. Assert the Bronze join doesn't fan out before trusting it. ≈$5
and ~2h of encode saved versus a re-embed.

**Bias direction for Task 6 (reasoning, not a measurement).** The
collapse is not neutral for the lift number. Issues are ~39% of the
table's rows but only ~10% of the index's vectors, and `_neighbor_rows`
nulls out every non-`pr` neighbour by design (issues carry no breach
outcome). So the deployed index is unusually **PR-dense** — a
correctly-keyed one would put roughly 4× more issue vectors into every
top-k, each pure crowding that displaces a PR neighbour and thins the
`F.avg` behind `similar_prior_breach_rate`. Whatever Task 6 measures
against the 0.612 champion is therefore measured on an index that
*favours* the similarity features: if the comparison wins, that caveat
belongs next to the win; if it comes back null, the null is stronger than
it looks, since even the favourable version didn't clear the bar.

## Bug 2 — the spine reached back before the embedded window

With the parser fixed, run `647656767930289` completed —
"Computed similarity for 10000 spine rows" — but the output was inert:
`avg(similar_neighbor_count) = 0.0`, one row with a non-null
`similar_prior_breach_rate`.

`build_similarity_spine` built the spine from **all** of Silver's
PR-opened events (the full Q3 quarter, ~13 M rows) and sampled 10,000.
But the index only holds PRs opened **on or after 2025-09-20**
(`embeddings_since`), and `compute_pr_similarity`'s point-in-time filter
(`event_time < as_of`) only ever returns neighbours *older* than the
spine row. A spine PR opened in July has no older neighbour in the index;
a random 10 K sample of the quarter is ~90% outside the embedded window
entirely, and the ~10% inside it can only match the thin slice opened
between 2025-09-20 and their own open date.

**Fix:** `build_similarity_spine` / `run_similarity` / the `pr_similarity`
job take `--since-date`, wired to the same `var.embeddings_since` the
embeddings job uses. The spine and the index now share one window.

## Also — index-handle caching

The latency probe (below) showed `VectorSearchIndex.query` calling
`client.get_index(...)` on every call — a metadata round-trip per spine
row. Cached to once; measured **~315 ms → ~182 ms p50** from a laptop.

## Bug 3 — the spine was read twice, and `rand` is nondeterministic

The scoped re-run (`26796189966426`, TERMINATED/SUCCESS, setup 531 s +
execution 880 s) came back **still inert**: 10,000 rows,
**44 with any neighbour** (0.44%), `avg(similar_neighbor_count) = 0.044`,
30 non-null rates. Bug 2's fix was necessary but not sufficient.

Diagnosis by elimination, not by guessing at the next plausible
component:

1. **The join is healthy.** 972,801 of 1,564,185 in-window spine rows
   (62%) match an embedding by `entity_key`, measured directly.
2. **The filter format is fine.** Probed the live index with six
   `event_time` encodings for one real vector: `isoformat()` (`+00:00`,
   what the code sends), `Z`, bare `T`, date-only and epoch-millis all
   return 10 rows and correctly exclude the self-match; only
   `'YYYY-MM-DD HH:MM:SS'` errors (`BadRequest`).
3. **The index reliably returns neighbours for this population.** 25
   random real spine rows, real embeddings, real `created_at` as `as_of`:
   **25/25 returned a full 10 neighbours.**
4. **`pairs` was therefore non-empty in the job** — independently
   confirmed by the output itself, since the empty-`pairs` branch sets
   `similar_prior_breach_rate` NULL for *every* row and the table has 30
   non-null values and a `max(similar_neighbor_count)` of 10.

So ~62,000 real neighbour pairs were computed and then lost between the
`groupBy` and the final join. `compute_pr_similarity` evaluated `spine`
**twice** — once via `.collect()` to build `pairs`, once again in
`spine.select("repo_id","pr_number").join(per_spine, ...)` — and
`run_similarity` hands it `orderBy(F.rand(seed)).limit(n)`, uncached.
Spark classifies `Rand` as nondeterministic; the second evaluation drew a
different 10,000 rows, so pairs from sample A were joined against keys
from sample B.

The arithmetic corroborates it: two independent 10,000-row samples from
1,564,185 spine rows, times the 62% embedding rate, predicts **~40**
coincidental matches. Observed: **44**. And `avg_rate = 0.262` across
those 44 — the rows that did survive produced entirely sensible features.

**Not reproduced locally** (n=1): `orderBy(rand(seed=42)).limit(1000)`
over a `spark.range` source collected twice gave identical rows. The
local plan has none of the cluster's AQE/Delta partitioning variability,
so the local check neither confirms nor refutes the mechanism — the
conclusion rests on the elimination above, and is labelled as such.

**Fix:** `compute_pr_similarity` collects the spine once into
`spine_rows` and builds the final join's keys from that same list
(`_SPINE_KEY_SCHEMA`), never touching the DataFrame again. Correct
regardless of whether the nondeterminism is AQE-specific — a function
that collects a DataFrame and then re-scans it for a join is fragile by
construction — and it removes a second full scan of Silver. Pinned by
`test_the_spine_is_read_once_so_a_nondeterministic_sample_cannot_shift`,
which counts reads rather than comparing output, because a second
evaluation is invisible whenever the two samples happen to agree.

## Single-query latency (n=25, warm, from a laptop, 2026-09-06)

`p50 182 ms · p90 283 ms · p95 304 ms · mean 196 ms`, against the code
path `compute_pr_similarity` uses (`almanac.embed.query.load_index`).

**Correction — the "~34 ms from the cluster" figure first recorded here
was wrong, and wrong in this project's already-named Gate 1 way: a rate
divided by a denominator that was mostly no-ops.** It came from run
`647656767930289`'s 342 s execution ÷ 10,000 spine rows. But that run's
spine spanned the whole quarter while the index holds only
`>= 2025-09-20`, and `_neighbor_rows` returns immediately when
`embedding is None` — so the large majority of those 10,000 rows never
issued a query at all. The scoped run gives the honest number: 880 s of
execution over ~6,200 real queries (10,000 sampled × the measured 62%
embedding rate) ≈ **140 ms/query from the cluster**, inclusive of Spark
overhead. `similarity_sample_size` stays at **10000**; at 140 ms that is
a ~15 min, ~$0.60 job rather than the ~11 min/$0.44 estimated from the
bad rate.

## Re-run

```sh
export DATABRICKS_HOST=https://<workspace>.azuredatabricks.net
cd infra/terraform && terraform apply -target=databricks_job.pr_similarity
databricks jobs run-now 518991216623709 --no-wait
# then similarity_comparison once pr_similarity's table exists
```

## Task 4's real output — the fixed run

Run `1097688475035466`, TERMINATED/SUCCESS, setup 321 s + execution 840 s.

| metric | run `26796189966426` (bug 3) | run `1097688475035466` (fixed) |
|---|---|---|
| rows | 10,000 | 10,000 |
| rows with ≥1 neighbour | 44 (0.44%) | **6,210 (62.10%)** |
| `avg(similar_neighbor_count)` | 0.044 | **6.21** |
| `max(similar_neighbor_count)` | 10 | 10 |
| non-null `similar_prior_breach_rate` | 30 | **4,785** |
| `avg(similar_prior_breach_rate)` | 0.262 | **0.2657** (sd 0.3491) |

**62.10% is the confirmation, not just the improvement.** The
spine-to-embedding join rate was measured independently at 62% (972,801
of 1,564,185 in-window rows) *before* this run, and every row with an
embedding got the full k=10 while every row without got zero — so
`avg(similar_neighbor_count) = 6.21` is exactly `0.621 × 10`. A partially
working run would not land on the independently predicted number.

Execution over ~6,210 real queries is **135 ms/query**, confirming the
~140 ms corrected above rather than the retracted ~34 ms.

`similar_prior_breach_rate` averaging 0.2657 sits sensibly above the
in-window breach base rate — the neighbours of a PR are not a random
draw, which is the entire premise of the feature.

## Sizing Task 6's population (measured before running it)

Task 6 trains only on PRs with a defined breach outcome, so the sampled
spine has to survive that filter. Measured against Gold for the embedded
window:

| population | rows | share |
|---|---|---|
| PRs opened `>= 2025-09-20` | 1,564,165 | — |
| trainable (`label_exclusion IS NULL OR 'closed_no_response'`) | 760,715 | 48.6% |
| trainable **and** embedded | 434,673 | 57.1% of trainable |

So the 10,000-row sample yields roughly 4,900 trainable rows, ~2,800 of
them carrying real similarity features and ~2,100 with
`similar_neighbor_count = 0` and a null rate. That dilution is honest,
not an artifact: those PRs have no title/body to embed, so the feature is
genuinely unavailable for them.

**A first pass at this measurement said 8.9%, and was wrong** — it used
`label_exclusion IS NULL AND NOT is_censored`, but `compute_breach_labels`
also trains on `closed_no_response`. At 8.9% the sample would have been
~890 rows and Task 6 would have needed a larger, more expensive run; at
the correct 48.6% it needs no change. Recorded because the wrong number
would have bought a ~2.7 h, ~$6.50 job that was never necessary.

**Caveat on the gate itself.** `beats_champion` compares an average
precision measured on ~4,900 sampled rows against 0.612, which was
measured on the full population — different populations, so that
comparison is the weaker of the two available. The paired with-vs-without
delta, where both arms train on the identical `scored` rows, is the sound
number and is what the Task 6 result should be read on.

## §6's `POST /similar-prs`, demoed against the live index

The exit gate asks for the §6 contract itself to run against the real
endpoint, not just `VectorSearchIndex.query`. Three real PRs, real
embeddings, real `as_of`, k=5, against `almanac-embeddings`:

```
POST /similar-prs {repo_id:998922291, pr_number:1476, as_of:'2025-09-29T11:17:14+00:00'}
    pr:998922291:1475   event_time=2025-09-29T10:56:18  score=0.6376
    pr:1024190983:405   event_time=2025-09-23T22:30:25  score=0.5617
    pr:998922291:1454   event_time=2025-09-27T21:03:18  score=0.5613
    pr:1060870165:5     event_time=2025-09-21T23:41:48  score=0.5608
    pr:970331732:2787   event_time=2025-09-02T09:26:55  score=0.5578
```

**The top hit is the immediately preceding PR in the same repo**, opened
21 minutes earlier — not hand-picked, it is simply what the first sampled
PR returned. The third hit is another PR from that same repo. Every
neighbour across all three probes was strictly older than its query's
`as_of` (asserted, not eyeballed), and an unknown PR returns `[]` rather
than raising, since there is no vector to query with.

Two of the three probes returned neighbours scoring ≥0.99 — near-duplicate
text, which is what templated bot PRs look like to an embedding model.
Real behaviour worth knowing about, not an error.

**What was real and what was not:** the index, the endpoint, the vectors,
the timestamps and the query path are all real. The `embeddings`
DataFrame passed in was a 3-row local frame built from rows fetched
through the SQL warehouse, rather than a Spark read of the full Delta
table — the entity_key lookup is therefore exercised against real data
but not at real table scale.

## Task 6 — the real comparison: a real lift that does not clear the gate

Run `754385014066975`, TERMINATED/SUCCESS, setup 412 s + execution 290 s.
Experiments `1250063953296795` (without) and `1250063953296796` (with).

| candidate | without similarity | with similarity | Δ AP |
|---|---|---|---|
| `baseline` | 0.2172 | 0.2172 | — |
| `default` | 0.4848 | **0.5332** | **+0.0484** |
| `is_unbalance` | **0.4953** | 0.4890 | −0.0063 |
| best of arm | 0.4953 | **0.5332** | **+0.0379** |

Supporting metrics on the `default` candidate: ROC-AUC 0.8011 → **0.8198**
(+0.0187), log-loss 0.3977 → **0.3791** (lower is better). Both move the
same direction as AP.

**The identical `baseline` AP (0.2172) in both arms is the check that the
comparison is paired**, not two different populations quietly compared —
it is the positive rate of the shared `scored` set, and it lands on the
same four decimals from two independent runs.

**Read the `default` row, not the best-of-arm row, as the like-for-like
number.** Best-of-arm mixes two changes: the added columns *and* a switch
of winning candidate (`is_unbalance` → `default`). Holding the candidate
fixed, the similarity features are worth **+0.0484 AP (+10.0% relative)**.
The lift is also not uniform — `is_unbalance` got slightly *worse* with
the columns added (−0.0063), which is worth stating rather than averaging
away.

**Gate outcome: `beats_champion = False`** (0.5332 < 0.612), and the
registry confirms the gate actually fired rather than merely being
computed: `almanac_dbx.models.pr_review_sla_risk` still has exactly one
version and `@champion` still points at version 1, despite the job running
with `--register`. So per §8.3a, `databricks.tf`'s `train_model` job does
**not** get the similarity parameters wired in.

**What this result is, honestly.** It is neither the clean win nor the
clean null the phase table anticipated. The similarity features carry real,
reproducible signal on a held-out split — three metrics agree — but the
absolute score on this population does not reach a champion that was
measured on a much larger, different one, so nothing gets promoted. Two
things would have to change before the comparison could be called decisive
either way: a spine sampled at full population scale rather than 10,000
rows, and a champion re-measured on the same rows.

**And the measurement sits on a favourable index.** Per Bug 1, the
deployed index lost 594,181 issue vectors to the entity_key collapse,
making it unusually PR-dense; `_neighbor_rows` nulls every non-`pr`
neighbour by design, so a correctly-keyed index would push ~4× more issue
vectors into each top-k, crowding out PR neighbours and thinning
`similar_prior_breach_rate`. The +0.0484 is therefore an *upper* estimate
of what a correctly-keyed index would give, not a floor.
