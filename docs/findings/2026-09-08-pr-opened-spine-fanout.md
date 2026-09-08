# A PR can be opened twice, and the feature spine multiplies it

**Date:** 2026-09-08
**Found by:** Phase 7 Task 10's warehouse window, querying the predictions
table the scoring job had just written.
**Status:** measured and located; **not yet fixed**.

## The observation

`almanac.model.score_runner` wrote **7,320,196** rows. §5.3's trainable
population, measured independently on 2026-09-04 by direct SQL against
`gold.fact_pull_request`, is **7,320,121**. A difference of **+75**.

The predictions table declares `keys=("repo_id", "pr_number")`. Delta has
no `UNIQUE` constraint, and `almanac.contracts.enforce` checks *schema*
only — keys become `NOT NULL` CHECK constraints, not uniqueness — so
nothing refused the write.

```sql
SELECT count(*), count(DISTINCT repo_id, pr_number),
       count(*) - count(DISTINCT repo_id, pr_number)
FROM almanac_dbx.features.pr_breach_predictions;
-- 7320196 | 7320121 | 75
```

The distinct count is **exactly** §5.3's figure. The *set* of PRs is
right; 75 rows are duplicates of PRs already in it.

## Narrowing it

```sql
-- 25 keys, not 75: some appear more than twice
SELECT count(*) AS dup_keys,
       sum(CASE WHEN distinct_scores > 1 THEN 1 ELSE 0 END) AS differing_scores,
       sum(CASE WHEN distinct_breach > 1 THEN 1 ELSE 0 END) AS differing_label
FROM (SELECT repo_id, pr_number, count(*) AS c,
             count(DISTINCT breach_risk) AS distinct_scores,
             count(DISTINCT breach)      AS distinct_breach
      FROM almanac_dbx.features.pr_breach_predictions
      GROUP BY repo_id, pr_number HAVING count(*) > 1);
-- 25 | 5 | 0
```

**25 PRs occupy 100 rows** — four each, where one was expected. Five of
them carry *different* `breach_risk` values, so they are not merely
copies: the same PR was scored from two different feature vectors. The
label never differs.

Row counts per stage, for those same 25 PRs:

| Stage | Rows | Per PR |
|---|---|---|
| `silver.events`, `PullRequestEvent`/`opened` | 50 | 2 |
| — distinct `event_id` among those | **50** | 2 |
| — distinct (PR, `created_at`) | 33 | 1 or 2 |
| `gold.fact_pull_request` | 25 | **1** |
| `features.pr_breach_predictions` | 100 | **4** |

## Root cause

**GH Archive contains two genuinely distinct `PullRequestEvent`/`opened`
events for 25 PRs**, with **different `event_id`s** — 50 events, 50
distinct ids. Silver's deduplication is on `event_id` and is *correct* to
keep both: they are different events. Of the 25, **8 carry two different
`created_at` values and 17 carry the same one** (33 distinct PR/time
pairs over 25 PRs). The 8 with two open times are what produce the 5
differing scores, since `as_of_timestamp` is the opening event's own
`created_at` and a different `as_of` means a different point-in-time
feature vector.

Gold is unaffected — `fact_pull_request` holds exactly one row per PR.
The fan-out is entirely inside the feature platform:

1. `build_pr_opened_spine` returns **one row per opened *event***, not
   per PR — its own docstring says so. → 2 spine rows.
2. `compute_pr_static` filters the same event set through `_opened()` and
   also emits one row per opened event. → 2 rows.
3. `assemble_training_set` joins them with a plain equi-join on
   `(repo_id, pr_number)`. → **2 × 2 = 4**.

The two temporal groups (`author_activity`, `repo_activity`) go through
`as_of_join`, which selects the latest prior row per key and therefore
does not multiply. This is why the factor is exactly 4 and not higher.

## Impact

- **The predictions table violates its own declared key.** 75 rows of
  7,320,196 — **0.001%**.
- **The registered champion was trained on the same defect.** Training
  uses `build_classification_frame`, the identical builder, so
  `pr_review_sla_risk` v1 saw those 25 PRs at 4× weight. At 0.001% of the
  population this is far below the resolution of any metric the model was
  selected on, and the measured calibration (below) is unaffected in any
  visible way — but it is a real deviation from "one row per entity".
- **It is not a leakage bug.** Every duplicate row's features are still
  computed strictly before that row's own `as_of_timestamp`. The
  governing invariant holds; what fails is uniqueness.

## Why no test caught it

`tests/unit/test_features_spine.py` builds its fixtures with one opened
event per PR, so the multi-open case never existed in the test data. It
cannot be caught by `ruff`, `mypy --strict`, or the local suite — it is a
property of the real firehose at 341M-row scale, in the same family as
every other defect in `2026-09-04-cloud-job-run-history.md`.

## The fix, not yet applied

Point-in-time correctness picks the direction: keep the **earliest**
opened event per PR. A later duplicate would move `as_of_timestamp`
forward and let the feature vector see strictly more history — the
leakage direction. Earliest is both correct and conservative.

Two call sites currently state the "opened events" filter separately
(`spine.py`'s inline `where`, `groups.py`'s `_opened()`), which is the
duplication that let them drift apart in the first place. The fix belongs
in one shared derivation both consume, not in two parallel patches —
`almanac-code-style` item 11, and the same shape as the `as_of_join`
O(N²) lesson where fixing only the reporting caller left the real defect
standing.

Ties need a deterministic break, since **17 of the 25 have two events at
the identical `created_at`**: without one, `as_of_timestamp` is stable but
the surviving row's other columns are not, and the byte-for-byte
reproducibility invariant fails. `event_id` is the natural tiebreak.

**Not done in this commit** — it is a change to Phase 3's feature
platform found during Phase 7's reporting window, and it needs its own
failing test, a re-run of the scoring job, and a decision about whether
the champion is retrained.
