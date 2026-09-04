# Findings — the live serving endpoint, measured

**Date:** 2026-09-04
**Source:** `databricks_model_serving.pr_review_sla_risk`, endpoint id
`almanac-pr-review-sla-risk`, served entity
`almanac_dbx.models.pr_review_sla_risk` version 1 (the classification
`default` candidate — `docs/findings/2026-09-04-classification-model-serving-measured.md`).
`terraform apply -target=databricks_model_serving.pr_review_sla_risk`;
20 real `POST` invocations against
`https://adb-7405615444091260.0.azuredatabricks.net/serving-endpoints/almanac-pr-review-sla-risk/invocations`,
timed with `curl -w "%{time_total}"` (excludes CLI startup overhead —
the first, CLI-based call is reported separately below and not mixed
into the percentiles).
A cold-start measurement followed once real idle time had genuinely
elapsed (below) — one more `POST`, timed the same way, after the user
independently hit the endpoint once via the browser UI (an unplanned
real invocation that reset the idle clock and got a fresh wait started
from it, rather than invalidating the measurement).
**What it closes:** design doc §8.1's stated deliverable — "a described
endpoint into a URL that answers," with a measured cold-start
distribution and warm p50/p95, replacing the community-sourced "10–20
seconds, occasionally minutes" figure the doc explicitly refused to
quote until measured.

---

## The endpoint is live

```
databricks serving-endpoints get almanac-pr-review-sla-risk
  state: {"config_update": "NOT_UPDATING", "ready": "READY"}
  served_entities[0].state.deployment: "DEPLOYMENT_READY"
```

Provisioning took **8m27s** from `terraform apply` to `READY` (Terraform
blocks on this by default) — a real number worth recording alongside
cold start, since it's the "first request the whole system ever gets"
in a colder sense than scale-to-zero wake.

A real invocation, using the model's actual signature (10 features,
`float64`, the fix from this session's earlier defect):

```json
{"dataframe_records": [{"prior_pr_count": 3.0, "prior_merge_rate": 0.6,
  "events_total_to_date": 42.0, "bot_events_to_date": 2.0,
  "prs_opened_to_date": 5.0, "bot_share_to_date": 0.1, "is_draft": 0.0,
  "is_bot_author": 0.0, "opened_day_of_week": 3.0, "opened_hour": 14.0}]}
```
returns `{"predictions": [false]}` — the endpoint validates the request
against the logged signature and serves a real prediction from the
registered UC model version, not a stub.

## Warm latency, measured — n = 20

20 sequential real invocations, immediately following the endpoint's
first (CLI-issued) request, so the compute was already warm for all 20:

| Stat | Value |
|---|---|
| min | 252.3 ms |
| p50 | 263.5 ms |
| p95 | 376.8 ms |
| max | 412.8 ms |
| mean | 294.6 ms |

All 20 succeeded (`HTTP 200`), single feature row per request. This is
real network + endpoint latency (`curl`'s own `time_total`), not
CLI-process overhead — the one CLI-issued call (`databricks
serving-endpoints query`) took 2.67s wall-clock, which is Python CLI
startup dominating, not the endpoint; it is reported here for honesty
but excluded from the percentiles above as a different thing being
timed.

## Cold start, measured — n = 1

**Checked live (Gate 2), not assumed**: [Databricks Model Serving scales
to zero after 30 minutes of no requests](https://answers.databricks.com/does-serverless-scale-up-down)
(Databricks Answers, accessed 2026-09-04) — you are not charged while
scaled to zero, and a new request after that window triggers a genuine
scale-up.

The first attempt at this measurement was invalidated for a real,
worth-recording reason: a background timer was started to wait out the
30-minute window, but the user independently opened the endpoint's UI
page and issued a query through it a few minutes in — a genuine
invocation, which resets Databricks' idle clock same as any other
request. Restarted the wait from that point rather than discard it as
noise. That restart's own local timer then ran into an unrelated
wrinkle: `sleep`'s countdown paused for however long the machine
running it was suspended, so the local process was still "sleeping"
well past its nominal 32 minutes. What actually matters is Databricks'
own server-side idle clock, not the local timer used to approximate
it — real wall-clock time since the user's request was independently
confirmed at **~43 minutes** (comfortably past the 30-minute threshold)
before the timed request below was sent, so the local timer's stall was
irrelevant to the measurement's validity, just to how it was scheduled.

**The measured cold start: 51.96 seconds**, `HTTP 200`, a correct
prediction (`{"predictions": [false]}`) — inside the "occasionally
minutes" tail of the community-sourced range §8.1 refused to quote
until measured, and well above the "10–20 seconds" figure that range
led with. Three immediate follow-up requests confirmed the endpoint was
warm again right after: **0.485s, 0.505s, 0.308s** — consistent with
the n=20 warm-latency distribution above, confirming the 52s figure is
real wake-from-zero latency, not a fluke or a timeout.

## Exploring the decision boundary, live — both a `false` and a `true`

After the endpoint was up, tested interactively rather than left as one
canned example — the user queried it directly through the Serving UI's
own query tool, and asked to find a payload that flips the prediction.
Both directions verified two ways: against the model's real
`predict_proba` (loaded locally via `mlflow.pyfunc.load_model`,
`runs:/e4f29f7f8b264c16b29f081e377c445e/model`) and against the live
endpoint itself, so the endpoint's hard-label output is checked against
the actual probability behind it, not assumed to track it.

**`false` — two real payloads, both non-breach:**

| Payload | `predict_proba` | Endpoint |
|---|---|---|
| `prior_pr_count=3, prior_merge_rate=0.6, bot_share_to_date=0.1, is_bot_author=0` (typical human PR) | 31.66% | `false` |
| Same, but `prior_merge_rate=0, bot_share_to_date=1.0, is_bot_author=1` (bot-heavy) | 35.21% | `false` |

The probability moved in the expected direction (+3.6 points toward
breach) when pushed toward bot-like inputs, but stayed under the
endpoint's 0.5 hard-label cutoff. This is not a defect — it is why §5.3
picked PR-AUC (a ranking metric) over accuracy in the first place:
breach is only 25.55% of the population, so a well-calibrated model
rarely crosses 50% confidence even on a genuinely higher-risk PR, and a
served hard label alone hides that ranking movement. The endpoint
currently serves only the class label, not the probability.

**`true` — found by measuring, not guessing.** Read `native.feature_importances_`
off the real model first rather than keep hand-picking inputs:
`prior_pr_count` (highest), `bot_share_to_date`, `prs_opened_to_date`,
`prior_merge_rate`, `events_total_to_date`, `bot_events_to_date`,
`is_bot_author`, `opened_day_of_week`, `opened_hour`, and
**`is_draft` — importance 0, never once split on by the trained trees.**
A 4,000-row random search over the full plausible feature range then
found the boundary: **656 of 4,000 draws (16.4%) crossed 0.5**, max
**74.96%**. The winning combination is a real, worth-recording surprise —
not "more bot signals," but very high prior activity
(`prior_pr_count=131`, `prs_opened_to_date=173`) paired with very low
recent engagement (`events_total_to_date=8`):

| Payload | `predict_proba` | Endpoint |
|---|---|---|
| `prior_pr_count=131, prior_merge_rate=0.2, events_total_to_date=8, bot_events_to_date=15, prs_opened_to_date=173, bot_share_to_date=0.29, is_draft=1, is_bot_author=1, opened_day_of_week=4, opened_hour=2` | **74.96%** | **`true`** |

Confirmed live: `curl` against the real invocation URL with this exact
payload returned `{"predictions": [true]}`, matching the local
`predict_proba` call exactly.

**One genuinely non-monotonic effect surfaced along the way**: holding
the above payload's other values fixed and dropping `prior_merge_rate`
from 0.1 to exactly 0.0 *lowered* the predicted probability (47.60% →
35.01%), the opposite of the "lower merge rate should mean higher risk"
intuition. A real interaction a gradient-boosted tree can learn — a
`prior_merge_rate` of exactly 0 combined with these other feature values
apparently reads more like "brand new, unproven" than "reliably fails to
merge" — and a reminder that eyeballing a tree ensemble's behavior one
feature at a time is unreliable; the random search across the full
feature vector is what actually found the boundary.

## Cost

Near-zero by design (§8.1: $0.07/DBU + a $0.07 per-launch charge,
`scale_to_zero_enabled: true`). This pass consumed: ~8.5 minutes of
provisioning compute, then a few minutes of warm `Small`-workload CPU
serving 21 total real requests (1 CLI + 20 curl). Not itemized to an
exact dollar figure — no billing-API pull was made for this — but
bounded and small by construction; the endpoint is being left up rather
than torn down, per §8.1's explicit "kept up through a real demo window"
design intent, so cost continues to accrue only from actual future
invocations plus near-zero idle, not from being on.
