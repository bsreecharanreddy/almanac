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

## Cold start — not measured this session, and said so rather than guessed

**Checked live (Gate 2), not assumed**: [Databricks Model Serving scales
to zero after 30 minutes of no requests](https://answers.databricks.com/does-serverless-scale-up-down)
(Databricks Answers, accessed 2026-09-04) — you are not charged while
scaled to zero, and a new request after that window triggers a genuine
scale-up. That 30-minute idle window did not exist in this session (the
endpoint went straight from creation to the 20-request warm-latency
pass above), so **a true scale-to-zero cold start is not claimed here**.
The community-sourced "10–20 seconds, occasionally minutes" figure §8.1
refused to quote is *still* not quoted as measured — it remains open,
to be measured the next time this endpoint is invoked after sitting
idle for 30+ minutes, which this project can do opportunistically
(e.g. the next session) rather than by holding this one open and idle
on purpose.

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
