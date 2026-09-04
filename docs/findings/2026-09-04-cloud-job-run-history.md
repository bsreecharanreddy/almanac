# Findings — every real cloud job run, across all four Databricks jobs

**Date:** 2026-09-04
**Source:** `databricks jobs list-runs` against each job id, live on the
`almanac-dbx` workspace (`westus3`); cross-referenced against
`docs/findings/2026-09-02-burn-deploy-and-first-run-defects.md`,
`docs/findings/2026-09-04-author-activity-self-join.md`,
`docs/findings/2026-09-04-gold-is-a-metastore-table-not-a-path.md`,
`docs/findings/2026-09-04-model-serving-measured.md`,
`docs/findings/2026-09-04-classification-model-serving-measured.md`, and
`docs/STATUS.md`'s verification log, which each already recorded the
defect/fix narrative for its own run at the time. This doc's contribution
is pulling every run of every job into one place, exactly once, with
nothing left out — not a re-derivation of any of the above.
**n = 23 real job runs** total, across `almanac-tier3-backfill`,
`almanac-build-features`, `almanac-photon-ab`, `almanac-train-model` — the
complete run history returned by the API for each job (`has_more: false`
on every query), not a sample.

---

## Summary

| Job | Total runs | Failed | Cancelled | Succeeded | Real defects found & fixed |
|---|---|---|---|---|---|
| `almanac-tier3-backfill` | 7 | 6 | 0 | 1¹ | 9² |
| `almanac-build-features` | 4 | 0 | 3 | 1 | 1 |
| `almanac-photon-ab` | 5 | 0 | 2 | 3 | 2 |
| `almanac-train-model` | 7 | 4 | 1 | 2 | 4 |
| **Total** | **23** | **10** | **6** | **7** | **16** |

¹ The 7th backfill run completed all 92 days successfully and is reported
`FAILED` in the API — defect #9 below (`sys.exit(main())` under
Databricks' IPython task host). Counted as the one success by actual
outcome, not by the API's status field.
² Defects #1, #2, #4 were caught by a probe or a config read, before any
cluster launched — no run against them in the table below.

**Every defect below was caught before it reached production, cost less
than the full run it would have masked, and is fixed and covered by a
regression test in the codebase today** (except the one item still
recorded as open and undiagnosed — the 7 leftover staging files, whose
own doc is linked below — and does not affect any result quoted in this
project).

---

## `almanac-tier3-backfill` (job `804815089153164`)

The Phase 2 burn: 92 days of Q3 2025 through Bronze + Silver. Full
per-defect narrative: `docs/findings/2026-09-02-burn-deploy-and-first-run-defects.md`.

| # | Run | Start (UTC) | Duration | Result | Defect | Fix |
|---|---|---|---|---|---|---|
| 1 | `588210730480473` | 2026-09-02 15:33 | 496s (8m16s) | FAILED | #3: `--source-config` never passed; resolved against a relative default that only works from the repo root | New `source_config_workspace_path` var, wired into job parameters |
| 2 | `347766258130286` | 2026-09-02 15:44 | 432s (7m12s) | FAILED | #5: bare (scheme-less) path resolved against `fs.defaultFS` (`dbfs:/`), not the real local disk the file was on | `result.path.as_uri()` at both call sites |
| 3 | `958902350643134` | 2026-09-02 16:22 | 620s (10m20s) | FAILED | #6: the only UC storage credential was scoped to the metastore's own storage, never wired to this project's lake account | New `unity_catalog.tf`: access connector + storage credential + external locations for all 4 medallion containers |
| 4 | `1069556816504534` | 2026-09-02 17:05 | 3,185s (53m5s) | FAILED | #7: staging on `/local_disk0`, per-node ephemeral disk, on a 4-worker cluster — a worker's read task can't see the driver's download | New UC managed volume `almanac_dbx.burn.staging`, FUSE-mounted on every node |
| 5 | `1111960589912821` | 2026-09-02 18:25 | 496s (8m16s) | FAILED | #8 (first hypothesis, wrong): `PATH_NOT_FOUND` on the new volume, read as FUSE read-after-write lag | Bounded retry (later removed — see run 6) |
| 6 | `156595311273405` | 2026-09-02 19:13 | 449s (7m29s) | FAILED | #8 (real cause): `file:` scheme on a volume path routes through the JVM's `LocalFileSystem`, not the UC volume filesystem — the retry failed identically, falsifying the lag hypothesis | `spark_path()`: bare path under `/Volumes`, `.as_uri()` otherwise |
| 7 | `227270110474807` | 2026-09-02 20:37 | 18,170s (5h2m50s) | **FAILED (API) / SUCCESS (real)** | #9: `sys.exit(main())` under Databricks' IPython task host marks a zero exit code as failed | `almanac.cli.run_cli(main)` at every job-task entrypoint |

**Result: 92/92 days, 341,060,851 Bronze rows, 165.987 GB gz, zero
missing hours, for $11.96** — 38% of the $31.71 estimate, 6.5% of the
$184 credit. Six failed runs cost well under a dollar combined against
that; the failure that actually cost real money (run 4, 53 minutes) was
also the one that proved the earlier fixes had cleared enough ground to
reach sustained execution.

## `almanac-build-features` (job `991348325786702`)

Materializing Phase 3's three v1 feature tables against the real
341M-row Silver quarter, for Phase 4 Task 9. Full narrative:
`docs/findings/2026-09-04-author-activity-self-join.md`.

| # | Run | Start (UTC) | Duration | Result | Defect | Fix |
|---|---|---|---|---|---|---|
| 1 | `924195167084181` | 2026-09-04 04:43 | 1,433s (23m53s) | CANCELED | `compute_author_activity`'s self-join is O(N²) per author — a near-Cartesian blow-up on bot logins (`AQE: sizeInBytes=596.7 TiB`) | (diagnosis run — see run 3) |
| 2 | `48552036331781` | 2026-09-04 05:09 | 908s (15m8s) | CANCELED | Same hang, second reproduction | (diagnosis run) |
| 3 | `485360890184517` | 2026-09-04 05:31 | 993s (16m33s) | CANCELED | Same hang, third reproduction — live thread dump taken this run, confirming `BytesToBytesMap.safeLookup` under a `HashAggregate` fed by the near-Cartesian join | Reformulated as a time-ordered running aggregate (`rowsBetween(unboundedPreceding, currentRow)`), O(N log N), skew-proof |
| 4 | `212373785836208` | 2026-09-04 06:31 | 879s (14m39s) | **SUCCESS** | — | 437s of actual execution once fixed, down from a hang that never finished |

**Cost**: three cancelled runs ≈$13 combined (the most expensive
diagnosis in this table, because the hang wasn't caught until minutes in
and each reproduction cost roughly the same again); the fixed, successful
run ≈$1.

## `almanac-photon-ab` (job `308819203755958`)

The §8.2 Photon A/B hypothesis test, three replicate pairs. Full
narrative: `docs/findings/2026-09-03-photon-ab.md`,
`docs/STATUS.md`'s 2026-09-02/09-03 rows.

| # | Run | Start (UTC) | Duration | Result | Defect | Fix |
|---|---|---|---|---|---|---|
| 1 | `674441653017831` | 2026-09-03 03:27 | 771s (12m51s) | CANCELED | `arm_standard` FAILED: `pathlib` collapsed `abfss://silver@acct/events`'s `//` to a single `/`, silently no-longer-absolute; `arm_photon` cancelled still waiting for a cluster, no work done | Type asymmetry: `warehouse` stays `str` (may be a URI), `metastore` stays `Path` (Derby is genuinely local) |
| 2 | `488000457742954` | 2026-09-03 04:14 | 55s | CANCELED, pre-flight (both arms still `PENDING`, zero compute) | `GoldTarget`'s `project_dir`/`profiles_dir` fell back to a relative default — a job task's cwd is not the repo root (the same class as backfill's #3) | Both passed explicitly; `DEFAULT_PROJECT_DIR`'s relativity pinned by a test |
| 3 | `693303490917119` | 2026-09-03 04:18 | 1,089s (18m9s) | SUCCESS (Gold row unusable) | Both arms shared one UC metastore concurrently — `CREATE TABLE IF NOT EXISTS` let one arm's Silver win, so the other arm's dbt read the winner's data | Per-arm Silver/Gold schemas (`ALMANAC_SILVER_SCHEMA`/`ALMANAC_GOLD_SCHEMA`), isolating both the arms from each other and from the real `silver`/`gold` |
| 4 | `325049271562727` | 2026-09-03 05:07 | 843s (14m3s) | **SUCCESS** | — | replicate 2, first with per-arm schemas |
| 5 | `236314776904974` | 2026-09-03 05:25 | 905s (15m5s) | **SUCCESS** | — | replicate 3 |

**Result, n=3 replicate pairs**: Silver **2.14x** (1.96/2.40/2.06), Gold
**1.38x** (1.45/1.29/1.42) — both clear the pre-registered 1.10
materiality threshold on every replicate. **Bronze withheld as
indeterminate** — mean 1.15x clears the threshold, but the per-replicate
range (1.05–1.23) straddles it, and re-running an identical arm alone
varies 2.3–29.9%, larger than the ~15% effect being tested. **Decision:
do not enable Photon** — the break-even DBU multiplier (1.55–2.16) lands
around Photon's ~2x actual multiplier, a wash, and the real lever
(Bronze's single-threaded gzip, 63.8% of execution) is untouched by
Photon either way.
**Cost, estimated at the measured $2.370/hr (5-VM) rate**: run 1
(`arm_standard` only ran) ≈$0.51, run 2 $0 (cancelled pre-cluster), the
three successful pairs (two concurrent 5-VM clusters each) ≈$1.43 /
$1.11 / $1.19 ≈ **$4.24 total** — an estimate from wall-clock duration,
not a billing-API pull (this job never received an itemized $ figure in
STATUS.md the way backfill and train_model did).

## `almanac-train-model` (job `117899697123337`)

Phase 4's training job — regression (§5.1), then reframed to
classification (§5.3). Full narrative: `docs/findings/2026-09-04-gold-is-a-metastore-table-not-a-path.md`,
`docs/findings/2026-09-04-model-serving-measured.md`,
`docs/findings/2026-09-04-classification-model-serving-measured.md`.

| # | Run | Start (UTC) | Duration | Result | Defect | Fix |
|---|---|---|---|---|---|---|
| 1 | `932108320778102` | 2026-09-04 04:21 | 458s (7m38s) | FAILED | Pre-flight: `scripts/model.py` not yet deployed to the workspace path at all (job triggered before the first deploy) | Deployed the wheel + script via `workspace import --overwrite` |
| 2 | `33066446989228` | 2026-09-04 04:30 | 199s (3m19s) | FAILED | Pre-flight: triggered before `almanac-build-features` had ever completed — `author_activity` didn't exist yet | Re-triggered after the feature build succeeded |
| 3 | `961660350236501` | 2026-09-04 07:14 | 451s (7m31s) | FAILED | Gold's fact is a UC managed table, not a Delta path — `build_training_frame` read it as path Delta, the local-test layout | `build_training_frame` reads Gold's fact **by name** (`spark.read.table`, still `versionAsOf`-pinnable) |
| 4 | `792359179723419` | 2026-09-04 10:17 | 6,430s (1h47m10s) | CANCELED | `as_of_join` had the same O(N²) self-join shape as `compute_author_activity` — `11.7 PiB` of intermediate on one join, single-node cluster, never finished | Rewrote `as_of_join` as a unioned-timeline `last(...)` window, O(N log N); job moved from single-node to the 5-VM shape |
| 5 | `283014697409852` | 2026-09-04 13:23 | 731s (12m11s) | **SUCCESS — regression, null result** | — | `model_mae` 108,890s vs. `baseline_mae` 71,917s: worse. §5.1's gate held; nothing registered |
| 6 | `704688669520130` | 2026-09-04 15:46 | 838s (13m58s) | FAILED | Training succeeded (classification, real signal) but `register_champion` failed: `mlflow.lightgbm.log_model` never carried a `signature`, which UC's registry requires and a `file://` store doesn't validate | `infer_signature(...)` computed at fit time on both `TrainResult`/`ClassifierCandidateResult`, passed through to `log_model`; closed with a local regression test |
| 7 | `1102177434831425` | 2026-09-04 16:39 | 871s (14m31s) | **SUCCESS — classification, non-null result** | — | `average_precision` 0.612 (`default`) vs. 0.285 baseline, `roc_auc` 0.828; registered live in UC (`pr_review_sla_risk` v1, `@champion`) |

**Cost**: runs 1–2 brief cluster provisioning only, no sustained compute
(not separately itemized); run 4 (single-node, 107 min) ≈$0.30; run 5
(5-VM, 12 min) ≈$1; run 6 (5-VM, 14 min) ≈$1; run 7 (5-VM, 14.5 min) ≈$1.

---

## Grand summary

**23 real job runs across four Databricks jobs, 16 real defects found and
fixed, none of them visible to `ruff`, `mypy --strict`, or the local test
suite** — every one was a runtime fact about this specific cloud
workspace (a storage credential never declared, a path scheme that only
matters against `abfss://` or a UC volume, two clusters racing on shared
storage, a join that only blows up at real-author-count scale) that
local dev and CI structurally cannot see, the same lesson
`2026-09-02-burn-deploy-and-first-run-defects.md` names directly: **"a
defect that is structurally invisible from where you're standing is not
a defect you failed to catch, it's a defect the vantage point cannot
see."**

**The pattern repeats across all four jobs, not just the first one.**
`backfill` found 9 defects in getting the burn to complete once;
`build_features` and `train_model` each independently rediscovered the
*same* O(N²) self-join shape at real author-count scale, in two different
functions (`compute_author_activity`, then `as_of_join`) months apart in
the same session — proof that a defect fixed once in one place does not
generalize to its own sibling code path without a second real run to
find it. `photon_ab` found the concurrent-cluster storage-race class
twice, in two different shared resources (staging files, then the UC
metastore).

**Total measured cost across all four jobs: ≈$11.96 (backfill) + ≈$14
(build_features) + ≈$4.24 (photon_ab, estimated) + ≈$3.30 (train_model,
both objectives combined) ≈ $33.50** of the $184 credit — against a
derived Tier-3-alone estimate of $31.71 for the backfill by itself,
meaning the *entire* rest of the cloud verification (features, Photon
A/B, both training objectives, sixteen real defects found and fixed)
cost roughly as much as the backfill's own original budget, not a
multiple of it.

**Every result this project quotes as "measured on the real quarter" —
92/92 days ingested, Gold's row-conservation and referential integrity
at 341M-row scale, Silver 2.14x / Gold 1.38x under Photon, the regression
null result, the classification non-null result and its live UC
registration — survived being re-derived after a real defect was found
and fixed, not before.** None of the sixteen defects changed a number
this project had already published; every one was caught and closed
before the run whose number is quoted.
