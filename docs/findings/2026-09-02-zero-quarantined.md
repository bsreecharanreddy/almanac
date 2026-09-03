# Findings — 0 quarantined of 341M rows, and whether that meant anything

**Date:** 2026-09-02
**Run:** Databricks job run `227270110474807`, Q3 2025, 92 days.
**Method:** the run's printed summary for the counts; a new set of
violation cases in `tests/unit/test_quality.py` for the rules; reading
`conf/sources/gharchive.yml` against `src/almanac/pipeline/quality.py` for
what each rule can and cannot discriminate.

---

## The number that prompted the question

```
rows_bronze  341,060,851
rows_clean   341,060,789
rows_quarantined  0
```

Not one row in a full quarter failed a reject rule. That is either very
clean data or an inert ruleset, and **the two produce the same number.**
Reporting it as evidence the quality layer works — without first
establishing which — would have been a claim the run did not support.

## What was actually missing

Every quality test in the suite proved the *mechanism*: a reject
quarantines, a warn does not, a null predicate counts as a failure, the
split conserves every row. All of them ran against rules **defined in the
test file** (`id_present`, `repo_present`, `actor_present`).

The only test touching the shipped ruleset was
`test_every_declared_rule_resolves_against_the_silver_shape`, and it
asserts exactly what its name says: that each rule *resolves* against the
Silver columns. It forces analysis and discards the result. A rule that
parses, binds, and then never fires passes it.

So before this, **no test proved that any rule the pipeline actually
ships had ever quarantined anything.** The 3 quarantined rows ever
observed came from the *legacy* 2014 fixture (`repo_id_present`) — a
schema era Tier 3's span does not contain.

## What was added

Six violation cases, each taking a Silver row that passes every shipped
rule and corrupting exactly one column, asserting the exact set of shipped
rules that trips:

| Case | Corruption | Rules that must fire |
|---|---|---|
| `event_id_null` | `event_id` → null | `event_id_present` |
| `event_id_empty` | `event_id` → `""` | `event_id_present` |
| `created_at_null` | `created_at` → null | `created_at_present`, `created_at_not_future` |
| `created_at_future` | `created_at` → `ingested_at + 1 day` | `created_at_not_future` |
| `repo_id_null` | `repo_id` → null | `repo_id_present` |
| `event_type_null` | `event_type` → null | `event_type_present` |

Plus a baseline test that the uncorrupted row fails nothing (without it a
case could pass for the wrong reason), and a completeness guard asserting
every `REJECT` rule in the YAML is covered by some case — so a rule added
to the config without a case reddens the suite rather than silently
widening the gap this finding is about.

`created_at_null` tripping two rules is not sloppiness in the case, it is
the three-valued-logic guard visible on the real ruleset: `NULL <=
ingested_at` is `NULL`, which `coalesce(_, False)` turns into a failure
rather than a silent pass.

**Result: all five reject rules fire.** 17 tests pass in
`tests/unit/test_quality.py`. So 0 quarantined is a statement about the
data, not an artifact of dead rules.

---

## With one carve-out that matters

**`created_at_not_future` could not have discriminated anything on this
run.** Its expression is `created_at <= ingested_at`. Every row in the
backfill is Q3 2025 data stamped with a 2026-09-02 `ingested_at` — about a
year of margin on every one of 341 million rows. The rule can fire only on
a *future-dated corrupt timestamp*, never on the clock-skew or
late-arriving case it was written to guard.

It is a real-time-ingestion rule being run in batch. It is not wrong to
keep — it costs nothing and it will matter the moment anything ingests
near-real-time — but on this run **four rules had discriminating power,
not five**, and the finding should be read that way.

## What 0 quarantined still does not mean

**The rules test presence, not correctness.** All five ask whether a field
is there. A row with a valid-but-wrong `repo_id`, a plausible-but-incorrect
`created_at`, or a real `event_type` naming the wrong thing passes every
one of them. "No structurally malformed rows" is the claim the run
supports; "no bad data" is not.

The membership check on `event_type` that would add a correctness-shaped
signal is still only a carried-forward idea, not implemented — and per the
comment already in `gharchive.yml` it belongs at `warn`, since a new
GitHub event type is drift to flag, not data to reject.

---

## A second number, unasked for and better than expected

`rows_bronze - rows_clean - rows_quarantined` = **62 rows**, removed by
`deduplicate` before the rules ran. That is 1 duplicate in 5.5 million.

This is the **first measurement of §12 trap 4 on real data at scale.**
Phase 0 explicitly could not test it — its duplicate ratio used
*non-adjacent* hours and so never compared two consecutive ones, and the
gap was recorded and deferred. Phase 1 covered it with unit tests. This
run is the first time the cross-hour dedup ran over 2,208 genuinely
consecutive hourly files, and it found real duplicates at a rate
consistent with Phase 0's order-of-magnitude estimate.

Duplicates spanning a *day* boundary remain out of scope by design —
Silver's grain is a day, stated in its module docstring, not accidental.
At 1 in 5.5M within-day, the residual across 91 day-boundaries is
negligible, but it is unmeasured rather than zero.
