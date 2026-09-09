# The quarantine path fired for the first time, on real data

**Date:** 2026-09-08
**Window:** 2026-09-05, ingested during Phase 8 Task 7

## What happened

Across the whole Q3 2025 backfill — **341,060,851 events** — exactly **zero**
rows were ever quarantined. The panel showing it read like a broken panel,
and `docs/limitations.md` said so: a rule that never fires is
indistinguishable from a rule that cannot fire.

The first 2026 window changed that. **100 rows quarantined**, all of them
`ForkEvent`, all failing one rule:

```
_failed_rules  ["repo_id_present"]
event_type     ForkEvent
day            2026-09-05
n              100
```

| | |
|---|---|
| ForkEvents that day, valid | 26,829 |
| ForkEvents quarantined | **100** |
| quarantine rate, ForkEvents | **0.37%** |
| quarantine rate, whole window | 0.0054% (100 of 1,867,991) |

## It is the source, not the parser

Checked against the raw archive file rather than inferred. Event
`14469939899`, from `2026-09-05-3.json.gz`:

```json
"type": "ForkEvent",
"repo": {},
"actor": { ... present ... },
"payload": {"action": "forked", "forkee": {"id": 1357781838, ...}}
```

**`repo` is an empty object.** The actor, the timestamp and the whole
`forkee` node are present; only the repo being forked is missing. So
`repo_id` is genuinely absent upstream, the parser was right to produce
NULL, and the rule was right to catch it.

Note what is *not* recoverable: `payload.forkee` describes the **new fork**,
not the repository that was forked. The identity that went missing is not
sitting in another field under a different name.

## Why this is a good result

Three properties this project asserts were exercised for the first time by
something real rather than by a fixture:

1. **Bad records are quarantined, never dropped.** The rows are in
   `silver.events_quarantine`, readable, with their reason attached.
2. **`_failed_rules` is an array, not a boolean**, so the quarantine is
   analysable *by rule* — the query above groups by it directly, which a
   boolean could not answer.
3. **The split conserves.** Bronze minus Silver moved from 62 to **162**
   when the window landed: 62 pre-existing duplicates removed by dedup, plus
   exactly the **100** quarantined. Nothing vanished from both sides, which
   is the failure mode `coalesce(cond, False)` exists to prevent.

## What this corrects

"Zero of 341M events were ever quarantined" was true, and is now
**incomplete**: it holds for Q3 2025 and not for the 2026 window. The README
and CHANGELOG are updated to say both — the zero, and the first real firing —
because the second is the more useful fact about whether the mechanism works.
