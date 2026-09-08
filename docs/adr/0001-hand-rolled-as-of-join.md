# ADR-0001: The as-of join is this project's own code, not a managed feature store

**Status:** Accepted 2026-09-03, implemented Phase 3, still in force.

## Context

CLAUDE.md states one governing invariant: a feature computed `as_of` T
uses only events with `created_at < T`, and the vector must be
reproducible byte-for-byte from the same Delta version a year later. It
also states *why* that invariant is the project: leakage is "invisible in
code review and impossible to bluff."

## Decision

`almanac.features.join.as_of_join` is written and owned here, and the
leakage suite tests **that code**, not a vendor's documented contract.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Feature Engineering in Unity Catalog** — `create_training_set` does a native point-in-time join against any UC table with a `TIMESERIES` primary key, with online-table sync built in. Checked live 2026-09-03; a legitimate current option. | It moves the one mechanism this project exists to demonstrate behind a vendor function. The leakage suite would then be testing Databricks, not Almanac. |
| **Feast** | Pushes join and transform logic *outside* its own framework, so adopting it would not remove the need to write this logic — only add a second piece of infrastructure to operate. |

Neither survives the same test the rest of the repo was built against:
§3.1's peer-of-Gold separation, `dim_repo`'s SCD2, Silver's dedup and the
null-safe quality rules are all hand-built for the same reason.

Handing point-in-time correctness to a vendor function is a different
trade from handing it DBU pricing math.

## Consequences

**We own the performance, and it cost real money to learn that.**
`compute_author_activity`'s self-join was O(N²) per author and only
failed at real scale — a handful of bot logins open enough PRs to make
the join a multi-terabyte blow-up, and `spark.sql.adaptive.skewJoin`
cannot split a self-join. Two paid Databricks runs were cancelled.

Worse, the same shape was then found **again in `as_of_join` itself** —
the primitive every caller depends on. STATUS.md calls that second fix
"the consequential one". Both are now running aggregates over a unioned
timeline, O(N log N).

That episode is the strongest argument *for* this decision as well as the
clearest cost of it: the defect was ours to find, and it was findable.

**Evidence:** `docs/findings/2026-09-04-author-activity-self-join.md`,
design doc §4.4.
