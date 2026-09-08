# ADR-0005: Unity Catalog's own lineage, not OpenLineage

**Status:** Accepted 2026-09-07. Supersedes §9's Phase 7 row.

## Context

§9 planned OpenLineage instrumentation for Phase 7. Before writing the
task, the live workspace was checked rather than the plan trusted.

## Decision

Lineage comes from `system.access.column_lineage` and
`system.access.table_lineage`, extracted by `almanac.governance.lineage`
into a committed artifact under `docs/lineage/`.

## Why

`system.access.column_lineage` was **already enabled and already held
18,102 rows spanning 2026-09-02 → 09-07** — Phases 2 through 6 — with
**zero instrumentation ever done**. Adding OpenLineage would have
instrumented a pipeline that was already emitting lineage, to produce a
second, thinner graph.

## Alternatives considered

**OpenLineage / Marquez.** Rejected on the above: it is a real option for
a heterogeneous stack, but here it is duplicate infrastructure for a
capability the platform already provides for free, and it would not cover
the `abfss://` paths any better.

## The trap that shaped the implementation

Measured before the code was written: of Almanac's **4,778** lineage rows
across 29 sources, **4,133 (86.5%) carry only `source_path`, never
`source_table_full_name`** — Bronze, Silver and features are external
Delta paths.

**The obvious implementation, keying on table name, returns 13.5% of the
graph and reports no error.** So resolution runs path → name.

Canonicalising the other way is equally wrong and less obvious: **Gold's
tables are managed**, so their `storage_path` points into
`unity-catalog-storage`, which would collapse every managed Gold table
into one meaningless node. Silver is external and genuinely appears under
both identities; `information_schema.tables` unifies them. `int_pr_events`
is a view and has no `storage_path` at all.

The tests were proven to discriminate rather than assumed to: against a
name-only `canonicalize`, **5 of 7 go red**.

## Consequences

UC lineage cannot see local Spark runs — i.e. the entire test suite — and
retention is a rolling one-year window. Both ship **inside** the
artifact, because a lineage graph reads as complete unless it says
otherwise.

CI cannot regenerate the artifact (system tables need a workspace), so
the committed copy is guarded by a test asserting it still describes a
whole pipeline. A broken extraction yields a *smaller* graph rather than
an error, which is exactly the silent failure that guards against.

**Evidence:** `docs/lineage/lineage.md`, design doc §4.7.
