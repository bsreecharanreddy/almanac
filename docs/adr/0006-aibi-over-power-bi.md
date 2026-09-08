# ADR-0006: Databricks AI/BI dashboards, not Power BI

**Status:** Accepted 2026-09-07. Supersedes §7's "three Power BI pages".

## Context

§7 specified three Power BI report pages, including a non-negotiable
limitations panel.

## Decision

All three pages are Databricks AI/BI (Lakeview) dashboards, defined as
committed JSON under `dashboards/` and deployed by
`databricks_dashboard` resources.

## Why Power BI was not possible here

Three independent blockers, each checked against Microsoft's own docs:

1. **Power BI Desktop is Windows-only**, with no Mac version and none
   planned — Microsoft restated this as recently as September 2025. This
   machine is macOS.
2. **Publishing from Databricks needs a Premium licence** — Premium
   capacity, PPU, or Fabric capacity — plus XMLA endpoint access.
3. **The one free path** (connecting manually from the Power BI service)
   runs on a licence **restricted to My workspace**, which cannot share
   and cannot publish anywhere else. A dashboard nobody can open is not a
   deliverable.

A fourth, found when trying: Power BI rejects personal Microsoft
accounts, and this tenant's only Global Administrator is one.

## Alternatives considered

The original plan kept **one** Power BI page for the limitations panel,
with the other two as AI/BI. Dropped: a third dashboard is strictly
better than a page nobody can open, and it left an orphaned exit-gate row
with no owning task.

## Consequences

**Better for this project, not merely available.** `databricks_dashboard`
takes a `file_path` to committed JSON, so the pages are diffable,
reviewable, destroyable and testable like every other resource here — a
`.pbix` is a binary blob no CI can inspect. `tests/unit/test_reporting_dashboards.py`
exists because the format allows it.

**The cost of the format**, learned in Task 10's window: the Lakeview
widget schema is **not publicly documented** and the REST API accepts an
invalid spec without complaint. Every data widget shipped with
`{"version": 1, "widgetType": "table"}`, passed every offline test, applied
cleanly through Terraform, and rendered "Invalid widget definition is
imported" on all three pages. The valid shape had to be **read back from
the workspace** after letting the UI rebuild one widget.

A second cost: **`terraform apply` does not lock a dashboard.** An open
browser editor holding stale state overwrote a correct deployed config
twice, with the apply reporting success both times.

**Evidence:** design doc §4.7,
`docs/findings/2026-09-08-reporting-window-evidence.md`.
