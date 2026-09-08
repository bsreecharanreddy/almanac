# ADR-0007: A second Terraform root module in `centralus` for Lakebase

**Status:** Accepted 2026-09-06. Stack since torn down; the split stands
as the recorded resolution.

## Context

Phase 6's exit gate required an online feature store. The design assumed
Lakebase would run beside everything else in the main `westus3`
workspace.

## The finding

**Lakebase names 19 supported regions and `westus3` is not among them**,
and a Lakebase project's region is *inherited from its workspace and
cannot be modified*. The main workspace is `westus3`.

Confirmed live rather than taken from the docs — a compatibility table is
not a support matrix. From the westus3 workspace,
`/api/2.0/database/instances` returned "temporarily unavailable", then a
connection reset, then timed out three times, and `/api/2.0/postgres/projects`
timed out too — while `/api/2.0/clusters/spark-versions` **answered
instantly throughout**. That control call is what makes the failure
attributable to Lakebase rather than to auth or the network.

## Decision

`infra/terraform-lakebase/` — a separate root module with its own state,
self-contained in `centralus`: resource group, ADLS Gen2, workspace, UC
wiring, Lakebase instance, four-stage job.

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Move the whole stack to a supported region** | 341M rows of Bronze/Silver, Gold, the registered champion and both MLflow experiments live in westus3. Migrating them to satisfy one Phase 6 component is a far larger change than a second stack. |
| **Use `westus2`**, the obvious nearby Lakebase region | **Measured, and it was wrong.** westus2 reports `Standard_D4ds_v6` as `NotAvailableForSubscription`; `centralus` offers it unrestricted. |

That second row is the **second appearance of the same mistake shape** in
this repo: an earlier claim that "VM SKUs are restricted subscription-wide"
was generalised from `n=2` regions and was wrong. Checking per-region
before choosing is now the recorded rule, and here it changed the answer.

## Consequences

Two root modules, two states, two `terraform destroy` targets — genuinely
more operational surface, and the reason `almanac.infra.window` exists
with an explicit target list rather than a bare apply.

A quota caution recorded with it: `az vm list-usage` returns
`currentValue`/`limit` as **strings**, so a naive integer comparison
silently returns nothing instead of failing.

Measured idle rate for the instance: **$12.06/day**, read the day *after*
the window, because `system.billing.usage` lags and cannot be queried for
the period it is measuring.

**Evidence:** `docs/findings/2026-09-06-lakebase-region-and-sku-constraints.md`.
