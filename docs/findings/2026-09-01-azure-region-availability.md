# Findings — VM SKU availability decides the region, not quota

**Date:** 2026-09-01
**Regions measured:** `eastus2`, `eastus`, `centralus`, `westus3`
**Source:** `az vm list-skus --location <r> --resource-type virtualMachines --all`
and `az vm list-usage --location <r>`, plus the Azure Retail Prices API for
the cost comparison. Same rule as the pricing findings: first-party APIs
only, no aggregators.

---

## The short version

The subscription upgrade to pay-as-you-go lifted the regional vCPU cap
from **4 to 10** and cleared the blocker recorded on 2026-09-01. It also
revealed a second, larger one that quota numbers had been masking:

**In `eastus2` and `eastus`, every node type in Databricks' own Azure
compatibility reference is `NotAvailableForSubscription`.** A cluster
there would fail to launch at any quota level. The region moved to
**`westus3`**. A third gate — family quota — then ruled out the originally
planned `D4ds_v5` as well; the worker SKU is **`Standard_D4ds_v6`**.

---

## What was measured

Availability of the node types Databricks lists for Azure:

| Node type | eastus2 | eastus | centralus | westus3 |
|---|---|---|---|---|
| `Standard_D4ds_v5` (planned) | restricted | restricted | **OK** | **OK** |
| `Standard_E4ds_v5` | restricted | restricted | **OK** | **OK** |
| `Standard_E8ds_v5` | restricted | restricted | **OK** | **OK** |
| `Standard_DS3_v2` | restricted | restricted | restricted | restricted |
| `Standard_D4s_v3` | restricted | restricted | restricted | restricted |
| `Standard_L8s`, `Standard_F4s` | restricted | restricted | restricted | — |

Unrestricted SKU counts tell the same story from the other side:

| Region | Unrestricted SKUs | …of which v7 |
|---|---|---|
| eastus2 | 430 | 234 |
| eastus | 575 | 234 |
| centralus | 900 | 234 |
| westus3 | 961 | 220 |

`eastus2` offers this subscription little beyond the **v7** generation —
and **v7 appears nowhere** in Databricks' compatibility reference. The two
sets do not intersect.

The restriction is not a zone gap. Both scopes are present:

```json
{"reasonCode": "NotAvailableForSubscription",
 "restrictionInfo": {"locations": ["eastus2"]}, "type": "Location"}
{"reasonCode": "NotAvailableForSubscription",
 "restrictionInfo": {"locations": ["eastus2"], "zones": ["1","2","3"]},
 "type": "Zone"}
```

## Why `westus3` and not `centralus`

Both offer the SKU. `westus3` is cheaper on every measured rate:

| Rate | eastus2 | centralus | westus3 |
|---|---|---|---|
| `Standard_D4ds_v5` VM | $0.2260/hr | $0.2550/hr | **$0.2260/hr** |
| `Standard_E8ds_v5` VM | $0.5760/hr | $0.6510/hr | **$0.5760/hr** |
| Premium Jobs Compute DBU | $0.30 | $0.30 | **$0.30** |
| Premium Automated Serverless DBU | $0.45 | $0.47 | **$0.45** |
| Premium Realtime Inferencing DBU | $0.07 | $0.079 | **$0.07** |

`westus3` is identical to the original `eastus2` costing on every line, so
**the pricing findings and the Unity Catalog decision carry over
unchanged**. `centralus` would have been ~13% more on both VM and DBU.

## A third gate: family quota, not just availability

Availability and quota are **separate** gates, and passing one says nothing
about the other. `Standard_D4ds_v5` is offered in `westus3` and is in the
Databricks catalog — but its family quota is **0 and could not be raised**:

```
$ az quota update --resource-name standardDDSv5Family --scope .../westus3 --limit-object value=48
ERROR: (QuotaNotAvailableForResource) Request failed.
```

Self-service quota works by *raising* an existing allocation. Where the
family has never been allocated (`limit: 0`), there is nothing to raise and
the request is refused. A family limit of 0 blocks launches no matter what
`Total Regional vCPUs` says.

So the real requirement is a **three-way intersection**, computed rather
than assumed:

1. in Databricks' node-type catalog (337 entries, queried from the live
   workspace via `/api/2.0/clusters/list-node-types`),
2. `az vm list-skus` shows unrestricted in `westus3`, **and**
3. family quota > 0, so an increase can actually be granted.

**37 node types qualify.** The chosen one is `Standard_D4ds_v6` — the
direct successor to the planned v5, same 4-core/16 GB shape.

| | `D4ds_v5` | `D4ds_v6` |
|---|---|---|
| In Databricks catalog | yes | yes |
| Available in `westus3` | yes | yes |
| Family quota | **0, unraisable** | 10 → **48 granted** |
| Price (`westus3`) | $0.2260/hr | **$0.2490/hr** (+10.2%) |

Granted after request (verified via `az vm list-usage`, not from the
request response):

```
      86  Total Regional vCPUs          (asked 48; Azure granted more)
      48  Standard Ddsv6 Family vCPUs
       0  Standard DDSv5 Family vCPUs   (refused, as above)
```

### Cost impact

1 driver + 3 workers × `D4ds_v6`, Premium:

| | v5 (planned) | v6 (actual) |
|---|---|---|
| VM (4 nodes) | $0.904/hr | $0.996/hr |
| DBU (3.0 @ $0.30) | $0.900/hr | $0.900/hr |
| **Total** | **$1.804/hr** | **$1.896/hr** |
| **Hours on $184** | 102 | **97** |

A 5-hour reduction against a 10–15 hour need. The Unity Catalog / Premium
decision is unaffected; the budget is still not the binding constraint.

## Quota, start to finish

`westus3` started where every region does on a new subscription:

```
Total Regional vCPUs             10
Standard DDSv5 Family vCPUs       0
Standard Ddsv6 Family vCPUs      10
Total Regional Low-priority       3
```

A 4-node cluster needs 16, so both the regional cap and the family
allocation had to move. Requesting against an **available** SKU is a far
more routine ask than requesting an availability restriction be lifted, and
it was granted immediately — see the granted figures above.

**Low-priority (spot) remains at 3** and was not raised. Spot workers are
therefore not available for the backfill. Noted rather than fixed: nothing
in the current plan depends on spot, and it is a separate request if a
later phase wants it.

Quota never blocked `terraform apply` — the workspace and storage account
consume no vCPUs, only cluster launches do. The infrastructure was created
and verified while the quota requests were still outstanding.

## Region portability

Nothing in `infra/terraform/` hardcodes a region — `var.location` feeds the
resource group and every resource inherits from it. Moving regions is a
one-line change plus `apply`.

The deeper reason it is cheap: **no data would need migrating.** Every byte
in the lake is reproducible from GH Archive (public) plus the code in this
repo. A region move is `terraform destroy` + `apply` elsewhere + re-run the
backfill — never a storage-to-storage copy, and never an egress bill.

That is a property worth stating rather than discovering: a platform whose
state is fully reproducible from a public source and version-controlled
code has no region lock-in. If `eastus2` or `eastus` opens up later, the
switch costs one variable and one backfill.

Two caveats if that move ever happens:

- An Azure Databricks workspace **cannot be moved** between regions; it is
  destroy-and-recreate. Fine here, where teardown is the plan anyway.
- A **Unity Catalog metastore is per-region**. A new region means a new
  metastore, so catalog/schema definitions get re-applied — an argument for
  keeping them in code rather than clicking them into the UI.

## Corrections this measurement forced

1. An earlier reading of this data concluded the restriction was
   **subscription-wide**, generalizing from `eastus2` and `eastus` alone.
   `centralus` and `westus3` disprove it. Same shape of error as the
   overstated volume claim corrected in `947935f`: a conclusion drawn from
   too small a sample, where widening the sample was cheap.
2. Standard-tier Databricks workspaces were **discontinued 2026-04-01** for
   new creation, with existing ones auto-upgrading to Premium by
   2026-10-01. The Unity Catalog finding recorded Premium as a *decision*;
   it is **forced**. The cost analysis behind it stands and still shows
   Premium was the right call on the merits — but the framing was
   overstated and is corrected in
   `docs/findings/2026-09-01-azure-pricing.md`.
