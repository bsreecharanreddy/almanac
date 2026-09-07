# Lakebase is not available in westus3, and the obvious fallback fails too

**Date:** 2026-09-06
**Phase:** 6, Task 9
**Status:** resolved — a second, disposable stack in `centralus`

## The finding

Phase 6's exit gate requires an online feature store. **Lakebase cannot be
created in this project's main workspace**, and no amount of configuration
fixes it: a Lakebase project's region is inherited from its workspace and
cannot be changed.

## Evidence

**Primary source.** Microsoft Learn's Lakebase region list
(`/azure/databricks/oltp/projects/manage-projects#availability`, page updated
2026-09-03, read 2026-09-06) names 19 regions:

```
eastus, eastus2, centralus, northcentralus, southcentralus, westus, westus2,
canadacentral, brazilsouth, northeurope, uksouth, westeurope, francecentral,
germanywestcentral, australiaeast, centralindia, southeastasia, eastasia,
japaneast
```

`westus3` is absent. The same page states: *"Your Lakebase project is created
in your Databricks workspace region"* and *"The Region for your Lakebase
project is set to your Databricks workspace region and cannot be modified."*
The main workspace is `westus3`.

**Confirmed against the live API before concluding**, because a docs list
alone is a compatibility table, not a support matrix. From the westus3
workspace (`n=5` attempts across two endpoints):

| Call | Result |
|---|---|
| `/api/2.0/database/instances` (attempt 1) | `service … is temporarily unavailable` |
| `/api/2.0/database/instances` (attempt 2) | `read: connection reset by peer` |
| `/api/2.0/database/instances` (attempts 3-5) | no response, killed at 25s each |
| `/api/2.0/postgres/projects` (Autoscaling API) | no response, killed at 30s |
| **`/api/2.0/clusters/spark-versions`** (control) | **answered instantly, every time** |

The control call is what makes this attributable: authentication, network and
CLI are all fine, and only the Lakebase endpoints fail. Both the Provisioned
(`database/instances`, which the `databricks_database_instance` Terraform
resource uses) and Autoscaling (`postgres/projects`) surfaces behave the same
way.

## The fallback region was wrong, and measuring caught it

The obvious move is westus2 — a Lakebase region, adjacent to the data. It
does not work, for an unrelated reason:

| Region | Lakebase | `Standard_D4ds_v6` |
|---|---|---|
| westus2 | supported | **`NotAvailableForSubscription`** |
| centralus | supported | unrestricted (no restrictions returned) |

Command:

```bash
az vm list-skus --location <region> --size Standard_D4ds_v6 \
  --query "[].{n:name,restrictions:restrictions[].reasonCode}" -o json
```

This is the second time this exact shape has appeared in this project. The
first was recorded in the design-decision gate: a claim that "VM SKUs are
restricted subscription-wide", drawn from `n=2` regions, was wrong — other
regions offered the SKU normally. Here the polarity is reversed but the
lesson is identical: **SKU availability is per-region and must be queried
per-region.**

**Quota, checked in the chosen region:**

```
centralus  Total Regional vCPUs:        0/10
centralus  Standard Ddsv6 Family vCPUs: 0/10
```

The streaming job runs a driver plus one worker of `Standard_D4ds_v6` — 8
vCPUs — which fits under both caps. A caution for anyone re-running this:
`az vm list-usage` returns `currentValue` and `limit` as **strings**, so a
naive `int`-comparison filter silently yields nothing rather than failing.

## Two corrections to §4.6's recorded claims

1. **"Lakebase scale-to-zero is not supported" is too broad.** Lakebase
   Postgres *does* support scale-to-zero — enabled by default with a 24-hour
   inactivity timeout, tunable from 60 seconds to 7 days. The restriction is
   specific to **online feature stores** built on Lakebase, which is what
   §4.6 was actually about, so the cost reasoning stands. The blanket
   sentence does not.
2. **Two different CU scales exist and are easy to conflate.** The online
   store API accepts `CU_1|CU_2|CU_4|CU_8`. Lakebase projects run 0.5 CU to
   112 CU (autoscaling to 64), where each CU is 2 GB of RAM. Different
   surfaces, different vocabularies.

## Resolution

`infra/terraform-lakebase/` — a separate root module with its own state,
creating a self-contained stack in `centralus`: resource group, ADLS Gen2
account, Databricks workspace, Unity Catalog wiring, the Lakebase instance,
and the four-stage streaming job. It is destroyed at the end of the
measurement window.

Separate state is the safety property: `terraform destroy` in that directory
provably cannot reach the 341M-row quarter, the embeddings table, or the
MLflow experiments in the westus3 stack. The live poller produces its own
data, so the new stack needs no cross-region access at all.

**The Unity Catalog metastore auto-provisioned** as
`metastore_azure_centralus` on workspace creation, matching the automatic
naming of the existing `metastore_azure_westus3`. This had been the largest
unknown in the plan and needed no account-admin action.

**One bug that only a real apply could find:** the storage account name
`almanac-lblake<suffix>` was rejected — Azure storage account names are
lowercase-alphanumeric only, and this stack's prefix carries a hyphen where
the main stack's (`almanac`) does not. `terraform validate` passes it;
`apply` does not. Fixed with `replace(var.prefix, "-", "")`.

## Cost note

This stack adds a second Databricks managed resource group, with its own NAT
gateway — the main stack measured that class of standing cost at roughly
$2.50/day. Acceptable for a same-session window, expensive if forgotten,
which is why the README leads with teardown.
