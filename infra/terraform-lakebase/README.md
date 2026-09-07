# Almanac — the ephemeral Lakebase stack (Phase 6 Task 9)

A second, disposable Terraform root module that exists for exactly one
reason: **Lakebase is not available in `westus3`**, where the main stack's
workspace lives, and a Lakebase project's region is inherited from its
workspace and cannot be changed.

## Why it exists

Measured before concluding, 2026-09-06:

- Microsoft Learn's own region list for Lakebase names 19 regions. `westus3`
  is not among them.
- From the westus3 workspace, `/api/2.0/database/instances` returned
  "temporarily unavailable", then a connection reset, then timed out on
  three consecutive attempts; `/api/2.0/postgres/projects` timed out too.
  An unrelated control API (`/api/2.0/clusters/spark-versions`) answered
  instantly throughout, so this was neither auth nor connectivity.

## Why centralus, not westus2

The region has to satisfy **two** constraints, and the obvious candidate
fails the second:

| Region | Lakebase | `Standard_D4ds_v6` |
|---|---|---|
| westus2 | supported | **`NotAvailableForSubscription`** |
| centralus | supported | unrestricted |

Checked with `az vm list-skus --location <r> --size Standard_D4ds_v6`. This
is the same trap the project's design-decision gate was written for: an
earlier "SKUs are restricted subscription-wide" claim drawn from two regions
turned out to be wrong in exactly this shape.

Quota in centralus is `Total Regional vCPUs: 0/10` and
`Standard Ddsv6 Family vCPUs: 0/10`. The job runs a driver plus one worker —
8 vCPUs — which fits.

## Why a separate root module

Its own state file. Everything here is created for a bounded measurement
window and destroyed at the end of it, and a `terraform destroy` run in this
directory must not be able to reach the 341M-row quarter, the embeddings
table, or the MLflow experiments that live in the westus3 stack. Separation
is the safety property, not tidiness.

The live poller produces its own data, so this stack needs no access to the
westus3 lake at all.

## Usage

`terraform apply` cannot run cold: the `databricks` provider is configured
from `azurerm_databricks_workspace.this.workspace_url`, which does not exist
until the workspace is created, so the Databricks *data sources* fail to
resolve on a first plan. Two stages:

```bash
# 1. Azure layer (~5-10 min; workspace creation is the slow part)
terraform apply -target=azurerm_databricks_workspace.this \
                -target=azurerm_storage_container.stream \
                -target=azurerm_role_assignment.uc_storage

# 2. Unity Catalog, Lakebase, and the four-stage job
terraform apply
```

Between the two, deploy the code and the token into the **new** workspace —
secret scopes and workspace files are per-workspace, so nothing carries over
from westus3:

```bash
# The output already carries the scheme -- do not prefix it again.
export DATABRICKS_HOST="$(terraform output -raw workspace_url)"

uv build
databricks workspace mkdirs /Workspace/Shared/almanac/dist
databricks workspace import --overwrite --format AUTO \
  --file dist/almanac-0.1.0-py3-none-any.whl \
  /Workspace/Shared/almanac/dist/almanac-0.1.0-py3-none-any.whl
databricks workspace import --overwrite --format AUTO \
  --file scripts/streaming.py /Workspace/Shared/almanac/scripts/streaming.py
databricks workspace import --overwrite --format AUTO \
  --file conf/sources/github_events.yml \
  /Workspace/Shared/almanac/conf/sources/github_events.yml

databricks secrets create-scope almanac
databricks secrets put-secret almanac github_token   # prompts; no value on the command line
```

**Rebuild the wheel first.** A stale wheel is a real failure mode this
project has already paid for — a job that ran 7m38s before failing on code
that was never deployed.

## Teardown — the whole point

```bash
terraform plan -destroy      # read the resource count before applying it
terraform destroy
```

Read the count first. Phase 5's targeted destroy pulled in a dependent job
and would have thrown away the run history that was the evidence for four
real runs. Everything in this stack is meant to go, but the count is still
the check that it is going and nothing else is.

Capture every measurement **before** teardown, never after.
