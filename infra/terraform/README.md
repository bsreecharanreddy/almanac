# Almanac — Azure infrastructure

Provisions the resource group, ADLS Gen2 lake, Databricks workspace, and
the Phase 2 burn's **job definitions**. **No compute on `apply`.** A job
cluster spins up only when a run is started — by hand, during an attended
burn — and self-terminates when the run ends. The credit is spent only
during an actual backfill.

## Prerequisite: subscription vCPU quota

**Blocked on an Azure Free Trial subscription.** Verified 2026-09-01:

```
Total Regional vCPUs (eastus2)   limit: 4
Standard DDSv5 Family vCPUs      limit: 4
```

Four vCPUs is the Free Trial cap. The intended backfill cluster
(1 driver + 3 workers × `Standard_D4ds_v5`) needs **16**. The workspace
itself creates fine under the cap — **every cluster launch then fails on
quota**, which surfaces as a confusing Databricks error rather than an
obvious billing one.

Check before applying:

```bash
az vm list-usage --location eastus2 \
  --query "[?contains(localName,'Total Regional vCPUs')].{name:localName,current:currentValue,limit:limit}" -o table
```

Proceed only when the regional limit comfortably exceeds the planned
cluster size.

## Usage

```bash
terraform init
terraform plan
terraform apply
# ... work ...
terraform destroy     # between sessions; this is a cost control
```

`terraform validate` needs no credentials, so `databricks.tf` can be
reviewed anywhere. `plan`/`apply` need `az login` and the workspace already
applied — its data sources query the live API.

## The Phase 2 burn — `databricks.tf`

`databricks_job.backfill` runs the Tier 3 quarter through bronze + silver
via `scripts/backfill.py`, checkpointed per day. Defined on `apply`,
accrues nothing; started by hand, job cluster self-terminates.

```bash
databricks jobs run-now --job-id "$(terraform output -raw backfill_job_url | grep -oE '[0-9]+$')"
terraform destroy
databricks clusters list --output json | jq '[.clusters[]] | length'   # expect 0
```

`backfill_python_file`, `almanac_wheel` and `backfill_pip_dependencies` are
deploy-time inputs: sync the repo (`databricks repos`) and `uv build` the
wheel first. A Databricks Asset Bundle would derive the wheel and deps from
`pyproject.toml`; Terraform still carries the job definition as this
project's IaC of record.

## Why premium tier

Unity Catalog requires it. Premium doubles the Jobs Compute DBU rate
($0.15 → $0.30/hr, measured via the Azure Retail Prices API), costing
~34 cluster-hours out of ~136 available on the credit — against a Tier 3
need estimated at 10–15. The budget is not the binding constraint, so the
governance and lineage story is worth more than compute that would go
unused. Full working: `docs/findings/2026-09-01-azure-pricing.md`.

Set `-var="databricks_sku=standard"` to reverse that, if measured
throughput later shows the backfill needs more than ~60 cluster-hours.

## Cost controls

- **Job clusters only** — All-purpose DBUs cost $0.40–0.55/hr against
  Jobs' $0.15–0.30, and an all-purpose cluster left on a schedule is the
  most common source of a surprise portfolio bill
- Auto-termination on anything interactive
- `terraform destroy` between sessions
- Budget alert configured **before** the first apply
- Every resource tagged `project` / `env` / `owner`
