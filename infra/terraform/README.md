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

## The Phase 7 reporting window — one command up, one command down

`reporting.tf` declares a serverless SQL warehouse for §7's dashboards. It is
the only billable resource Phase 7 provisions, and design doc §4.7 narrows the
phase's cloud window to it alone.

```bash
uv run python -m almanac.infra.window up     # plan + guard, applies nothing
make window-up                               # the same plan, then applied
# ... capture the evidence, before anything irreversible ...
make window-down
```

`make window-up` never runs a bare `terraform apply`. Measured 2026-09-07: a
bare plan in this directory reads **6 to add**, five of which are Phase 5's and
Phase 6's deliberately destroyed stacks — the Lakebase instance ($12.06/day
idle), the Vector Search endpoint ($6.72/day, no scale-to-zero), its index, the
streaming job and its volume. Roughly **$19/day** of silence.

### Phase 6's four teardown traps, as checks rather than as warnings

The traps below are documented in
[`../terraform-lakebase/README.md`](../terraform-lakebase/README.md), where they
were first paid for. `almanac.infra.window` refuses to proceed on each, and
`tests/unit/test_infra_window.py` proves each refusal fires:

| Trap | The check |
|---|---|
| `terraform output` goes blank once a referenced resource is destroyed, and the next Databricks call fails as an **auth** error | Identity is read from `terraform show -json` state. `workspace_url` raises a message that says *"this is not an authentication failure"* when the workspace is missing |
| An external location refuses to delete, citing dependents already gone | The teardown re-reads state afterwards and refuses to report success while any target is **still in state**. A zero exit code is not proof |
| `force_destroy = true` in config is inert until an `apply` writes it to state | A plain `terraform plan` over the targets must be a **no-op** before the destroy plan is even made. Any pending update aborts with the targeted-apply command to run first |
| A bare `apply` plans to **recreate** destroyed resources | Every plan is targeted, then parsed: a bring-up that would change anything outside the window is refused by address, and a destroy plan containing a single `create` is refused outright |

Two properties that are easy to lose and hard to notice:

- **The apply runs the saved plan file**, not `apply -target=…`. The latter
  re-plans, so what executes is not what was guarded.
- **A targeted destroy is checked for cascade.** Phase 5's pulled in
  `databricks_job.pr_similarity` as a dependent, and deleting a job deletes its
  run history — which was the evidence for four real runs.

The warehouse carries no `output`, on purpose: an output is dropped from state
the moment its resource is destroyed. This repo's own state shows it — 14 of the
17 declared outputs are present, and the three missing ones are exactly those
reading Phase 6's torn-down streaming resources.

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
