variable "location" {
  type        = string
  description = <<-DESC
    Azure region. Must support Azure Databricks AND actually offer the
    worker SKU to this subscription -- those are two different things, and
    the second one is what forced this value.

    Measured 2026-09-01 (`az vm list-skus --all`, four regions): every node
    type in Databricks' Azure compatibility reference is
    NotAvailableForSubscription in eastus2 and eastus -- Location- and
    Zone-scoped, all three zones -- while centralus and westus3 offer
    v5/v6 nodes normally. This is a per-region restriction on newer
    subscriptions, not a subscription-wide one.

    The worker SKU is Standard_D4ds_v6, not the originally planned v5:
    availability and quota are separate gates, and the DDSv5 family has a
    zero allocation that self-service cannot raise (there is nothing to
    raise). Ddsv6 was granted 48 vCPUs on request.

    westus3 over centralus: identical on every measured rate
    (VM $0.2260/hr, Jobs DBU $0.30, Realtime Inferencing DBU $0.07),
    where centralus is ~13% higher on both ($0.2550 / $0.47 / $0.079).

    Nothing in this config hardcodes a region, so moving is a one-line
    change here plus a re-run of the pipeline. See
    docs/findings/2026-09-01-azure-region-availability.md.
  DESC
  default     = "westus3"
}

variable "prefix" {
  type        = string
  description = "Name prefix for all resources."
  default     = "almanac"
}

variable "databricks_sku" {
  type        = string
  description = <<-DESC
    standard | premium | trial.

    'premium' is required, not chosen. New Standard-tier workspaces were
    discontinued 2026-04-01 and existing ones auto-upgrade to Premium by
    2026-10-01, so 'standard' is not selectable for a new workspace.

    It is also what we would have picked anyway: Unity Catalog requires
    Premium, and while Premium doubles the Jobs Compute DBU rate
    ($0.15 -> $0.30/hr, measured 2026-09-01 via the Azure Retail Prices
    API), that costs ~34 cluster-hours out of ~136 on the available credit
    against a Tier 3 backfill needing an estimated 10-15. The budget was
    never the binding constraint. See
    docs/findings/2026-09-01-azure-pricing.md.
  DESC
  default     = "premium"

  validation {
    condition     = contains(["standard", "premium", "trial"], var.databricks_sku)
    error_message = "databricks_sku must be standard, premium, or trial."
  }
}

variable "tags" {
  type        = map(string)
  description = "Applied to every resource. Cost attribution depends on these."
  default = {
    project = "almanac"
    env     = "dev"
    owner   = "sree"
  }
}
