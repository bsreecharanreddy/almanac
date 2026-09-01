variable "location" {
  type        = string
  description = "Azure region. Must support Azure Databricks and the chosen worker SKU."
  default     = "eastus2"
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

    'premium' is deliberate: Unity Catalog requires it. Premium doubles the
    Jobs Compute DBU rate ($0.15 -> $0.30/hr, measured 2026-09-01 via the
    Azure Retail Prices API), costing ~34 cluster-hours out of ~136 on the
    available credit. The Tier 3 backfill needs an estimated 10-15, so the
    budget is not the binding constraint and the governance story is worth
    more than compute we would not use. See
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
