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

# The Phase 2 burn (databricks.tf). Nothing here provisions compute on apply.

variable "databricks_node_type" {
  type        = string
  description = "Job-cluster SKU; same node Phase 1's calibration measured on, so rates compare."
  default     = "Standard_D4ds_v6"
}

variable "backfill_workers" {
  type        = number
  description = "Worker count; num_workers = N provisions N+1 VMs (N workers + a driver)."
  default     = 4
}

variable "enable_photon" {
  type        = bool
  description = "Photon on the backfill cluster. False to match the no-Photon calibration; the §8.2 A/B is a separate job."
  default     = false
}

variable "backfill_start" {
  type        = string
  description = "First day of the backfill, YYYY-MM-DD."
  default     = "2025-07-01"
}

variable "backfill_end" {
  type        = string
  description = "Last day of the backfill, inclusive."
  default     = "2025-09-30"
}

variable "backfill_checkpoint_dir" {
  type = string
  # Not an abfss:// URI: BackfillCheckpoint is pathlib, and it must outlive
  # the cluster for a resumed run to skip finished days. Not /dbfs/FileStore
  # either -- measured 2026-09-02, this workspace has public DBFS root
  # disabled (`Error: Public DBFS root is disabled`, same restriction Task 7
  # hit once before, now confirmed to cover FileStore too). A Unity Catalog
  # volume is FUSE-mounted and pathlib-compatible without that restriction;
  # `almanac_dbx.burn.checkpoints` is the one this run actually used.
  description = "FUSE-mounted, persistent dir for the per-day resume markers."
  default     = "/Volumes/almanac_dbx/burn/checkpoints/tier3"
}

variable "backfill_python_file" {
  type = string
  # /Workspace/Repos/... assumes a Databricks Repo linked to this repo's git
  # remote; this workspace has no Git credential configured for it, so the
  # actual run used `databricks sync` to a plain workspace path instead.
  description = "Workspace path of scripts/backfill.py, set at deploy time (repos sync or bundle)."
  default     = "/Workspace/Shared/almanac/scripts/backfill.py"
}

variable "photon_ab_python_file" {
  type        = string
  description = "Workspace path of scripts/photon_ab.py."
  default     = "/Workspace/Shared/almanac/scripts/photon_ab.py"
}

variable "source_config_workspace_path" {
  type = string
  # scripts/backfill.py and scripts/photon_ab.py both default
  # --source-config to the relative path conf/sources/gharchive.yml, which
  # resolves against the repo root -- true for `make`/CI, false for a
  # Databricks job task's working directory. Measured 2026-09-02: the
  # first real run failed FileNotFoundError on exactly this, before either
  # script read a single byte of data. Passed explicitly rather than fixed
  # by relying on the script's CWD assumption.
  description = "Workspace path of the synced conf/sources/gharchive.yml, passed explicitly to both scripts."
  default     = "/Workspace/Shared/almanac/conf/sources/gharchive.yml"
}

variable "photon_ab_out_dir" {
  type = string
  # Same DBFS-root-disabled finding as backfill_checkpoint_dir above; a UC
  # volume (`almanac_dbx.burn.photon_ab`) replaces it.
  description = "FUSE-mounted dir each A/B arm writes its measurement JSON to."
  default     = "/Volumes/almanac_dbx/burn/photon_ab"
}

variable "photon_ab_dbt_dependencies" {
  type = list(string)
  # The Photon A/B times Gold too, so its clusters need the dbt extra the
  # backfill does not. Keep in sync with pyproject.toml [project.optional-dependencies].dbt.
  description = "dbt deps for the Photon A/B clusters, on top of backfill_pip_dependencies."
  default = [
    "dbt-core>=1.12.3,<1.13",
    "dbt-spark[session]>=1.11.0,<1.12",
  ]
}

variable "almanac_wheel" {
  type        = string
  description = "Built almanac wheel (uv build), installed on each job cluster. A bundle would resolve this from pyproject.toml."
  default     = "/Workspace/Shared/almanac/dist/almanac-0.1.0-py3-none-any.whl"
}

variable "backfill_pip_dependencies" {
  type = list(string)
  # Keep in sync with pyproject.toml [project].dependencies -- a raw
  # databricks_job cannot derive them; a bundle would.
  description = "Runtime deps installed on each job cluster alongside the wheel."
  default = [
    "httpx>=0.28.1",
    "pydantic>=2.13.5",
    "pydantic-settings>=2.15.0",
    "pyyaml>=6.0.3",
  ]
}
