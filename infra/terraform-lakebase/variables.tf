variable "location" {
  type = string
  # centralus, and the choice is measured rather than preferred. It must be a
  # Lakebase region (Microsoft Learn's list, 2026-09-06) AND offer
  # Standard_D4ds_v6 to this subscription. westus2 satisfies the first and
  # fails the second (`NotAvailableForSubscription`); centralus satisfies
  # both, with `Standard Ddsv6 Family vCPUs: 0/10` and
  # `Total Regional vCPUs: 0/10` -- enough for the 8 vCPUs this job needs.
  description = "Azure region: must support Lakebase AND offer the node SKU to this subscription."
  default     = "centralus"
}

variable "prefix" {
  type        = string
  description = "Name prefix; keeps this stack's resources visibly separate from the westus3 one."
  default     = "almanac-lb"
}

variable "tags" {
  type = map(string)
  # ephemeral=true is not decoration: it marks every resource here as part of
  # a bounded window, so a later cost review can tell this stack apart from
  # the long-lived one in system.billing.usage.
  description = "Applied to every resource, including the job cluster, for cost attribution."
  default = {
    project   = "almanac"
    phase     = "6"
    ephemeral = "true"
  }
}

variable "databricks_node_type" {
  type = string
  # Same SKU the rest of the project measured on, so DBU rates remain
  # comparable. Driver + streaming_workers must stay within the region's
  # 10-vCPU cap: 2 x D4ds_v6 = 8.
  description = "Job-cluster SKU; verified unrestricted in this region before use."
  default     = "Standard_D4ds_v6"
}

variable "streaming_workers" {
  type = number
  # One. The live feed yields ~192 events per poll (measured n=30), so a
  # 30-poll window is a few thousand rows -- and the region's 10-vCPU cap
  # leaves room for exactly one worker beside the driver anyway.
  description = "Workers on the streaming job cluster."
  default     = 1
}

variable "catalog" {
  type        = string
  description = "Unity Catalog catalog created in this workspace for the streaming tables."
  default     = "almanac_lb"
}

variable "schema" {
  type        = string
  description = "Schema within the catalog holding the streaming feature tables."
  default     = "features"
}

variable "online_store_capacity" {
  type = string
  # Smallest of CU_1|CU_2|CU_4|CU_8. The real DBU rate is still unmeasured --
  # system.billing.list_prices returns nothing for LAKEBASE/POSTGRES/OLTP,
  # the second time this project has hit that gap -- so Task 9 confirms it
  # the way Vector Search's was confirmed: by running it and reading
  # system.billing.usage afterwards.
  description = "Lakebase compute units for the online store."
  default     = "CU_1"
}

variable "streaming_max_polls" {
  type = string
  # A string: databricks_job task parameters are strings. 30 polls at the
  # server's advertised 60s interval is a ~30-minute window, the same n the
  # Task 1 capture measurement used.
  description = "Poll count for the bounded live window."
  default     = "30"
}

variable "streaming_secret_scope" {
  type        = string
  description = "Databricks secret scope in THIS workspace holding the GitHub token."
  default     = "almanac"
}

variable "streaming_secret_key" {
  type        = string
  description = "Key within the scope holding the GitHub token."
  default     = "github_token"
}

variable "almanac_wheel" {
  type        = string
  description = "Built almanac wheel, uploaded to this workspace before the first run."
  default     = "/Workspace/Shared/almanac/dist/almanac-0.1.0-py3-none-any.whl"
}

variable "streaming_python_file" {
  type        = string
  description = "Workspace path of scripts/streaming.py in THIS workspace."
  default     = "/Workspace/Shared/almanac/scripts/streaming.py"
}

variable "event_stream_config_workspace_path" {
  type = string
  # Passed explicitly, never left to the script's relative default: a job
  # task's working directory is not the repo root, which cost the main stack
  # a real FileNotFoundError on 2026-09-02.
  description = "Workspace path of conf/sources/github_events.yml in THIS workspace."
  default     = "/Workspace/Shared/almanac/conf/sources/github_events.yml"
}

variable "streaming_pip_dependencies" {
  type = list(string)
  # Keep in sync with pyproject.toml [project].dependencies plus the
  # feature-engineering client the publish stage needs -- a raw
  # databricks_job cannot derive them; a bundle would.
  description = "Runtime deps installed on the job cluster alongside the wheel."
  default = [
    "httpx>=0.28.1",
    "pydantic>=2.13.5",
    "pydantic-settings>=2.15.0",
    "pyyaml>=6.0.3",
    "databricks-feature-engineering>=0.17.1",
  ]
}

variable "prefix_online_catalog" {
  type = string
  # Separate from `catalog`, and deliberately so: Databricks requires an
  # online table's Unity Catalog catalog name to equal its backing Postgres
  # database name. publish_table creates a matching catalog by default, so
  # targeting the source catalog would be the thing that fails -- quietly, at
  # serving time rather than publish time.
  description = "Catalog the published online tables land in; created by publish_table."
  default     = "almanac_lb_online"
}
