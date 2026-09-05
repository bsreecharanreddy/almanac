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

variable "backfill_staging_dir" {
  type = string
  # Not /local_disk0: that's per-node ephemeral disk. fetch_hours() downloads
  # each hour's .json.gz via plain Python I/O in the driver process, but the
  # medallion job cluster runs with backfill_workers > 0 (multi-node, for
  # Tasks 8-9's fetch parallelization) -- day.py's _land_bronze then reads
  # that same path back as a distributed Spark job, and any read task
  # scheduled on a worker node other than the driver hits
  # FAILED_READ_FILE.FILE_NOT_EXIST, since the worker's own /local_disk0
  # never had the file. Measured 2026-09-02 on the backfill job's first run
  # to reach real Spark execution (defect #7). A UC volume is FUSE-mounted
  # identically on every node, same fix shape as backfill_checkpoint_dir
  # above; `almanac_dbx.burn.staging` is the one this fix actually used.
  description = "FUSE-mounted scratch dir for each hour's downloaded .json.gz, visible from every cluster node."
  default     = "/Volumes/almanac_dbx/burn/staging"
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

variable "gold_python_file" {
  type        = string
  description = "Workspace path of scripts/gold.py, the job entrypoint for almanac.gold.runner."
  default     = "/Workspace/Shared/almanac/scripts/gold.py"
}

variable "gold_project_dir" {
  type = string
  # The synced dbt/ directory holds dbt_project.yml and profiles.yml together,
  # so one path serves as both --project-dir and --profiles-dir.
  description = "Workspace path of the synced dbt project."
  default     = "/Workspace/Shared/almanac/dbt"
}

# Phase 4 (design doc §5.2): the training job, the UC model registry, and
# the serving endpoint. Nothing here provisions compute on apply.

variable "model_registry_catalog" {
  type = string
  # almanac_dbx, the metastore's own default-storage managed catalog: this
  # account has account-level Default Storage and no metastore storage_root,
  # so a fresh `databricks_catalog` cannot be created here (Task 9, 2026-09-04).
  description = "UC catalog holding the trained model (Phase 4, §5.2). The metastore's managed catalog."
  default     = "almanac_dbx"
}

variable "model_registry_schema" {
  type        = string
  description = "Unity Catalog schema, under model_registry_catalog, holding the trained model."
  default     = "models"
}

variable "model_python_file" {
  type        = string
  description = "Workspace path of scripts/model.py, the job entrypoint for almanac.model.runner."
  default     = "/Workspace/Shared/almanac/scripts/model.py"
}

variable "model_gold_table" {
  type = string
  # The Gold job's dbt `session` run creates `gold.fact_pull_request`, and
  # on this Unity Catalog workspace that resolves to the default catalog's
  # managed table `almanac_dbx.gold.fact_pull_request` -- not a Delta dir
  # under the job's --warehouse abfss path, which is where a local dbt run
  # would put it (Task 9, 2026-09-04). build_training_frame reads it by name.
  description = "Fully-qualified name of Gold's fact_pull_request, the training job's label source."
  default     = "almanac_dbx.gold.fact_pull_request"
}

variable "features_python_file" {
  type        = string
  description = "Workspace path of scripts/features.py, the job entrypoint for almanac.features.runner."
  default     = "/Workspace/Shared/almanac/scripts/features.py"
}

variable "model_pip_dependencies" {
  type = list(string)
  # Keep in sync with pyproject.toml's [project.optional-dependencies] ml
  # group -- a raw databricks_job cannot derive them; a bundle would. The
  # pandas ceiling is load-bearing: mlflow pins pandas<3 (Task 1's finding).
  description = "The ml extra's runtime deps, installed on the training job's cluster."
  default = [
    "mlflow>=3.15.2",
    "lightgbm>=4.7.0",
    "scikit-learn>=1.9.0",
    "pandas>=2.3.3,<3",
  ]
}

variable "model_serving_workload_size" {
  type        = string
  description = "Served-model workload size (Small|Medium|Large). scale_to_zero governs idle cost, not this."
  default     = "Small"
}

# Phase 5 (design doc §8.3a): the embedding index. Nothing here provisions a
# served vector index on apply -- databricks_vector_search_index's
# source_table must already exist, UC-registered, with Change Data Feed
# enabled, and only a real run of the embeddings job (Task 7) creates that.

variable "embeddings_schema" {
  type        = string
  description = "UC schema, under model_registry_catalog, holding the embeddings table the vector index syncs from."
  default     = "embeddings"
}

variable "embedding_dimension" {
  type = number
  # all-MiniLM-L6-v2's own output width (src/almanac/embed/pipeline.py's
  # DEFAULT_MODEL). Both numbers must move together if the model ever does --
  # Terraform cannot derive one from the other.
  description = "Vector width the embedding pipeline writes."
  default     = 384
}

variable "embeddings_python_file" {
  type        = string
  description = "Workspace path of scripts/embeddings.py, the job entrypoint for almanac.embed.pipeline."
  default     = "/Workspace/Shared/almanac/scripts/embeddings.py"
}

variable "embeddings_limit" {
  type = string
  # A string, not a number: databricks_job task parameters are strings, and
  # "" is the natural "no limit" sentinel. Set for a bounded proof run
  # (docs/findings/2026-09-05-embedding-worker-fork-deadlock.md), clear it
  # for the real run.
  description = "If set, --limit passed to the embeddings job -- a bounded proof run, not the scoped corpus."
  default     = ""
}

variable "embeddings_since" {
  type = string
  # The real run's scope knob. Full 14.9M-text corpus is ~25h of CPU-bound
  # encode at the measured ~168 texts/s and GPU is quota-blocked, so the
  # index is built over recent events. "2025-09-20" was measured at ~1.73M
  # qualifying texts (docs/findings/2026-09-05-embedding-worker-fork-
  # deadlock.md). "" embeds the whole corpus.
  description = "If set, --since-date (Bronze event_date lower bound) passed to the embeddings job."
  default     = ""
}

variable "embeddings_pip_dependencies" {
  type = list(string)
  # Keep in sync with pyproject.toml's [project.optional-dependencies] ml
  # group -- a raw databricks_job cannot derive them; a bundle would. torch
  # is sentence-transformers' own transitive dependency, not listed
  # separately, same convention model_pip_dependencies already follows for
  # scikit-learn's own transitive deps.
  description = "The ml extra's embedding-specific deps, installed on the embeddings job's cluster."
  default = [
    "sentence-transformers>=6.0.1",
    "databricks-ai-search>=0.78",
  ]
}

# Task 4/6's real run: compute pr_similarity against the live index, then
# compare with/without it against the registered champion. Neither job
# needs sentence-transformers/torch -- querying an already-built index
# is not embedding text.

variable "similarity_python_file" {
  type        = string
  description = "Workspace path of scripts/pr_similarity.py, the job entrypoint for almanac.features.similarity_runner."
  default     = "/Workspace/Shared/almanac/scripts/pr_similarity.py"
}

variable "similarity_comparison_python_file" {
  type        = string
  description = "Workspace path of scripts/similarity_comparison.py, the job entrypoint for almanac.model.similarity_comparison."
  default     = "/Workspace/Shared/almanac/scripts/similarity_comparison.py"
}

variable "similarity_pip_dependencies" {
  type        = list(string)
  description = "databricks-ai-search only -- querying the real index, not embedding."
  default     = ["databricks-ai-search>=0.78"]
}

variable "similarity_sample_size" {
  type = string
  # A string, not a number: databricks_job task parameters are strings.
  # Placeholder pending Task 7's real single-query-latency measurement
  # against the live index -- sized properly (docs/findings/) before the
  # pr_similarity job's first real run, not left at a guess.
  description = "Spine rows to query against the real index, bounded by measured per-query latency (§8.3a)."
  default     = "5000"
}
