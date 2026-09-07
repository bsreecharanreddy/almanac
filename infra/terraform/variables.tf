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

# Phase 7 Task 1. Its own schema, not `models`: Databricks also creates an
# internal `<payload table ID>_checkpoints` volume alongside the inference
# table, and mixing that machinery into the schema holding the registered
# model makes both harder to reason about and to grant on.
variable "serving_logs_schema" {
  type        = string
  description = "Unity Catalog schema, under model_registry_catalog, holding the serving endpoint's inference table."
  default     = "serving_logs"
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
  # The real run's scope knob, and the default matches what is deployed:
  # the full 14.9M-text corpus is ~25h of CPU-bound encode at the measured
  # ~168 texts/s and GPU is quota-blocked, so the index is built over
  # recent events. "2025-09-20" was measured at ~1.73M qualifying texts and
  # the real run embedded 1,459,551 (docs/findings/2026-09-05-embedding-
  # worker-fork-deadlock.md). "" embeds the whole corpus.
  description = "--since-date (Bronze event_date lower bound) for the embeddings job; \"\" is the full corpus."
  default     = "2025-09-20"
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
  # 10000: measured 2026-09-05 that a single similarity_search against the
  # live index is ~180 ms p50 from a laptop (~100 ms expected from a
  # same-region cluster), and compute_pr_similarity queries sequentially on
  # the driver -- 10 K rows is a ~20-30 min job and enough to train Task 6's
  # champion comparison. Larger is a one-line change
  # (docs/findings/2026-09-05-embedding-worker-fork-deadlock.md).
  description = "Spine rows to query against the real index, bounded by measured per-query latency (§8.3a)."
  default     = "10000"
}

variable "streaming_python_file" {
  type        = string
  description = "Workspace path of scripts/streaming.py, the job entrypoint for almanac.stream.runner."
  default     = "/Workspace/Shared/almanac/scripts/streaming.py"
}

variable "event_stream_config_workspace_path" {
  type = string
  # Same reason as source_config_workspace_path above: almanac.stream.runner
  # defaults --config to the relative conf/sources/github_events.yml, which
  # resolves against the repo root for `make`/CI and not for a job task's
  # working directory. Passed explicitly rather than trusting the CWD.
  description = "Workspace path of the synced conf/sources/github_events.yml, passed explicitly."
  default     = "/Workspace/Shared/almanac/conf/sources/github_events.yml"
}

variable "streaming_workers" {
  type = number
  # One worker, not backfill_workers: the ingest stage drains a landing zone
  # holding ~30 polls x ~192 events (measured 2026-09-06, n=30) -- a few
  # thousand rows. Sizing this like the 341M-row backfill would bill four
  # idle nodes to do nothing.
  description = "Workers on the streaming job cluster; the live feed is a sample, not the archive."
  default     = 1
}

variable "streaming_max_polls" {
  type = string
  # A string, not a number: databricks_job task parameters are strings.
  # 30 polls at the server's advertised 60s interval is a ~30-minute window,
  # the same n the Task 1 capture measurement used.
  description = "Poll count for the bounded live window (§4.6)."
  default     = "30"
}

variable "streaming_secret_scope" {
  type = string
  # The GitHub token never enters this repo, a job parameter, or a log line
  # -- it is resolved from a secret scope into GITHUB_TOKEN at cluster start,
  # which is the name conf/sources/github_events.yml's auth.token_env reads.
  description = "Databricks secret scope holding the GitHub token for the poller."
  default     = "almanac"
}

variable "streaming_secret_key" {
  type        = string
  description = "Key within streaming_secret_scope holding the GitHub token."
  default     = "github_token"
}

variable "online_store_capacity" {
  type = string
  # CU_1, the smallest of CU_1|CU_2|CU_4|CU_8. Databricks' docs suggest CU_2
  # for testing; this is a bounded window against a finite credit, and
  # capacity is the one field raisable in place afterwards. The real DBU rate
  # is still unmeasured -- system.billing.list_prices returns nothing for
  # LAKEBASE/POSTGRES/OLTP, the second time this project has hit that gap --
  # so Task 9 confirms it the way Vector Search's was confirmed, by running it.
  description = "Lakebase compute units for the online store (§4.6)."
  default     = "CU_1"
}

variable "online_store_stopped" {
  type = bool
  # Exposed, not relied on. The provider offers an explicit stop, which is
  # not the same thing as the automatic scale-to-zero §4.6 recorded as
  # unsupported -- and whether a stopped instance stops billing compute is
  # UNVERIFIED. Deletion is the teardown known to work; Task 9 measures this
  # one before any claim is made about it.
  description = "Stop the online store instead of deleting it. Billing impact unverified until Task 9."
  default     = false
}

variable "streaming_feature_schema" {
  type = string
  # Fully qualified (catalog.schema), not a bare name: register_feature_table
  # emits "{schema}.{table}", which is a valid Unity Catalog three-part name
  # only if the catalog is already in it.
  description = "UC catalog.schema the streaming feature tables register into."
  default     = "almanac_dbx.features"
}

variable "streaming_online_schema" {
  type = string
  # Deliberately separate from streaming_feature_schema rather than derived:
  # Databricks documents that an online table's *catalog* name must equal its
  # backing Postgres database name, which the source catalog has no reason to
  # satisfy. Task 9 confirms what this actually has to be; publishing into the
  # source catalog is the thing that would fail quietly at serving time.
  description = "UC catalog.schema the published online tables land in (§4.6)."
  default     = "almanac_online.features"
}

variable "streaming_publish_mode" {
  type = string
  # TRIGGERED for a first run: CONTINUOUS provisions a streaming sync pipeline
  # that keeps running -- and therefore keeps billing -- until it is torn down,
  # which is the exact shape Phase 5 got wrong with Vector Search. Task 9's
  # gate needs CONTINUOUS to show a live event moving a served value, so it is
  # switched on deliberately for that step, not left on by default.
  description = "publish_table mode: TRIGGERED or CONTINUOUS."
  default     = "TRIGGERED"
}

variable "streaming_publish_pip_dependencies" {
  type        = list(string)
  description = "The feature-engineering client, needed only by the publish task."
  default     = ["databricks-feature-engineering>=0.17.1"]
}

# Phase 7 (§4.7): the reporting warehouse. Its whole life is a short attended
# window, so both knobs below are set against a default that assumes otherwise.

variable "reporting_warehouse_size" {
  type = string
  # 2X-Small, the smallest the API offers, against a UI default of X-Large.
  # The Phase 7 dashboards read Gold and the serving log, not Bronze's 341M
  # rows, so latency is not the binding constraint -- and raising it is a
  # one-line change, the same knob shape as similarity_sample_size.
  description = "Cluster size for the reporting warehouse (§7's dashboards)."
  default     = "2X-Small"
}

variable "reporting_auto_stop_mins" {
  type = number
  # The Terraform provider's own default is 120: two hours of idle DBUs and
  # cloud instance charges after the last query, which is the exact shape of
  # bill this project's cost rules exist to prevent. Azure Databricks documents
  # the serverless floor as 5 minutes in the UI and as low as 1 via the SQL
  # warehouses API, which is what Terraform drives (Microsoft Learn, "Create a
  # SQL warehouse", updated 2026-08-20). 5 rather than 1 so that reading a
  # dashboard between screenshots does not restart the warehouse per panel.
  description = "Idle minutes before the reporting warehouse stops. Provider default is 120."
  default     = 5
}
