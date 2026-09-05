# Phase 5's embedding pipeline (design doc §8.3a): Bronze -> the embeddings
# table Task 3's Vector Search index syncs from. Same 5-VM shape as the
# other medallion jobs, but the parallelism actually matters here --
# run_embedding_pipeline_distributed's mapInPandas step is what uses the
# worker vCPUs; a single-node encode over the real 14.9M-text corpus at
# Task 1's measured 151.1 texts/sec would be ~27h, not an attended run.
#
# --register writes the UC external table Task 3's databricks_vector_
# search_index.source_table names. A fresh DELTA_SYNC index performs its
# own initial full sync at creation time (its own `apply`, after this job
# has run at least once); a later re-sync (new Bronze data, same index)
# is a `databricks vector-search-indexes sync-index` CLI call, not
# something this job or Terraform triggers on its own.
resource "databricks_job" "embeddings" {
  name        = "${var.prefix}-embeddings"
  description = "Embed Bronze's opened PR/issue text incrementally. Attended runs only."

  max_concurrent_runs = 1
  tags                = var.tags

  job_cluster {
    job_cluster_key = "embeddings"
    new_cluster {
      spark_version      = data.databricks_spark_version.lts.id
      node_type_id       = var.databricks_node_type
      num_workers        = var.backfill_workers
      runtime_engine     = "STANDARD"
      data_security_mode = "SINGLE_USER"
      single_user_name   = data.databricks_current_user.me.user_name
      custom_tags        = var.tags
      # Diagnostics parity with the feature-build/train clusters: a job
      # cluster self-terminates and takes its logs with it. Behaviour-neutral.
      cluster_log_conf {
        volumes {
          destination = "/Volumes/${databricks_volume.cluster_logs.catalog_name}/${databricks_volume.cluster_logs.schema_name}/${databricks_volume.cluster_logs.name}"
        }
      }
    }
  }

  task {
    task_key        = "embed"
    job_cluster_key = "embeddings"

    spark_python_task {
      python_file = var.embeddings_python_file
      source      = "WORKSPACE"
      parameters = [
        "--bronze-path", "${local.lake.bronze}/events",
        "--embeddings-path", "${local.lake.features}/embeddings",
        "--schema", var.embeddings_schema,
        # Bounds load_encoder to 16 calls total, matching this job cluster's
        # own 4-worker/16-vCPU shape -- measured 2026-09-05 that Spark's
        # default partition count (967) turned a ~1.7h estimate into 50+
        # real hours, dominated by per-partition model-load overhead.
        "--num-partitions", "16",
        "--register",
      ]
    }

    library {
      whl = var.almanac_wheel
    }

    dynamic "library" {
      for_each = var.embeddings_pip_dependencies
      content {
        pypi {
          package = library.value
        }
      }
    }
  }
}

output "embeddings_job_url" {
  description = "Databricks Workflows URL for Phase 5's embedding pipeline."
  value       = databricks_job.embeddings.url
}
