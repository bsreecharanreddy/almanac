# Task 4/6's real run (design doc §8.3a): compute pr_similarity against
# the live index, then compare with/without it against the registered
# champion (0.612 PR-AUC, src/almanac/model/similarity_comparison.py).
# Both jobs depend on Task 3's index existing and having synced, which in
# turn depends on the embeddings job (embeddings.tf) having run at least
# once -- none of that is expressible as a Terraform dependency (the
# dependency is "a real run happened", not a resource), so the run order
# is a real-run procedure, not enforced by `apply`.

resource "databricks_job" "pr_similarity" {
  name        = "${var.prefix}-pr-similarity"
  description = "Compute pr_similarity over a (possibly sampled) real spine against the live Vector Search index. Attended runs only."

  max_concurrent_runs = 1
  tags                = var.tags

  job_cluster {
    job_cluster_key = "similarity"
    new_cluster {
      spark_version      = data.databricks_spark_version.lts.id
      node_type_id       = var.databricks_node_type
      num_workers        = var.backfill_workers
      runtime_engine     = "STANDARD"
      data_security_mode = "SINGLE_USER"
      single_user_name   = data.databricks_current_user.me.user_name
      custom_tags        = var.tags
      cluster_log_conf {
        volumes {
          destination = "/Volumes/${databricks_volume.cluster_logs.catalog_name}/${databricks_volume.cluster_logs.schema_name}/${databricks_volume.cluster_logs.name}"
        }
      }
    }
  }

  task {
    task_key        = "pr_similarity"
    job_cluster_key = "similarity"

    spark_python_task {
      python_file = var.similarity_python_file
      source      = "WORKSPACE"
      # --since-date must match the embeddings job's own window: the index
      # only holds PRs opened on or after it, and compute_pr_similarity's
      # point-in-time filter only returns older neighbors, so a spine
      # reaching back further finds nothing for those rows
      # (docs/findings/2026-09-05-embedding-worker-fork-deadlock.md -- the
      # first real run made exactly this mistake).
      parameters = concat(
        var.embeddings_since != "" ? ["--since-date", var.embeddings_since] : [],
        [
          "--silver-path", "${local.lake.silver}/events",
          "--embeddings-path", "${local.lake.features}/embeddings",
          "--similarity-path", "${local.lake.features}/pr_similarity",
          "--gold-table", var.model_gold_table,
          "--threshold-seconds", "1487",
          "--endpoint-name", databricks_vector_search_endpoint.embeddings.name,
          "--index-name", databricks_vector_search_index.pr_issue_embeddings.name,
          # Bounded, not the full PR-opened population: each spine row costs
          # one real call against a paid, live endpoint, not a Spark-
          # distributable transform. Sized from the real single-query
          # latency measured 2026-09-05 (~34 ms from the cluster).
          "--sample-size", var.similarity_sample_size,
        ],
      )
    }

    library {
      whl = var.almanac_wheel
    }

    dynamic "library" {
      for_each = var.similarity_pip_dependencies
      content {
        pypi {
          package = library.value
        }
      }
    }
  }
}

output "pr_similarity_job_url" {
  description = "Databricks Workflows URL for Task 4's real similarity computation."
  value       = databricks_job.pr_similarity.url
}

resource "databricks_job" "similarity_comparison" {
  name        = "${var.prefix}-similarity-comparison"
  description = "Compare with/without pr_similarity against the registered champion. Attended runs only."

  max_concurrent_runs = 1
  tags                = var.tags

  job_cluster {
    job_cluster_key = "similarity_comparison"
    new_cluster {
      spark_version      = data.databricks_spark_version.lts.id
      node_type_id       = var.databricks_node_type
      num_workers        = var.backfill_workers
      runtime_engine     = "STANDARD"
      data_security_mode = "SINGLE_USER"
      single_user_name   = data.databricks_current_user.me.user_name
      custom_tags        = var.tags
      cluster_log_conf {
        volumes {
          destination = "/Volumes/${databricks_volume.cluster_logs.catalog_name}/${databricks_volume.cluster_logs.schema_name}/${databricks_volume.cluster_logs.name}"
        }
      }
    }
  }

  task {
    task_key        = "similarity_comparison"
    job_cluster_key = "similarity_comparison"

    spark_python_task {
      python_file = var.similarity_comparison_python_file
      source      = "WORKSPACE"
      parameters = [
        "--silver-path", "${local.lake.silver}/events",
        "--features-path", "${local.lake.features}/events",
        "--gold-table", var.model_gold_table,
        "--threshold-seconds", "1487",
        "--similarity-path", "${local.lake.features}/pr_similarity",
        "--tracking-uri", "databricks",
        "--experiment-name", "/Shared/almanac/pr-similarity-comparison",
        "--catalog", var.model_registry_catalog,
        "--schema", var.model_registry_schema,
        "--register",
      ]
    }

    library {
      whl = var.almanac_wheel
    }

    dynamic "library" {
      for_each = var.model_pip_dependencies
      content {
        pypi {
          package = library.value
        }
      }
    }
  }
}

output "similarity_comparison_job_url" {
  description = "Databricks Workflows URL for Task 6's real with/without comparison."
  value       = databricks_job.similarity_comparison.url
}
