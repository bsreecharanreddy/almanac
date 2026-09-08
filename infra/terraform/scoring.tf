# Phase 7 Task 9a: batch-score the quarter, so §7's page 1 has a score to rank by.
#
# The served endpoint returns a **class**, not a probability -- `train.py` logs
# the champion with a signature inferred from `model.predict()`, so every row in
# Task 1's inference table is a boolean. A boolean cannot rank an intervention
# queue, which is what §5 promises and what page 1 draws. This job produces the
# score offline instead, beside the true outcome, from the same registered
# champion, over the whole quarter rather than over 24 live predictions.
#
# Nothing here provisions compute on apply. It is started by hand in Task 10's
# attended window, like every other job in this stack.

resource "databricks_job" "score_quarter" {
  name        = "${var.prefix}-score-quarter"
  description = "Phase 7: score Gold's trainable population with the registered champion's predict_proba. Attended runs only."

  max_concurrent_runs = 1
  tags                = var.tags

  job_cluster {
    job_cluster_key = "score"
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
    task_key        = "score"
    job_cluster_key = "score"

    spark_python_task {
      python_file = var.score_python_file
      source      = "WORKSPACE"
      parameters = [
        "--silver-path", "${local.lake.silver}/events",
        "--features-path", "${local.lake.features}/events",
        "--gold-table", var.model_gold_table,
        "--out-path", "${local.lake.features}/${var.predictions_dir}",
        # The champion registered by the training job. A version, never
        # @champion by alias: a scored table whose model can change under it
        # is not reproducible, which is the property this table exists to have.
        "--model-uri", "models:/${var.model_registry_catalog}.${var.model_registry_schema}.pr_review_sla_risk/1",
        "--registry-uri", "databricks-uc",
        # The same §5.3 constant the training job was given. Reused, never
        # recomputed here -- two places deriving one threshold is two thresholds.
        "--threshold-seconds", "1487",
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

output "score_quarter_job_url" {
  description = "Databricks Workflows URL for Phase 7's batch scoring job."
  value       = databricks_job.score_quarter.url
}
