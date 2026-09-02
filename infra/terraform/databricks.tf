# Databricks Workflows for the Phase 2 burn. Job clusters only, no schedule
# or trigger: these run only when started by hand during an attended burn,
# and a job cluster self-terminates when the run ends. `terraform destroy`
# between sessions still applies.

data "databricks_current_user" "me" {}

# DBR 17.3 LTS (Spark 4.0.0): the runtime Phase 1's calibration measured on,
# so the burn's per-layer rate stays comparable (2026-09-01-cluster-throughput.md).
data "databricks_spark_version" "lts" {
  long_term_support = true
  spark_version     = "4.0"
}

locals {
  lake = {
    for tier in ["bronze", "silver", "gold"] :
    tier => "abfss://${tier}@${azurerm_storage_account.lake.name}.dfs.core.windows.net"
  }
}

resource "databricks_job" "backfill" {
  name        = "${var.prefix}-tier3-backfill"
  description = "Q3 2025 bronze+silver backfill over the span derived in STATUS.md. Attended runs only."

  max_concurrent_runs = 1
  tags                = var.tags

  job_cluster {
    job_cluster_key = "medallion"
    new_cluster {
      spark_version      = data.databricks_spark_version.lts.id
      node_type_id       = var.databricks_node_type
      num_workers        = var.backfill_workers
      runtime_engine     = var.enable_photon ? "PHOTON" : "STANDARD"
      data_security_mode = "SINGLE_USER"
      single_user_name   = data.databricks_current_user.me.user_name
      custom_tags        = var.tags
    }
  }

  task {
    task_key        = "backfill"
    job_cluster_key = "medallion"

    spark_python_task {
      python_file = var.backfill_python_file
      source      = "WORKSPACE"
      parameters = [
        "--start", var.backfill_start,
        "--end", var.backfill_end,
        "--bronze-path", "${local.lake.bronze}/events",
        "--silver-path", "${local.lake.silver}/events",
        # UC volume, not cluster-local disk: must be visible from every
        # worker node, not just the driver that downloaded it (defect #7).
        "--staging-dir", var.backfill_staging_dir,
        # FUSE path, not abfss://: BackfillCheckpoint is pathlib and must
        # outlive the cluster for a resumed run to skip finished days.
        "--checkpoint-dir", var.backfill_checkpoint_dir,
        "--source-config", var.source_config_workspace_path,
      ]
    }

    library {
      whl = var.almanac_wheel
    }

    dynamic "library" {
      for_each = var.backfill_pip_dependencies
      content {
        pypi {
          package = library.value
        }
      }
    }
  }
}

output "backfill_job_url" {
  description = "Databricks Workflows URL for the Tier 3 backfill job."
  value       = databricks_job.backfill.url
}

# The §8.2 Photon A/B: same slice, same shape, one variable. Two arms on two
# job clusters; `compare` is a manual CLI step once the billing API has the
# per-run DBU totals (24-48h later).
resource "databricks_job" "photon_ab" {
  name        = "${var.prefix}-photon-ab"
  description = "Photon on/off over Phase 1's calibration slice. Attended runs only."

  max_concurrent_runs = 1
  tags                = var.tags

  dynamic "job_cluster" {
    for_each = { standard = "STANDARD", photon = "PHOTON" }
    content {
      job_cluster_key = job_cluster.key
      new_cluster {
        spark_version      = data.databricks_spark_version.lts.id
        node_type_id       = var.databricks_node_type
        num_workers        = var.backfill_workers
        runtime_engine     = job_cluster.value
        data_security_mode = "SINGLE_USER"
        single_user_name   = data.databricks_current_user.me.user_name
        custom_tags        = var.tags
      }
    }
  }

  dynamic "task" {
    for_each = toset(["standard", "photon"])
    content {
      task_key        = "arm_${task.value}"
      job_cluster_key = task.value

      spark_python_task {
        python_file = var.photon_ab_python_file
        source      = "WORKSPACE"
        parameters = [
          # "run": photon_ab.py's main() is subcommand-dispatched
          # (argparse required=True); omitting it fails before any flag
          # is even parsed. Same class of gap as the missing --source-config.
          "run",
          task.value == "photon" ? "--photon" : "--no-photon",
          "--bronze-path", "${local.lake.bronze}/photon_ab_${task.value}",
          "--silver-path", "${local.lake.silver}/photon_ab_${task.value}",
          # Same defect #7 fix as the backfill job: process_day()'s Bronze
          # landing is the identical code path, so the same multi-node
          # staging-dir problem applies here too.
          "--staging-dir", var.backfill_staging_dir,
          # Left on /local_disk0 deliberately: Gold's dbt run hasn't been
          # exercised on this cluster yet, so it's unconfirmed whether it
          # hits the same multi-node problem, and an embedded Derby
          # metastore's file locking is not verified safe over a FUSE
          # volume the way plain file reads are. Flagging, not fixing --
          # revisit if/when a Photon A/B or Gold run actually fails here.
          "--warehouse", "/local_disk0/warehouse",
          "--metastore", "/local_disk0/metastore",
          "--out", "${var.photon_ab_out_dir}/arm_${task.value}.json",
          "--source-config", var.source_config_workspace_path,
        ]
      }

      library {
        whl = var.almanac_wheel
      }

      dynamic "library" {
        for_each = concat(var.backfill_pip_dependencies, var.photon_ab_dbt_dependencies)
        content {
          pypi {
            package = library.value
          }
        }
      }
    }
  }
}

output "photon_ab_job_url" {
  description = "Databricks Workflows URL for the Photon A/B job."
  value       = databricks_job.photon_ab.url
}
