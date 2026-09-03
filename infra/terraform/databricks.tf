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
        # This arm times Gold too, so it needs the same dbt log redirect the
        # gold job has: dbt logs relative to --project-dir, a workspace path.
        #
        # The schemas are per arm because the arms share one metastore and run
        # concurrently. Sharing them made run 693303490917119's Gold row
        # unusable: `CREATE TABLE IF NOT EXISTS` let one arm win `silver.events`
        # so the other read its data, and both merged into the same managed
        # gold tables. Also keeps the arms clear of the real `silver`/`gold`,
        # which the gold job owns.
        spark_env_vars = {
          DBT_LOG_PATH          = "/local_disk0/dbt_logs"
          ALMANAC_SILVER_SCHEMA = "ab_${job_cluster.key}_silver"
          ALMANAC_GOLD_SCHEMA   = "ab_${job_cluster.key}_gold"
        }
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
          #
          # Per arm, not shared. The two arms carry no depends_on, so they run
          # concurrently, and both measure the same calibration day -- pointed
          # at one directory they would race on identical filenames:
          # fetch_hour does not skip an existing file, it re-downloads and
          # write_bytes/replace's the same 2025-08-13-N.json.gz.part out from
          # under the other cluster while Spark may be reading it. A subdir
          # per arm also keeps each arm's fetch independently measured, and
          # leaves the 7 leftover 2025-09-30 files (open item in
          # 2026-09-02-burn-deploy-and-first-run-defects.md) undisturbed.
          "--staging-dir", "${var.backfill_staging_dir}/photon_ab_${task.value}",
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
          # Explicit for the same reason --source-config is: GoldTarget's
          # default project dir is the relative "dbt", and a job task's cwd is
          # not the repo root (defect #3). --target-path off the workspace,
          # which the cluster cannot write to.
          "--project-dir", var.gold_project_dir,
          "--target-path", "/local_disk0/dbt_target",
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

# Gold on the real lake. The backfill's process_day lands bronze+silver only,
# so until this runs, dim_repo / fact_pull_request / agg_repo_daily have only
# ever been built over 2,000-row fixtures.
resource "databricks_job" "gold" {
  name        = "${var.prefix}-gold"
  description = "dbt build for Gold over the backfilled Silver. Attended runs only."

  max_concurrent_runs = 1
  tags                = var.tags

  job_cluster {
    job_cluster_key = "gold"
    new_cluster {
      spark_version      = data.databricks_spark_version.lts.id
      node_type_id       = var.databricks_node_type
      num_workers        = var.backfill_workers
      runtime_engine     = var.enable_photon ? "PHOTON" : "STANDARD"
      data_security_mode = "SINGLE_USER"
      single_user_name   = data.databricks_current_user.me.user_name
      custom_tags        = var.tags
      # dbt writes its log dir relative to --project-dir, which here is a
      # workspace path the cluster cannot write to.
      spark_env_vars = {
        DBT_LOG_PATH = "/local_disk0/dbt_logs"
      }
    }
  }

  task {
    task_key        = "gold"
    job_cluster_key = "gold"

    spark_python_task {
      python_file = var.gold_python_file
      source      = "WORKSPACE"
      parameters = [
        # The base dir the backfill wrote, holding clean/ and quarantine/;
        # register_silver_sources appends both. A str, not a Path, all the way
        # down -- Path collapses the '//' and Spark answers "Missing cloud file
        # system scheme" (measured 2026-09-02, the first A/B arm died on it).
        "--silver-path", "${local.lake.silver}/events",
        "--warehouse", "${local.lake.gold}/warehouse",
        # Ephemeral, and correct for a *first* full build: there is nothing to
        # be incremental from, so a fresh catalog loses nothing. It is NOT
        # correct for a second run -- dim_repo is a dbt snapshot, and SCD2's
        # multi-version path needs catalog state that outlives the cluster.
        # Choosing that store (UC vs Derby-over-FUSE, still unverified for file
        # locking) is its own decision, not a default to back into here.
        "--metastore", "/local_disk0/metastore",
        "--project-dir", var.gold_project_dir,
        "--profiles-dir", var.gold_project_dir,
        "--target-path", "/local_disk0/dbt_target",
        # REMAINDER: every flag must precede the dbt command.
        "build",
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

output "gold_job_url" {
  description = "Databricks Workflows URL for the Gold dbt build."
  value       = databricks_job.gold.url
}
