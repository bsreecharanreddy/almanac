# Phase 6 (design doc §4.6): the live event stream and the Lakebase online
# store it feeds.
#
# Authored here but not applied until Task 9. The online store is the only
# resource in this repo that bills purely for existing -- Databricks documents
# that "Lakebase scale-to-zero is not supported" -- so it is created for a
# bounded window and destroyed at the end of it, and this file ships that
# teardown story rather than deferring it (§11, "teardown ships with
# provisioning"; Phase 5's Vector Search endpoint is the incident behind it).

# The poller writes with plain pathlib (`Path.write_text`, `Path.replace`), so
# this cannot be an abfss:// URI -- the same constraint backfill_staging_dir
# and backfill_checkpoint_dir already carry, and the same one that produced a
# real FILE_NOT_EXIST failure on 2026-09-02. A UC volume is FUSE-mounted
# identically on every node, so the poll stage writes it as a filesystem and
# the ingest stage reads it back as a Spark streaming source.
resource "databricks_volume" "stream_landing" {
  catalog_name = "almanac_dbx"
  schema_name  = "burn"
  name         = "stream_landing"
  volume_type  = "MANAGED"
  comment      = "Landing zone for polled GitHub Events JSONL (streaming Bronze, §4.6)."
}

locals {
  stream_landing_path = "/Volumes/${databricks_volume.stream_landing.catalog_name}/${databricks_volume.stream_landing.schema_name}/${databricks_volume.stream_landing.name}"
}

# Lakebase, via databricks_database_instance -- Public Preview, and the only
# Terraform surface for it. Deliberately NOT databricks_database_synced_
# database_table (Private Preview): the publish is owned by
# almanac.stream.online_store.publish_feature_table, because Databricks' own
# docs warn that deleting a synced table by any path other than the feature-
# engineering API leaves the underlying Postgres storage behind. One owner per
# object.
resource "databricks_database_instance" "online_store" {
  name = "${var.prefix}-online-store"
  # Smallest unit. Databricks' docs suggest CU_2 as a testing starting point;
  # this is a bounded window against a finite credit, and capacity is the one
  # field that can be raised in place later.
  capacity = var.online_store_capacity

  # An input-only knob the provider exposes and §4.6 did not know about when
  # it recorded "scale-to-zero is not supported": an instance can be *stopped*
  # explicitly, which is not the same thing as scaling to zero automatically.
  # Whether a stopped instance stops billing compute is UNVERIFIED -- Task 9
  # measures it the way Vector Search's idle rate was measured, by watching
  # system.billing.usage. Until then, deletion is the teardown that is known
  # to work, and this defaults to false so the knob is visible, not relied on.
  stopped = var.online_store_stopped

  lifecycle {
    # OFF on purpose, and the reason belongs here rather than in a commit
    # message: this resource is provisioned for a bounded measurement window
    # and torn down at the end of Task 9. vector_search.tf carries the
    # mirror-image note for the opposite reason (its guard exists to stop
    # provider drift silently recreating a live index).
    prevent_destroy = false
  }
}

# Two tasks, one cluster, run in order: `poll` is network-bound and needs no
# SparkSession, `ingest` is a Spark streaming query and needs no token. The
# split keeps a failure attributable to one of them (almanac.stream.runner).
resource "databricks_job" "streaming" {
  name        = "${var.prefix}-streaming"
  description = "Poll the live GitHub Events API for a bounded window, then drain the landing zone into Silver. Attended runs only."

  max_concurrent_runs = 1
  tags                = var.tags

  job_cluster {
    job_cluster_key = "streaming"
    new_cluster {
      spark_version      = data.databricks_spark_version.lts.id
      node_type_id       = var.databricks_node_type
      num_workers        = var.streaming_workers
      runtime_engine     = "STANDARD"
      data_security_mode = "SINGLE_USER"
      single_user_name   = data.databricks_current_user.me.user_name
      custom_tags        = var.tags

      # The token reaches the poller as an environment variable resolved from
      # a secret scope at cluster start -- never a job parameter, which would
      # put it in every run's visible parameter list. resolve_token() reads
      # exactly this name (conf/sources/github_events.yml, auth.token_env).
      spark_env_vars = {
        GITHUB_TOKEN = "{{secrets/${var.streaming_secret_scope}/${var.streaming_secret_key}}}"
      }

      cluster_log_conf {
        volumes {
          destination = "/Volumes/${databricks_volume.cluster_logs.catalog_name}/${databricks_volume.cluster_logs.schema_name}/${databricks_volume.cluster_logs.name}"
        }
      }
    }
  }

  task {
    task_key        = "poll"
    job_cluster_key = "streaming"

    spark_python_task {
      python_file = var.streaming_python_file
      source      = "WORKSPACE"
      parameters = [
        "poll",
        "--landing", local.stream_landing_path,
        # Explicit, never the script's relative default: a job task's working
        # directory is not the repo root, which cost a real run a
        # FileNotFoundError on 2026-09-02 (source_config_workspace_path).
        "--config", var.event_stream_config_workspace_path,
        # Bounded by construction. At the measured 60s poll interval this is
        # a ~30-minute window capturing ~7% of the firehose -- a sample, not a
        # mirror, which is why Task 4's replay harness owns the completeness
        # proof (docs/findings/2026-09-06-events-api-and-online-store-rates.md).
        "--max-polls", var.streaming_max_polls,
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

  task {
    task_key        = "ingest"
    job_cluster_key = "streaming"

    depends_on {
      task_key = "poll"
    }

    spark_python_task {
      python_file = var.streaming_python_file
      source      = "WORKSPACE"
      parameters = [
        "ingest",
        "--landing", local.stream_landing_path,
        "--silver-path", "${local.lake.silver}/events_stream",
        # abfss://, unlike the landing zone above: this one is only ever
        # touched by Spark's own checkpointLocation, never by pathlib.
        "--checkpoint", "${local.lake.silver}/events_stream_checkpoint",
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

output "streaming_job_url" {
  description = "Databricks Workflows URL for the bounded live-streaming window (Task 9)."
  value       = databricks_job.streaming.url
}

output "online_store_state" {
  description = "Lakebase instance state; publish_table requires AVAILABLE (Task 9 step 4)."
  value       = databricks_database_instance.online_store.state
}

output "online_store_read_write_dns" {
  description = "Postgres endpoint for the online store, for the served-feature query Task 9 demonstrates."
  value       = databricks_database_instance.online_store.read_write_dns
}
