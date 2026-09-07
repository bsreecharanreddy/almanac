# Unity Catalog is not optional here: publish_table accepts only UC feature
# tables. The metastore is expected to be auto-provisioned for this region
# (the westus3 one is named `metastore_azure_westus3`, which is the automatic
# naming pattern); if it is not, an account admin assigns one before apply.

data "databricks_current_user" "me" {}

data "databricks_spark_version" "lts" {
  long_term_support = true
}

# Managed identity for UC to reach the storage account. Regional, so it
# cannot be shared with the westus3 stack's connector.
resource "azurerm_databricks_access_connector" "uc" {
  name                = "${var.prefix}-lake-access-connector"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tags                = var.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "uc_storage" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.uc.identity[0].principal_id
}

resource "databricks_storage_credential" "lake" {
  name = "${var.prefix}-lake"

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.uc.id
  }

  # The role assignment is what makes the credential usable; without this the
  # first external-location validation races it and fails.
  depends_on = [azurerm_role_assignment.uc_storage]
}

resource "databricks_external_location" "stream" {
  name            = "${var.prefix}-stream"
  url             = local.stream_root
  credential_name = databricks_storage_credential.lake.id
  comment         = "Streaming Silver and feature tables for the bounded Lakebase window."
}

# An explicit storage_root rather than the metastore default: the
# auto-provisioned metastore reports no storage root, so a catalog relying on
# it would fail to create.
resource "databricks_catalog" "this" {
  name          = var.catalog
  storage_root  = local.stream_root
  comment       = "Phase 6 Task 9 -- ephemeral, destroyed with this stack."
  force_destroy = true

  depends_on = [databricks_external_location.stream]
}

resource "databricks_schema" "features" {
  catalog_name  = databricks_catalog.this.name
  name          = var.schema
  force_destroy = true
}

# The poller writes with plain pathlib, so its landing zone cannot be an
# abfss:// URI -- a UC volume is FUSE-mounted identically on every node. Same
# constraint that produced a real FILE_NOT_EXIST failure in the main stack on
# 2026-09-02.
resource "databricks_volume" "landing" {
  catalog_name = databricks_catalog.this.name
  schema_name  = databricks_schema.features.name
  name         = "stream_landing"
  volume_type  = "MANAGED"
  comment      = "Landing zone for polled GitHub Events JSONL (streaming Bronze)."
}

locals {
  landing_path   = "/Volumes/${databricks_catalog.this.name}/${databricks_schema.features.name}/${databricks_volume.landing.name}"
  silver_path    = "${local.stream_root}/silver/events_stream"
  checkpoint     = "${local.stream_root}/silver/events_stream_checkpoint"
  features_path  = "${local.stream_root}/features/events_stream"
  feature_schema = "${databricks_catalog.this.name}.${databricks_schema.features.name}"
}

# The whole reason this stack exists. Lakebase bills for existing -- an online
# feature store does not scale to zero, unlike Lakebase Postgres generally --
# so it is created for a bounded window and destroyed at the end of it.
resource "databricks_database_instance" "online_store" {
  name     = "${var.prefix}-online-store"
  capacity = var.online_store_capacity

  lifecycle {
    # OFF on purpose: this resource is meant to be destroyed, and a guard
    # here would block the teardown that is the whole cost-control story.
    prevent_destroy = false
  }
}

resource "databricks_job" "streaming" {
  name        = "${var.prefix}-streaming"
  description = "Poll the live GitHub Events API, land it, build online features, publish to Lakebase. Attended runs only."

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

      # The token is resolved from a secret scope at cluster start, never a
      # job parameter -- a parameter would put it in every run's visible
      # parameter list. resolve_token() reads exactly this name.
      spark_env_vars = {
        GITHUB_TOKEN = "{{secrets/${var.streaming_secret_scope}/${var.streaming_secret_key}}}"
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
        "--landing", local.landing_path,
        "--config", var.event_stream_config_workspace_path,
        "--max-polls", var.streaming_max_polls,
      ]
    }

    library {
      whl = var.almanac_wheel
    }

    dynamic "library" {
      for_each = var.streaming_pip_dependencies
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
        "--landing", local.landing_path,
        "--silver-path", local.silver_path,
        "--checkpoint", local.checkpoint,
      ]
    }

    library {
      whl = var.almanac_wheel
    }

    dynamic "library" {
      for_each = var.streaming_pip_dependencies
      content {
        pypi {
          package = library.value
        }
      }
    }
  }

  task {
    task_key        = "features"
    job_cluster_key = "streaming"

    depends_on {
      task_key = "ingest"
    }

    spark_python_task {
      python_file = var.streaming_python_file
      source      = "WORKSPACE"
      parameters = [
        "features",
        "--silver-path", local.silver_path,
        "--features-path", local.features_path,
        "--schema", local.feature_schema,
        # Emits the TIMESERIES primary key, the NOT NULL keys and the Change
        # Data Feed property publish_table requires. Without it the publish
        # stage fails against a store that is already billing.
        "--register",
      ]
    }

    library {
      whl = var.almanac_wheel
    }

    dynamic "library" {
      for_each = var.streaming_pip_dependencies
      content {
        pypi {
          package = library.value
        }
      }
    }
  }

  task {
    task_key        = "publish"
    job_cluster_key = "streaming"

    depends_on {
      task_key = "features"
    }

    spark_python_task {
      python_file = var.streaming_python_file
      source      = "WORKSPACE"
      parameters = [
        "publish",
        "--store-name", databricks_database_instance.online_store.name,
        "--schema", local.feature_schema,
        # A catalog of its own: Databricks requires an online table's catalog
        # name to equal its backing Postgres database name, which the source
        # catalog has no reason to satisfy. publish_table creates it.
        "--online-schema", "${var.prefix_online_catalog}.${var.schema}",
        "--publish-mode", "TRIGGERED",
      ]
    }

    library {
      whl = var.almanac_wheel
    }

    dynamic "library" {
      for_each = var.streaming_pip_dependencies
      content {
        pypi {
          package = library.value
        }
      }
    }
  }
}
