# Direct abfss:// access to the medallion lake (bronze/silver/gold/features)
# needs its own Unity Catalog storage credential. The existing `almanac_dbx`
# credential (visible via `databricks storage-credentials list`) only covers
# the metastore's own managed storage account (`dbstoragecyy2suzhxmuq4`) --
# that's what backs the UC volumes `backfill_checkpoint_dir` and
# `photon_ab_out_dir` use, not this project's own lake
# (`azurerm_storage_account.lake`, e.g. `almanaclakekoctmh...`).
#
# Measured 2026-09-02: the third real backfill run got past every prior fix
# (checkpoint dir, deploy path, --source-config, fs.defaultFS) and failed 88s
# in, writing Bronze for real, with "Invalid configuration value detected for
# fs.azure.account.key" -- there was no credential path to this storage
# account at all, Unity Catalog or legacy key-based. `terraform plan` never
# caught it because nothing here was missing a required argument; the gap was
# an entire resource that was never declared.

resource "azurerm_databricks_access_connector" "lake" {
  name                = "${var.prefix}-lake-access-connector"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location
  tags                = var.tags

  identity {
    type = "SystemAssigned"
  }
}

# Grants the connector's managed identity read/write on the storage account
# itself. Unity Catalog's storage credential below is what turns that into
# something a cluster can actually use from Spark.
resource "azurerm_role_assignment" "lake_access" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.lake.identity[0].principal_id
}

resource "databricks_storage_credential" "lake" {
  name = "${var.prefix}-lake"
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.lake.id
  }
  comment = "Managed identity credential for the almanac medallion lake storage account (bronze/silver/gold/features)."

  # Databricks validates the credential can actually reach the storage
  # account at creation time; the IAM role assignment has to exist first,
  # not just be queued. Azure role-assignment propagation can still lag a
  # role assignment's own API success by up to a few minutes -- if `apply`
  # ever fails here with a permission error despite this depends_on, it's
  # that propagation delay, not a wiring bug; re-running apply is the fix,
  # not skip_validation.
  depends_on = [azurerm_role_assignment.lake_access]
}

resource "databricks_external_location" "lake" {
  for_each = toset(["bronze", "silver", "gold", "features"])

  name            = "${var.prefix}-lake-${each.key}"
  url             = "abfss://${each.key}@${azurerm_storage_account.lake.name}.dfs.core.windows.net"
  credential_name = databricks_storage_credential.lake.id
  comment         = "Direct abfss:// access to the ${each.key} container for spark_python_task jobs writing outside Unity Catalog managed tables."
}

# READ_FILES/WRITE_FILES, not a catalog/schema grant: the backfill and
# Photon A/B jobs write Delta tables by path (df.write.save(path)), never
# through a UC-managed table, so external-location file grants are what
# actually gates them -- not USE_CATALOG/USE_SCHEMA.
resource "databricks_grants" "lake" {
  for_each = databricks_external_location.lake

  external_location = each.value.id
  grant {
    principal  = data.databricks_current_user.me.user_name
    privileges = ["READ_FILES", "WRITE_FILES", "CREATE_EXTERNAL_TABLE"]
  }
}

# Cluster log delivery for the attended-burn jobs. Task 9, 2026-09-04: the
# feature-build job hung twice in the same Delta write stage with tasks
# pinned at 0 CPU / 0 I/O -- a signature no live Spark UI counter explains.
# The job clusters had no log delivery configured, so every diagnosis so
# far was inference from stage counters. This volume gives the driver and
# executor log4j output (ABFS retry warnings, Delta commit contention) a
# home that outlives a self-terminating job cluster. Same `almanac_dbx.burn`
# managed-storage schema as the staging/checkpoints/photon_ab volumes the
# backfill already uses.
resource "databricks_volume" "cluster_logs" {
  catalog_name = "almanac_dbx"
  schema_name  = "burn"
  name         = "cluster_logs"
  volume_type  = "MANAGED"
  comment      = "Cluster log delivery for attended burn jobs (Task 9 diagnostics)."
}

# The model registry (Phase 4, design doc §5.2): a schema for the
# registered model, holding a managed table -- not a pointer at Delta
# files a job writes by path. mlflow.set_registry_uri("databricks-uc")
# targets `<model_registry_catalog>.<model_registry_schema>.*`.
#
# The catalog is `almanac_dbx`, the metastore's own default-storage
# managed catalog (Task 9, measured 2026-09-04): this account has
# account-level Default Storage and no metastore storage_root, so
# `databricks_catalog` create is refused ("provide a storage location, or
# use the UI") -- the catalog is not Terraform's to make here. The schema
# under it is, and it is where the model lives, aliased @champion.
resource "databricks_schema" "models" {
  catalog_name = var.model_registry_catalog
  name         = var.model_registry_schema
  comment      = "pr_review_sla_risk lives here (Phase 4), aliased @champion."

  # A registered model is state the credit-teardown cycle must not drop;
  # unlike the lake, there is no paid re-burn to recover it.
  force_destroy = false
}

resource "databricks_schema" "serving_logs" {
  catalog_name = var.model_registry_catalog
  name         = var.serving_logs_schema
  comment      = "Inference table for the pr_review_sla_risk endpoint (Phase 7 Task 1)."

  # Same reasoning as `models` above, and it binds harder here. A prediction
  # log is the one artifact in this project that cannot be re-derived at any
  # price: re-provisioning replays no history, because the only traffic that
  # can ever be captured is traffic that happened while capture was on.
  force_destroy = false
}
