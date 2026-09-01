# Storage account names are globally unique across all of Azure and limited
# to 3-24 lowercase alphanumeric characters. A fixed name would collide;
# a random suffix keeps `destroy` + `apply` cycles working.
resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

resource "azurerm_resource_group" "this" {
  name     = "${var.prefix}-rg"
  location = var.location
  tags     = var.tags
}

# ADLS Gen2. `is_hns_enabled` is what makes this Gen2 rather than plain
# blob storage -- without the hierarchical namespace, directory operations
# are emulated and Delta's file layout performs badly.
resource "azurerm_storage_account" "lake" {
  name                     = "${var.prefix}lake${random_string.suffix.result}"
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true

  # No public blob access: the lake holds derived data about real people
  # (design doc §11, pseudonymization).
  allow_nested_items_to_be_public = false
  min_tls_version                 = "TLS1_2"

  tags = var.tags
}

resource "azurerm_storage_container" "medallion" {
  for_each           = toset(["bronze", "silver", "gold", "features"])
  name               = each.key
  storage_account_id = azurerm_storage_account.lake.id
}

# The workspace itself accrues no DBUs while no cluster runs. Clusters are
# created by later phases and never here, so `apply` is close to free and
# the credit is spent only during an actual backfill.
resource "azurerm_databricks_workspace" "this" {
  name                        = "${var.prefix}-dbx"
  resource_group_name         = azurerm_resource_group.this.name
  location                    = azurerm_resource_group.this.location
  sku                         = var.databricks_sku
  managed_resource_group_name = "${var.prefix}-dbx-managed"
  tags                        = var.tags
}
