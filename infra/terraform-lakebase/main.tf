# Phase 6 Task 9: a second, disposable stack that exists for exactly one
# reason -- **Lakebase is not available in westus3**, where the main stack's
# workspace lives, and a Lakebase project's region is inherited from its
# workspace and cannot be changed (Microsoft Learn's own region list, checked
# live 2026-09-06: 19 regions, westus3 absent). Confirmed against the live
# API before concluding: /api/2.0/database/instances and
# /api/2.0/postgres/projects both fail or hang from the westus3 workspace
# while an unrelated control API answers instantly.
#
# **centralus, not westus2**, and that correction was measured rather than
# assumed: `az vm list-skus` reports Standard_D4ds_v6 as
# NotAvailableForSubscription in westus2 but unrestricted in centralus. This
# is the same trap docs/design's own gate-1 rule was written for -- an
# earlier "SKUs are restricted subscription-wide" claim drawn from two
# regions was wrong in exactly this way.
#
# Deliberately a separate root module with its own state, not a branch of the
# main one: everything here is created for a bounded measurement window and
# destroyed at the end of it, and a `terraform destroy` in this directory
# must not be able to reach the 341M-row quarter, the embeddings table or the
# MLflow experiments that live in the westus3 stack.

terraform {
  required_version = ">= 1.9"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.130"
    }
  }
}

provider "azurerm" {
  features {}
}

provider "databricks" {
  host = "https://${azurerm_databricks_workspace.this.workspace_url}"
}

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

# The live poller produces its own data, so this stack needs no access to the
# westus3 lake at all -- which is what makes it independently destroyable.
resource "azurerm_storage_account" "lake" {
  # Hyphens stripped: storage account names are lowercase-alphanumeric only,
  # 3-24 chars, and this stack's prefix carries a hyphen where the main
  # stack's does not -- so `almanac-lblake…` is rejected where `almanaclake…`
  # was fine. Caught on the first apply, not by validate.
  name                     = "${replace(var.prefix, "-", "")}lake${random_string.suffix.result}"
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true

  allow_nested_items_to_be_public = false
  min_tls_version                 = "TLS1_2"

  tags = var.tags
}

resource "azurerm_storage_container" "stream" {
  name               = "stream"
  storage_account_id = azurerm_storage_account.lake.id
}

resource "azurerm_databricks_workspace" "this" {
  name                        = "${var.prefix}-dbx"
  resource_group_name         = azurerm_resource_group.this.name
  location                    = azurerm_resource_group.this.location
  sku                         = "premium"
  managed_resource_group_name = "${var.prefix}-dbx-managed"
  tags                        = var.tags
}

locals {
  stream_root = "abfss://${azurerm_storage_container.stream.name}@${azurerm_storage_account.lake.name}.dfs.core.windows.net"
}
