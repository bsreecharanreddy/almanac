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
      version = "~> 1.90"
    }
  }
}

provider "azurerm" {
  features {}
}

# Workspace-level; picks up `az login` credentials for an Azure host, so no
# token here. Only plan/apply touch it -- `terraform validate` does not.
provider "databricks" {
  host = "https://${azurerm_databricks_workspace.this.workspace_url}"
}
