output "workspace_url" {
  description = "Databricks workspace URL."
  value       = "https://${azurerm_databricks_workspace.this.workspace_url}"
}

output "storage_account" {
  description = "ADLS Gen2 account holding the medallion containers."
  value       = azurerm_storage_account.lake.name
}

output "resource_group" {
  value = azurerm_resource_group.this.name
}

output "databricks_sku" {
  description = "premium is required for Unity Catalog."
  value       = azurerm_databricks_workspace.this.sku
}
