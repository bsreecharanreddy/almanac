output "workspace_url" {
  description = "The ephemeral centralus workspace hosting the Lakebase window."
  value       = "https://${azurerm_databricks_workspace.this.workspace_url}"
}

output "streaming_job_url" {
  description = "Workflows URL for the four-stage streaming job (Task 9's gate)."
  value       = databricks_job.streaming.url
}

output "online_store_state" {
  description = "Lakebase state; publish_table requires AVAILABLE."
  value       = databricks_database_instance.online_store.state
}

output "online_store_read_write_dns" {
  description = "Postgres endpoint, for the served-value query that demonstrates the gate."
  value       = databricks_database_instance.online_store.read_write_dns
}

output "landing_path" {
  description = "FUSE landing zone the poll stage writes and the ingest stage reads."
  value       = local.landing_path
}
