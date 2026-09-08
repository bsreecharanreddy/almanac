# Phase 7 Task 9b (design doc §4.7): §7's three report pages, as code.
#
# All three are AI/BI dashboards. §7 originally called for Power BI; §4.7
# supersedes that -- Power BI Desktop is Windows-only, publishing from Databricks
# needs a Premium/PPU/Fabric licence, and the one free path is restricted to a
# personal workspace that cannot share. `databricks_dashboard` takes a
# `file_path` to committed JSON, so these are diffable, reviewable and
# destroyable like every other resource here, where a .pbix is a binary blob.
#
# They point at the reporting warehouse, so `make window-down` takes the
# warehouse and leaves the definitions; nothing here bills while it is down.

locals {
  # The dashboards are keyed by their committed file so a new page is one map
  # entry rather than a copied resource block.
  dashboards = {
    "review-sla-risk" = {
      display_name = "Almanac — Review SLA Risk"
      file         = "01-review-sla-risk.json"
    }
    "model-platform-health" = {
      display_name = "Almanac — Model & Platform Health"
      file         = "02-model-platform-health.json"
    }
    "developer-engagement" = {
      display_name = "Almanac — Developer Engagement"
      file         = "03-developer-engagement.json"
    }
  }
}

resource "databricks_dashboard" "reporting" {
  for_each = local.dashboards

  display_name = each.value.display_name
  warehouse_id = databricks_sql_endpoint.reporting.id
  parent_path  = var.dashboard_parent_path
  file_path    = "${path.module}/../../dashboards/${each.value.file}"

  # false, not the provider's default of true: an embedded credential lets a
  # viewer run these queries as the publisher, and Gold carries real actor and
  # repo names even though no panel renders one (docs/pseudonymization.md).
  embed_credentials = false

  # `file_path` and `serialized_dashboard` cannot be swapped in place -- the
  # provider requires a destroy and recreate to move between them. Committed
  # JSON is the whole point here, so this never changes.
  lifecycle {
    ignore_changes = [serialized_dashboard]
  }
}

output "dashboard_urls" {
  description = "Databricks AI/BI URLs for §7's three report pages."
  value       = { for key, dashboard in databricks_dashboard.reporting : key => dashboard.id }
}
