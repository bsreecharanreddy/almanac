# Phase 7 (design doc §4.7): the reporting window. This is the only billable
# resource Phase 7 provisions, and §4.7 narrows the phase's cloud window to it
# alone -- the full-stack demo moves to Phase 8.
#
# It is brought up by `make window-up` and removed by `make window-down`, never
# by a bare `terraform apply`. A bare apply here plans "6 to add" (measured
# 2026-09-07, Task 1): five of those are Phase 5's and Phase 6's deliberately
# destroyed stacks, ~$19/day idle between them, neither scaling to zero.
# almanac.infra.window refuses any plan that reaches past this resource.

resource "databricks_sql_endpoint" "reporting" {
  name             = "${var.prefix}-reporting"
  cluster_size     = var.reporting_warehouse_size
  max_num_clusters = 1

  # Serverless, and PRO stated rather than inferred. The provider defaults
  # warehouse_type to PRO when serverless is on and CLASSIC otherwise; naming
  # it means a future default cannot silently move this to CLASSIC, which
  # would need the vCPU quota this subscription has already committed to job
  # clusters (see var.location).
  enable_serverless_compute = true
  warehouse_type            = "PRO"

  auto_stop_mins = var.reporting_auto_stop_mins

  # Same three keys as every other resource here, derived from var.tags rather
  # than restated, since cost attribution depends on them matching exactly.
  tags {
    dynamic "custom_tags" {
      for_each = var.tags
      content {
        key   = custom_tags.key
        value = custom_tags.value
      }
    }
  }
}

# Deliberately no `output` for the warehouse id or its URL. An output is
# dropped from state the moment the resource it reads is destroyed -- Phase 6's
# teardown watched every output go blank, including the workspace URL, and the
# next Databricks call failed as an *auth* error pointing at credentials rather
# than at the real cause. `almanac.infra.window` reads the id out of
# `terraform show -json` instead, which stays readable through a partial
# destroy. Task 9's dashboards reference the resource directly.
