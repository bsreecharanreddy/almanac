# Mosaic AI Vector Search (Phase 5, design doc §8.3a). The product renamed
# to "AI Search" at the PyPI/SDK layer (databricks-ai-search is canonical,
# databricks-vectorsearch a deprecated shim -- confirmed live 2026-09-04),
# but Terraform's own resource type names have not: these are still the GA,
# non-preview surface, unlike the CLI's newer `ai-search` command group,
# which is marked Public Preview (docs/findings/2026-09-04-phase-5-corpus-
# and-index-choice.md).
#
# Both resources are authored here but cannot successfully apply until
# Task 7's real run: the index's source_table must already exist,
# UC-registered, with Change Data Feed enabled -- which only a real run of
# run_embedding_pipeline (register=True) creates. Same "provision now, apply
# once the real dependency exists" shape as databricks_model_serving's
# entity_version in databricks.tf.

resource "databricks_vector_search_endpoint" "embeddings" {
  name = "${var.prefix}-embeddings"
  # STANDARD, not STORAGE_OPTIMIZED: the CLI's own --help lists both, but
  # the provider's current schema (checked live against the registry,
  # 2026-09-04) accepts only STANDARD -- and STANDARD is what Task 1's
  # sizing (~4 units for 14.9M vectors) was measured against.
  endpoint_type = "STANDARD"
}

resource "databricks_vector_search_index" "pr_issue_embeddings" {
  name          = "${var.model_registry_catalog}.${var.embeddings_schema}.pr_issue_embeddings_index"
  endpoint_name = databricks_vector_search_endpoint.embeddings.name
  primary_key   = "entity_key"
  index_type    = "DELTA_SYNC"

  delta_sync_index_spec {
    source_table = "${var.model_registry_catalog}.${var.embeddings_schema}.pr_issue_embeddings"
    # TRIGGERED, not CONTINUOUS: attended runs only, the same convention
    # every other job in this project follows. A continuous pipeline would
    # keep a cluster warm between sessions for no reason on a project with a
    # fixed credit.
    pipeline_type = "TRIGGERED"

    # Pre-computed offline (sentence-transformers, not a Databricks-hosted
    # embedding model endpoint) -- embedding_vector_columns, not
    # embedding_source_columns, points the index at the column
    # run_embedding_pipeline already wrote, rather than asking Vector Search
    # to compute embeddings itself.
    embedding_vector_columns {
      name                = "embedding"
      embedding_dimension = var.embedding_dimension
    }
  }

  # The provider reads `endpoint_id` and `index_subtype` (both server-assigned
  # -- HYBRID is the default subtype) as drifting to null on every plan, which
  # forces a replace. Measured 2026-09-05: a plain `terraform apply` would
  # destroy and recreate the index, throwing away its sync. Neither attribute
  # is something this config sets, so ignoring them is safe.
  lifecycle {
    ignore_changes  = [endpoint_id, index_subtype]
    prevent_destroy = true
  }
}

output "vector_search_endpoint_url" {
  description = "Vector Search endpoint URL (Task 4/5 query against the index this endpoint hosts)."
  value       = "https://${azurerm_databricks_workspace.this.workspace_url}/api/2.0/vector-search/endpoints/${databricks_vector_search_endpoint.embeddings.name}"
}
