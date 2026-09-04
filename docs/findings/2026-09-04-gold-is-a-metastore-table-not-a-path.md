# Findings — Gold's fact is a metastore table on Databricks, not a Delta path

**Date:** 2026-09-04
**Source:** Phase 4 Task 9's first training run on the workspace
(`961660350236501`), which failed in `build_training_frame`.
**What it changes:** `build_training_frame` reads Gold's `fact_pull_request`
by table name, not by path. Silver `/clean` and the feature tables stay
path reads.

---

## The failure

```
[PATH_NOT_FOUND] Path does not exist:
abfss://gold@almanaclakekoctmh.dfs.core.windows.net/warehouse/gold.db/fact_pull_request
```

`build_training_frame` was reading Gold's fact as path Delta at
`{gold_warehouse}/gold.db/fact_pull_request`, the same layout the local
Gold tests read (`tests/integration/test_gold_fact_pull_request.py` does
`spark.read.format("delta").load(warehouse / "gold.db" / "fact_pull_request")`).
Nothing is at that path on the lake.

## Why local and cloud differ

The Gold job runs dbt through its `session` method against a Derby
metastore and `spark.sql.warehouse.dir = abfss://gold@.../warehouse`.

- **Locally**, `CREATE TABLE gold.fact_pull_request` with that warehouse
  dir writes a managed table to `<warehouse>/gold.db/fact_pull_request` --
  a plain Delta directory, path-readable.
- **On the workspace**, the cluster's Unity Catalog default catalog
  (`almanac_dbx`) intercepts the unqualified `gold.fact_pull_request`, and
  the table is created as `almanac_dbx.gold.fact_pull_request`, stored in
  the metastore's own managed location
  (`abfss://unity-catalog-storage@dbstorage.../tables/<uuid>`), not under
  the `--warehouse` path at all. `--warehouse` and `--metastore` are
  effectively dead parameters there.

Confirmed with `databricks tables get almanac_dbx.gold.fact_pull_request`:
it exists, is `MANAGED`, last updated by the one successful Gold run
(`655249155948620`), and its `storage_location` is the UC managed path.

This is the same class as the backfill's `/local_disk0` and `file:` URI
divergences (`2026-09-02-burn-deploy-and-first-run-defects.md`): code that
is correct against a local SparkSession behaves differently against a
Unity Catalog cluster, and only a real run on the target surfaces it.

## The fix

Gold is a dbt model. dbt's entire model of the world is named tables in a
catalog/schema, so reading Gold's fact **by name** is the more correct
design, not a workaround: `spark.read.table(gold_table)`, version-pinnable
with `.option("versionAsOf", v)` exactly as the path read was.

- `build_training_frame`'s `gold_warehouse: str` becomes `gold_table: str`
  (a fully-qualified name).
- The training job passes `--gold-table almanac_dbx.gold.fact_pull_request`
  (`var.model_gold_table`).
- Silver `/clean` and the three feature tables are unchanged -- they are
  hand-written path Delta (the backfill and `run_features` both
  `df.write...save(path)`), never registered, so a path read is right for
  them. The asymmetry in `build_training_frame` is real: one dbt-managed
  table, three path tables, one path stream.

The model integration tests register their tmp Delta dir as an external
table (`CREATE TABLE ... USING DELTA LOCATION`) so the by-name read is
exercised for real, uniquely named per test since the `spark` fixture is
session-scoped.
