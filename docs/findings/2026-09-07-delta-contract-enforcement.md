# What Delta enforces for you, and the one thing it does not

**Date:** 2026-09-07
**Phase:** 7, Task 4
**Status:** settled; three mechanism choices and one real defect, all measured

Task 4 puts schema and semantic contracts on the feature tier and the
streaming Silver table. Gold already had contracts (dbt `contract: enforced`),
but Gold is dbt and these surfaces are PySpark writing Delta by path, so the
mechanism had to be chosen rather than copied. Everything below was measured
against `delta-spark 4.4.0` on the local session this repo already uses, not
read off a docs page — each probe is one file write and one append.

## Finding 1 — an overwrite that drops a column is accepted, and silently nulls it

This is the load-bearing one, and it is the reason a contract check exists at
all rather than leaning on Delta's own schema enforcement.

| Write | Result |
|---|---|
| `mode("overwrite")` **widening** a type (`bigint` → `string`) | **Rejected** (`AnalysisException`) |
| `mode("overwrite")` **dropping** a column | **Accepted** |

Accepted is the surprising half, and what "accepted" means is worse than it
sounds. The table keeps the column in its schema; every row's value becomes
null:

```
BEFORE: struct<k:bigint,v:double>
AFTER : struct<k:bigint,v:double>      <- schema unchanged
ROWS  : [Row(k=2, v=None)]             <- data gone
```

Nothing raises. A downstream reader sees the column it expected, all nulls,
and has no way to tell that from a genuine absence of data. `features/runner.py`
overwrites every feature table on every run, so this is exactly the shape a
regression here would take.

`contracts.enforce` therefore checks the frame **before** the write, and
`tests/integration/test_contracts_enforced.py::test_delta_alone_would_have_accepted_that_drop`
holds the finding in place so it cannot rot into a comment nobody rechecks.

## Finding 2 — a NULL fails a Delta CHECK, unlike a SQL-standard CHECK

In standard SQL a CHECK constraint passes on NULL (the predicate is unknown,
not false). Delta requires it to evaluate to **true**:

| Constraint | Row | Result |
|---|---|---|
| `CHECK (v >= 0)` | `v = NULL` | **Rejected** |
| `CHECK (v IS NULL OR v >= 0)` | `v = NULL` | Accepted |

Every check on a legitimately-nullable column in this repo is therefore written
`col IS NULL OR <predicate>`. Getting this backwards would not fail loudly — it
would reject valid rows on a column the design *intends* to be null, such as
`secs_since_last_event` for an entity with no history.

The same asymmetry is useful in the other direction: **key nullability, which
open-source Delta refuses to express as `ALTER COLUMN ... SET NOT NULL` on a
populated table, is expressible as `CHECK (key IS NOT NULL)`, which it accepts.**
That closes a real gap — `features/registration.not_null_key_sql` has never run
outside Databricks for exactly that reason (design §4.6), so until now nothing
enforced non-null keys anywhere the tests could see.

## Finding 3 — CHECK constraints survive `mode("overwrite")`, so they keep binding

The feature runner fully overwrites each table on every run. A constraint that
did not survive that would be first-run decoration.

| Step | Result |
|---|---|
| `ADD CONSTRAINT` by path (`delta.\`/path\``) | works, no metastore needed |
| `mode("overwrite")` after | `delta.constraints.v_nonneg` still in `SHOW TBLPROPERTIES` |
| violating append after the overwrite | still rejected |
| `ADD CONSTRAINT` violated by **existing** rows | rejected (`AnalysisException`) |
| `DROP CONSTRAINT IF EXISTS` by path | works |

Three consequences, all of which the implementation depends on:

- Constraints are applied **by path**, so they run identically on the local
  session and on Databricks — unlike the UC primary key and CDF statements,
  which are gated behind `--register`. Every local `make check` therefore
  exercises the real enforcement mechanism rather than a stand-in.
- Because `ADD CONSTRAINT` validates every existing row, `apply_constraints`
  compares against the stored expression and leaves an unchanged constraint
  alone rather than re-adding it — otherwise each run would rescan every table
  once per check.
- A constraint present on the table but absent from the contract is dropped, so
  a retired rule stops being enforced instead of lingering invisibly.

## Finding 4 — the contract found a real defect: `repo_activity` had a non-unique primary key

Measured on the committed 3,997-row Silver fixture, before any change:

| Feature table | Rows | Distinct keys | Duplicate keys |
|---|---|---|---|
| `author_activity` | 137 | 137 | 0 |
| `repo_activity` | 3,997 | 3,995 | **2** |
| `pr_static` | 137 | 137 | 0 |
| `repo_stream_activity` | 3,995 | 3,995 | 0 |
| `actor_stream_activity` | 3,993 | 3,993 | 0 |

`compute_repo_activity` was the one builder with no deduplication, and
`features/registration.py` declares `PRIMARY KEY (repo_id, event_time TIMESERIES)`
on exactly that pair. Two events for one repo at the same instant produce two
rows: a ROWS frame gives each its own position, so their running totals differ,
and both carry the same key.

That is not cosmetic. `as_of_join` looks the table up by that key, and the
online store serves the latest row per key — so a training feature and a served
feature were both picking between two rows arbitrarily. The fix keeps the row
whose cumulative counts include every event at that instant, which is what
"to date" means, and is deterministic where a bare `dropDuplicates` would not
have been.

**Adjacent, deliberately not changed:** `stream/features.py` deduplicates the
same tie with `dropDuplicates([entity, event_time])`, which satisfies the
uniqueness contract but picks its survivor arbitrarily — the two rows differ in
`secs_since_last_event`. That is a pre-existing property of Phase 6 code, not a
contract violation, so it is recorded here rather than folded into this task's
diff.

## How to re-run any of this

The probes are three files, each under 40 lines: write a Delta table, apply a
constraint, attempt a violating write, read `SHOW TBLPROPERTIES`. The
per-surface duplicate-key counts come from `contracts.duplicate_keys` against
`data/gold_fixture/silver/clean` (`make silver-fixture`), and the same
assertion runs on every `make check` through
`tests/integration/test_contracts_enforced.py`.

## Sources checked live, 2026-09-07

- [ADD CONSTRAINT clause — Databricks SQL reference](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-alter-table-add-constraint)
- [Constraints on Databricks](https://docs.databricks.com/aws/en/tables/constraints)
- [Constraints — Delta Lake documentation](https://docs.delta.io/latest/delta-constraints.html)
- [Schema enforcement — Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/tables/schema-enforcement)

The docs state that CHECK "must evaluate to true" and that `ADD CONSTRAINT`
validates existing rows; neither says what an overwrite missing a column does,
which is why Finding 1 came from a probe rather than a page.
