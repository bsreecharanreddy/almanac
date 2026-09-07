.PHONY: test test-fast test-all lint fmt typecheck check check-fast fixtures dbt \
	silver-fixture lineage window-up window-down

# -n 4: four xdist workers, each with its own SparkSession. Tuned for a
# local 8-core / 16 GB machine -- four Spark JVMs fit, eight would thrash.
# CI keeps the plain serial `pytest` (2-core runner) in .github/workflows.
# --durations=25: the suite is heavily back-loaded (the dbt/Gold Spark tests
# all land last), so percent-complete predicts nothing and "it feels slow" was
# never checkable. Reported on every run so a real slowdown is visible for
# free, rather than needing a dedicated instrumented run to find.
test:
	uv run pytest -m "not network" -n 4 --durations=25

# The ~195 tests that need no SparkSession -- seconds, not half an hour.
# The inner-loop counterpart to `test`; `check` still runs everything.
test-fast:
	uv run pytest -m "not network and not spark"

test-all:
	uv run pytest -v

lint:
	uv run ruff check .
	uv run ruff format --check .

fmt:
	uv run ruff format .
	uv run ruff check --fix .

typecheck:
	uv run mypy src tests

# The full gate. Runs before every push -- CI runs the same three steps.
check:
	@echo "[1/3] lint"
	@$(MAKE) lint
	@echo "[2/3] typecheck"
	@$(MAKE) typecheck
	@echo "[3/3] test"
	@$(MAKE) test

# Inner loop: lint + types + only the non-Spark tests. NOT a substitute for
# `check` before a push -- the Spark suite is where the regressions this
# project's multi-phase history keeps producing actually surface.
check-fast:
	@echo "[1/3] lint"
	@$(MAKE) lint
	@echo "[2/3] typecheck"
	@$(MAKE) typecheck
	@echo "[3/3] test-fast"
	@$(MAKE) test-fast

fixtures:
	uv run python scripts/build_fixtures.py

# Bronze -> Silver on the committed fixtures, so Gold has real Delta tables
# to select from. Ephemeral output under data/, gitignored like the
# warehouse and metastore it feeds.
silver-fixture:
	uv run python scripts/build_silver_fixture.py

# Gold. Runs through the runner, never a bare `dbt` command: the SparkSession
# has to exist, with Delta and a persistent metastore, before dbt asks for one.
dbt: silver-fixture
	uv run python -m almanac.gold.runner --silver-path data/gold_fixture/silver build

# Needs a workspace: system.access.column_lineage is a Databricks system table,
# so CI cannot regenerate this. The artifact is committed, and
# tests/unit/test_governance_lineage_artifact.py guards it against silently
# losing a tier -- which is what a broken extraction looks like.
lineage:
	DATABRICKS_HOST=$${DATABRICKS_HOST:?set DATABRICKS_HOST} \
	uv run python -m almanac.governance.lineage_runner \
		--warehouse-id $${DATABRICKS_WAREHOUSE_ID:?set DATABRICKS_WAREHOUSE_ID}

# The one billable thing Phase 7 provisions (design doc §4.7): a serverless SQL
# warehouse for the dashboards, up for an attended window and then gone. Never
# a bare `terraform apply` -- that plans to recreate Phase 5's and Phase 6's
# deliberately destroyed stacks (~$19/day idle, measured 2026-09-07). Both
# targets read the plan and refuse anything reaching past the window; drop
# `--apply` to see that plan without running it.
window-up:
	uv run python -m almanac.infra.window up --apply

window-down:
	uv run python -m almanac.infra.window down --apply
