.PHONY: test test-fast test-all lint fmt typecheck check check-fast fixtures dbt \
	silver-fixture lineage window-up window-down lakebase-up lakebase-down coverage \
	demo-build demo

# -n 4: four xdist workers, each with its own SparkSession. Tuned for a
# local 8-core / 16 GB machine -- four Spark JVMs fit, eight would thrash.
# "Fit" means on an otherwise idle machine: on 2026-09-08 a run with a
# 15-minute load average of 35 gave 2 failures and 11 errors across
# test_pipeline, test_silver_partitions and test_model_similarity_comparison,
# every one of which passed serially, and the whole suite passed on a
# re-run once load dropped. Treat a failure in those files as "check the
# load" before "check the code".
# CI keeps the plain serial `pytest` (2-core runner) in .github/workflows.
# --durations=25: the suite is heavily back-loaded (the dbt/Gold Spark tests
# all land last), so percent-complete predicts nothing and "it feels slow" was
# never checkable. Reported on every run so a real slowdown is visible for
# free, rather than needing a dedicated instrumented run to find.
test:
	uv run pytest -m "not network" -n 4 --durations=25

# The 467 tests that need no SparkSession -- seconds, not half an hour.
# Measured 2026-09-11: 467 passed, 253 deselected, 36.59s.
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

# Fixtures -> Silver -> the committed artifacts the demo reads. Spark runs
# HERE and nowhere else: `make demo` never starts a JVM.
demo-build:
	uv run python -m almanac.demo.build

# The app. Reads demo/data/ and demo/model/ -- no SparkSession, no JVM, no
# network. Run `make demo-build` first if the artifacts are missing.
demo:
	uv run --extra demo streamlit run demo/app.py

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

# The Lakebase stack, which is a *second* root module in centralus because
# Lakebase is not offered in westus3 and a project inherits its workspace's
# region. `lakebase-up` refuses an unsupported region before terraform is
# invoked at all -- targeting the westus3 instance instead cost 44 minutes on
# 2026-09-08, because an unsupported region does not fail, it hangs.
#
# Between the two stages `lakebase-up` runs, the wheel, scripts and config
# must be deployed into the NEW workspace and the token put in its secret
# scope: secret scopes are per-workspace and nothing carries over.
# See infra/terraform-lakebase/README.md.
lakebase-up:
	uv run python -m almanac.infra.lakebase_window up --apply

lakebase-down:
	uv run python -m almanac.infra.lakebase_window down --apply

# §10's coverage figure. The scope lives in pyproject.toml's
# [tool.coverage.run], so this and CI cannot disagree about what is measured.
# The Spark tests are not optional here: the same scope measures 60% on the
# non-Spark subset alone (2026-09-07), because the transform layer is
# exercised almost entirely by them.
coverage:
	uv run pytest -m "not network" -n 4 --cov --cov-report=term
