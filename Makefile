.PHONY: test test-fast test-all lint fmt typecheck check check-fast fixtures dbt

# -n 4: four xdist workers, each with its own SparkSession. Tuned for a
# local 8-core / 16 GB machine -- four Spark JVMs fit, eight would thrash.
# CI keeps the plain serial `pytest` (2-core runner) in .github/workflows.
test:
	uv run pytest -m "not network" -n 4

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
