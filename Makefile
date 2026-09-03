.PHONY: test test-all lint fmt typecheck check fixtures dbt

test:
	uv run pytest -m "not network" -v

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

check:
	@echo "[1/3] lint"
	@$(MAKE) lint
	@echo "[2/3] typecheck"
	@$(MAKE) typecheck
	@echo "[3/3] test"
	@$(MAKE) test

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
