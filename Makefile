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

check: lint typecheck test

fixtures:
	uv run python scripts/build_fixtures.py

# Gold. Runs through the runner, never a bare `dbt` command: the SparkSession
# has to exist, with Delta and a persistent metastore, before dbt asks for one.
dbt:
	uv run python -m almanac.gold.runner build
