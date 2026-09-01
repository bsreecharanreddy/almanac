.PHONY: test test-all lint fmt typecheck check fixtures

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
