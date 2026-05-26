SHELL := /bin/bash

.PHONY: setup
setup:
	uv sync --group dev
	uv run pre-commit install

.PHONY: test
test:
	uv run pytest tests/

.PHONY: lint
lint:
	uv run ruff format --check baski/
	uv run ruff check baski/

.PHONY: lint-fix
lint-fix:
	uv run ruff format baski/
	uv run ruff check baski/ --fix

.PHONY: typecheck
typecheck:
	uv run mypy baski/

.PHONY: ci
ci: lint typecheck test
