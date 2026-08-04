SHELL := /bin/bash

.PHONY: setup
setup:
	uv sync --group dev
	uv run pre-commit install --hook-type pre-commit --hook-type pre-push

.PHONY: lint
lint:
	uv run ruff format --check baski/
	uv run ruff check baski/
	uv run python -m baski.lint --recursive baski/

.PHONY: lint-fix
lint-fix:
	uv run ruff format baski/
	uv run ruff check baski/ --fix
	uv run python -m baski.lint --recursive baski/

.PHONY: typecheck
typecheck:
	uv run mypy baski/

.PHONY: test
test:
	uv run pytest tests/

.PHONY: pre-push
pre-push: typecheck test

.PHONY: ci
ci: lint typecheck test
