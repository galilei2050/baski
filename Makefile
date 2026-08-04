SHELL := /bin/bash

.PHONY: setup
setup:
	uv sync --group dev
	uv run pre-commit install --hook-type pre-commit --hook-type pre-push

.PHONY: lint
lint:
	uv run ruff format --check baski/ baski_lint/
	uv run ruff check baski/ baski_lint/
	uv run python -m baski_lint --recursive baski/ baski_lint/

.PHONY: lint-fix
lint-fix:
	uv run ruff format baski/ baski_lint/
	uv run ruff check baski/ baski_lint/ --fix
	uv run python -m baski_lint --recursive baski/ baski_lint/

.PHONY: typecheck
typecheck:
	uv run mypy baski/ baski_lint/

.PHONY: test
test:
	uv run pytest tests/

.PHONY: pre-push
pre-push: typecheck test

.PHONY: ci
ci: lint typecheck test
