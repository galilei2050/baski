"""Project-agnostic AST lint rules, shared by every project built on baski.

A top-level package rather than `baski.lint`, for two reasons.

It must not live at a repo root as a bare `anon_lint.py`: the root of an editable dependency is on
`sys.path`, so a top-level module there is importable by every consumer and silently shadows the
consumer's own copy depending on how the interpreter was started. Two copies had already drifted
apart under that cover.

And it must not sit under `baski/`: importing `baski.lint` runs `baski/__init__.py`, which eagerly
pulls fastapi, pymongo, and google-cloud — 1702 modules and ~1s, against 372us for the checker
itself, paid on every `make lint` in every consumer. In an environment without those runtime
dependencies it fails outright with an exit code the build cannot tell apart from a real finding.
A build-time tool has no business importing a runtime library; shipping it in the same distribution
under its own name keeps the single-copy guarantee without the coupling.
"""

from baski_lint.anon import Config, Finding, format_finding, lint_file, lint_source, main

__all__ = ["Config", "Finding", "format_finding", "lint_file", "lint_source", "main"]
