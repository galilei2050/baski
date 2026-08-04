"""Project-agnostic AST lint rules, shared by every project built on baski.

Lives inside the package rather than at the repo root on purpose: a root-level `anon_lint.py` is
importable as a top-level module by anything that has this repo on `sys.path` — which every editable
consumer does — so a consumer's own copy is silently shadowed depending on how the interpreter was
started. `baski.lint` can only ever resolve to one thing.
"""

from baski.lint.anon import Config, Finding, format_finding, lint_file, lint_source, main

__all__ = ["Config", "Finding", "format_finding", "lint_file", "lint_source", "main"]
