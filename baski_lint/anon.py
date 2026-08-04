"""anon-lint: AST rules for smells ruff has no check for.

- ANON001/ANON002 — ban anonymous tuple/dict in function/class type annotations.
- ANON003 — ban a long-lived dependency (client, database, bot, store) in a free function's
  parameters: it is a method missing its class, and behaviour with no home object is behaviour the
  next reader writes a second copy of. Which type names count is per-project, so the list is read
  from `[tool.anon_lint]` in the nearest `pyproject.toml`; the built-in defaults below cover the
  third-party clients every consumer of this library already holds.

Run as:
    python -m baski_lint <files_or_dirs...> [--recursive]

Tests live in tests/test_anon_lint.py and run under `uv run pytest`.
"""

from __future__ import annotations

import argparse
import ast
import re
import sys
import tomllib
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

DICT_LIKE = {"dict", "Dict", "Mapping", "MutableMapping"}
TUPLE_LIKE = {"tuple", "Tuple"}
ANY_NAMES = {"Any"}

# ANON003 defaults: third-party clients any consumer of this library may hold. A project's OWN
# collaborators are not guessable from here — it adds them under `[tool.anon_lint]` (see `_config`).
# Deliberately absent: a deps/container object. Passing one is the whole point of having it.
DEPENDENCY_TYPES = {
    "AsyncAnthropic",
    "AsyncClient",  # httpx
    "AsyncCollection",
    "AsyncDatabase",
    "AsyncElevenLabs",
    "Bot",
    "PlaywrightClient",
    "Scheduler",
    "SerpApiClient",
}
# Naming conventions that mark a collaborator without having to enumerate every one.
DEPENDENCY_SUFFIXES = ("Store", "Log", "Registry")
# Subscripts that wrap a type without changing what the parameter holds — a dependency inside one is
# still a dependency, so ANON003 looks through them rather than reading the wrapper's own name.
_TRANSPARENT_WRAPPERS = {"Optional", "Union", "Annotated"}

# A tuple annotation needs at least this many element types before we flag it
# as anonymous (single-element tuples are usually `tuple[X]` containers).
_TUPLE_MIN_FLAG_ELEMENTS = 2
# A dict annotation must have exactly key + value to be a candidate.
_DICT_SLICE_ELEMENTS = 2

# Codes only — the reason text that follows must NOT be swallowed into the last code, or
# `# noqa: ANON003 wraps one library call` suppresses nothing while looking like it does.
NOQA_RE = re.compile(r"#\s*noqa\s*:\s*([A-Za-z]+[0-9]+(?:\s*,\s*[A-Za-z]+[0-9]+)*)")


@dataclass(frozen=True)
class Config:
    """Which type names ANON003 treats as a long-lived dependency, for one project."""

    dependency_types: frozenset[str]
    dependency_suffixes: tuple[str, ...]


DEFAULT_CONFIG = Config(frozenset(DEPENDENCY_TYPES), DEPENDENCY_SUFFIXES)


@cache
def _config(start: Path) -> Config:
    """Read `[tool.anon_lint]` from the nearest `pyproject.toml` at or above `start`.

    Walking up rather than taking a flag: the linter is invoked from a project's own Makefile, so
    the project it is linting is the one it sits inside. `extend_dependency_types` adds to the
    built-in defaults; `dependency_types` replaces them outright.
    """
    for directory in (start, *start.parents):
        manifest = directory / "pyproject.toml"
        if not manifest.is_file():
            continue
        table = tomllib.loads(manifest.read_text(encoding="utf-8")).get("tool", {}).get("anon_lint", {})
        types = set(table.get("dependency_types", DEPENDENCY_TYPES)) | set(table.get("extend_dependency_types", []))
        suffixes = tuple(table.get("dependency_suffixes", DEPENDENCY_SUFFIXES))
        return Config(frozenset(types), suffixes)
    return DEFAULT_CONFIG


@dataclass(frozen=True)
class Finding:
    """One anonymous-annotation violation: file/line plus code and message."""

    path: Path
    line: int
    col: int
    code: str
    message: str


def _name_of(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _is_ellipsis(node: ast.expr) -> bool:
    return isinstance(node, ast.Constant) and node.value is Ellipsis


def _is_any(node: ast.expr) -> bool:
    return _name_of(node) in ANY_NAMES


def _is_bare_dict_like(node: ast.expr) -> bool:
    return _name_of(node) in DICT_LIKE


def _is_dict_like_anywhere(node: ast.expr) -> bool:
    if _is_bare_dict_like(node):
        return True
    return isinstance(node, ast.Subscript) and _is_bare_dict_like(node.value)


def _slice_elts(slice_node: ast.expr) -> list[ast.expr]:
    if isinstance(slice_node, ast.Tuple):
        return list(slice_node.elts)
    return [slice_node]


def _src(node: ast.expr) -> str:
    try:
        return ast.unparse(node)
    except Exception:  # noqa: BLE001 — ast.unparse can fail on edge cases; fall back to "?"
        return "?"


def _is_typealias(annotation: ast.expr | None) -> bool:
    return annotation is not None and _name_of(annotation) == "TypeAlias"


def _annotation_names(annotation: ast.expr | None) -> Iterator[str]:
    """Every type name an annotation could resolve to.

    Unwraps each spelling the same parameter takes: `X`, `X[...]`, `pkg.X`, a quoted `"X"` under
    TYPE_CHECKING, and every wrapper that leaves the dependency in place — `X | None`, `Optional[X]`,
    `Union[X, None]`, `Annotated[X, ...]`. Unwrapping only some of them would leave the others as a
    one-token way to silence ANON003 without changing anything about the design it catches.
    """
    if annotation is None:
        return
    if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
        try:
            annotation = ast.parse(annotation.value, mode="eval").body
        except SyntaxError:
            return
    if isinstance(annotation, ast.BinOp) and isinstance(annotation.op, ast.BitOr):
        yield from _annotation_names(annotation.left)
        yield from _annotation_names(annotation.right)
        return
    if isinstance(annotation, ast.Subscript) and _subscript_base(annotation) in _TRANSPARENT_WRAPPERS:
        for element in _slice_elts(annotation.slice):
            yield from _annotation_names(element)
        return
    name = _subscript_base(annotation) or _name_of(annotation)
    if name is not None:
        yield name


def _dependency_name(annotation: ast.expr | None, config: Config) -> str | None:
    """The dependency type this annotation names, or None if it names none."""
    return next(
        (
            n
            for n in _annotation_names(annotation)
            if n in config.dependency_types or n.endswith(config.dependency_suffixes)
        ),
        None,
    )


def _subscript_base(node: ast.expr) -> str | None:
    return _name_of(node.value) if isinstance(node, ast.Subscript) else None


class _Checker:
    def __init__(self, source_lines: list[str], path: Path, config: Config = DEFAULT_CONFIG) -> None:
        self.source_lines = source_lines
        self.path = path
        self.config = config
        self.findings: list[Finding] = []

    def check_free_function_deps(self, fn: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        """Flag a module-level function that takes a long-lived collaborator as a parameter."""
        args = fn.args
        # `*args` / `**kwargs` included: a collaborator arrives the same way through either, and
        # ANON001/ANON002 already check both, so skipping them here would be an inconsistency.
        for arg in (*args.posonlyargs, *args.args, *args.kwonlyargs, args.vararg, args.kwarg):
            if arg is None:
                continue
            dependency = _dependency_name(arg.annotation, self.config)
            if dependency is None:
                continue
            if self._suppressed(fn.lineno, "ANON003"):
                return
            self.findings.append(
                Finding(
                    self.path,
                    fn.lineno,
                    fn.col_offset,
                    "ANON003",
                    f"{fn.name}({arg.arg}: {dependency}) — bind it in a class, don't pass it per call",
                )
            )
            return  # one finding per function; the fix is the same whichever parameter tripped it

    def _suppressed(self, line: int, code: str) -> bool:
        if line < 1 or line > len(self.source_lines):
            return False
        match = NOQA_RE.search(self.source_lines[line - 1])
        if not match:
            return False
        codes = {c.strip().upper() for c in match.group(1).split(",")}
        return code in codes

    def _report(self, anchor: ast.expr, offender: ast.expr, code: str, message: str) -> None:
        line = getattr(anchor, "lineno", 1)
        col = getattr(anchor, "col_offset", 0)
        if self._suppressed(line, code):
            return
        self.findings.append(Finding(self.path, line, col, code, f"{_src(offender)} — {message}"))

    def check_annotation(self, node: ast.expr | None) -> None:
        """Walk a single function/class annotation looking for anonymous tuple/dict."""
        if node is None:
            return
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            try:
                inner = ast.parse(node.value, mode="eval").body
            except SyntaxError:
                return
            self._walk(inner, report_node=node)
            return
        self._walk(node, report_node=None)

    def _walk(self, node: ast.expr, *, report_node: ast.expr | None) -> None:
        if _is_bare_dict_like(node):
            anchor = report_node if report_node is not None else node
            self._report(anchor, node, "ANON002", "use @dataclass/TypedDict/pydantic")
            return
        if isinstance(node, ast.Subscript):
            self._walk_subscript(node, report_node=report_node)
            return
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.expr):
                self._walk(child, report_node=report_node)

    def _walk_subscript(self, node: ast.Subscript, *, report_node: ast.expr | None) -> None:
        base = _subscript_base(node)
        if base in TUPLE_LIKE:
            self._check_tuple(node, report_node=report_node)
        elif base in DICT_LIKE:
            self._check_dict(node, report_node=report_node)
        for elt in _slice_elts(node.slice):
            self._walk(elt, report_node=report_node)

    def _check_tuple(self, sub: ast.Subscript, *, report_node: ast.expr | None) -> None:
        anchor = report_node if report_node is not None else sub
        slice_node = sub.slice
        if not isinstance(slice_node, ast.Tuple):
            return
        elts = slice_node.elts
        if len(elts) < _TUPLE_MIN_FLAG_ELEMENTS:
            return
        if _is_ellipsis(elts[-1]):
            return
        self._report(anchor, sub, "ANON001", "use @dataclass/NamedTuple")

    def _check_dict(self, sub: ast.Subscript, *, report_node: ast.expr | None) -> None:
        anchor = report_node if report_node is not None else sub
        elts = _slice_elts(sub.slice)
        if len(elts) != _DICT_SLICE_ELEMENTS:
            return
        value = elts[1]
        if _is_any(value) or _is_dict_like_anywhere(value):
            self._report(anchor, sub, "ANON002", "use @dataclass/TypedDict/pydantic")


def _check_function(fn: ast.FunctionDef | ast.AsyncFunctionDef, checker: _Checker) -> None:
    a = fn.args
    for arg in (*a.posonlyargs, *a.args, *a.kwonlyargs):
        checker.check_annotation(arg.annotation)
    if a.vararg is not None:
        checker.check_annotation(a.vararg.annotation)
    if a.kwarg is not None:
        checker.check_annotation(a.kwarg.annotation)
    checker.check_annotation(fn.returns)


def _check_module_level(tree: ast.Module, checker: _Checker) -> None:
    """ANON003 pass. Module-level functions only — a method is already bound to its collaborators."""
    for node in tree.body:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            checker.check_free_function_deps(node)


def lint_source(source: str, path: Path, config: Config = DEFAULT_CONFIG) -> list[Finding]:
    """Run the checker on a single source string; return all findings (may be empty)."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    checker = _Checker(source.splitlines(), path, config)
    _check_module_level(tree, checker)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
            _check_function(node, checker)
        elif isinstance(node, ast.ClassDef):
            for stmt in node.body:
                if isinstance(stmt, ast.AnnAssign) and not _is_typealias(stmt.annotation):
                    checker.check_annotation(stmt.annotation)
    checker.findings.sort(key=lambda f: (str(f.path), f.line, f.col, f.code))
    return checker.findings


def lint_file(path: Path) -> list[Finding]:
    """Read a file from disk and lint it; silently skips unreadable paths.

    The ANON003 config comes from the file's own project, so linting a path outside this repo still
    uses that project's dependency list rather than this one's.
    """
    try:
        source = path.read_text(encoding="utf-8")
    except OSError:
        return []
    return lint_source(source, path, _config(path.resolve().parent))


def iter_files(targets: Iterable[Path], *, recursive: bool) -> Iterator[Path]:
    """Expand a list of files/dirs into the actual `.py` files to lint."""
    for target in targets:
        if target.is_file():
            yield target
        elif target.is_dir():
            pattern = "**/*.py" if recursive else "*.py"
            yield from sorted(target.glob(pattern))


def format_finding(f: Finding) -> str:
    """Render a finding as the canonical `path:line:col: CODE message` form."""
    return f"{f.path}:{f.line}:{f.col}: {f.code} {f.message}"


def main(argv: list[str] | None = None) -> int:
    """CLI entry point: 0 clean, 1 findings, 2 the linter could not run.

    The three codes are distinct because a build reads nothing else. Collapsing "your config is
    malformed" into the same 1 as "your code has a violation" sends someone hunting the wrong file.
    """
    parser = argparse.ArgumentParser(prog="anon-lint")
    parser.add_argument("paths", nargs="+", type=Path)
    parser.add_argument("--recursive", "-r", action="store_true")
    args = parser.parse_args(argv)

    findings: list[Finding] = []
    try:
        for f in iter_files(args.paths, recursive=args.recursive):
            findings.extend(lint_file(f))
    except tomllib.TOMLDecodeError as exc:
        sys.stderr.write(f"anon-lint: unreadable [tool.anon_lint] config — {exc}\n")
        return 2
    for fnd in findings:
        sys.stdout.write(format_finding(fnd) + "\n")
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
