"""Tests for `baski.lint` — the anonymous-tuple/dict checker (ANON001/002) and the free-function
dependency checker (ANON003).

A lint rule that silently stops matching is worse than no rule: `make lint` is green either way. So
each test below is written to die on a specific way the checker could break, and the CLI test covers
the exit code, which is the only thing the build actually reads.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from baski_lint import Config, lint_source, main

_FIRES = [
    ("def f() -> tuple[str, int]: ...", {"ANON001"}),
    ("def f(x: tuple[str, int, bool]) -> None: ...", {"ANON001"}),
    ("def f() -> dict[str, Any]: ...", {"ANON002"}),
    ("def f() -> dict: ...", {"ANON002"}),
    ("def f() -> list[tuple[str, int]]: ...", {"ANON001"}),
    ("class C:\n    data: dict[str, dict[str, int]]\n", {"ANON002"}),
    ('def f(x: "tuple[str, int]") -> None: ...', {"ANON001"}),
    ("def f(*args: tuple[str, int]) -> None: ...", {"ANON001"}),
    ("def f(**kw: dict[str, Any]) -> None: ...", {"ANON002"}),
    ("from typing import Tuple\ndef f() -> Tuple[str, int]: ...", {"ANON001"}),
    ("from typing import Dict, Any\ndef f() -> Dict[str, Any]: ...", {"ANON002"}),
    ("def f() -> Callable[[int], dict[str, Any]]: ...", {"ANON002"}),
]

_CLEAN = [
    "def f() -> tuple[int, ...]: ...",
    "def f() -> tuple[int]: ...",
    "def f() -> dict[str, int]: ...",
    "def f() -> dict[str, MyClass]: ...",
    "def f() -> tuple[str, int]: ...  # noqa: ANON001",
    "def f() -> dict[str, Any]: ...  # noqa: ANON002",
    "from typing import TypeAlias\nResult: TypeAlias = tuple[str, int]\n",
    "def f() -> None:\n    x: tuple[str, int] = ('a', 1)\n",
    "x: tuple[str, int] = ('a', 1)\n",
    "def f() -> None: ...",
    "class C:\n    x: int\n    y: str\n",
]


@pytest.mark.parametrize(("src", "expected"), _FIRES)
def test_fires(src: str, expected: set[str]) -> None:
    findings = lint_source(textwrap.dedent(src), Path("x.py"))
    assert findings, f"expected {expected}, got nothing"
    codes = {f.code for f in findings}
    assert expected.issubset(codes), f"expected {expected}, got {codes}"


@pytest.mark.parametrize("src", _CLEAN)
def test_clean(src: str) -> None:
    findings = lint_source(textwrap.dedent(src), Path("x.py"))
    assert findings == [], [f"{f.code}: {f.message}" for f in findings]


def test_noqa_multiple_codes() -> None:
    src = "def f() -> tuple[str, int]: ...  # noqa: ANON001, ANON002"
    assert lint_source(src, Path("x.py")) == []


def test_class_attr_fires() -> None:
    src = "class C:\n    data: tuple[str, int]\n"
    findings = lint_source(src, Path("x.py"))
    assert [f.code for f in findings] == ["ANON001"]


def test_nested_tuple_in_dict_value() -> None:
    src = "def f() -> dict[str, tuple[int, int]]: ...\n"
    findings = lint_source(src, Path("x.py"))
    assert "ANON001" in {f.code for f in findings}


# ── ANON003: a dependency in a free function's parameters ──

_CONFIG = Config(frozenset({"AsyncAnthropic", "AsyncDatabase", "Bot"}), ("Store", "Log"))


def _anon003(src: str) -> list[str]:
    return [f.code for f in lint_source(textwrap.dedent(src), Path("x.py"), _CONFIG)]


def test_a_free_function_taking_a_client_is_flagged() -> None:
    """The shape the rule exists for: the client is threaded in at every call site.

    Position is asserted, not just the code — a finding anchored anywhere but the `def` line makes
    both the report and the `# noqa` escape hatch point at the wrong place.
    """
    src = "async def classify(anthropic: AsyncAnthropic, evidence: Evidence) -> Classification: ...\n"

    (finding,) = lint_source(src, Path("x.py"), _CONFIG)

    assert (finding.code, finding.line, finding.col) == ("ANON003", 1, 0)
    assert "classify(anthropic: AsyncAnthropic)" in finding.message


def test_every_parameter_kind_counts() -> None:
    """Keyword-only is how most collaborators are actually written (`def f(*, database: ...)`), so a
    rule that only walked positional parameters would miss the shape it was written for."""
    assert _anon003("def a(database: AsyncDatabase) -> None: ...") == ["ANON003"]
    assert _anon003("def b(*, database: AsyncDatabase) -> None: ...") == ["ANON003"]
    assert _anon003("def c(bot: Bot, /) -> None: ...") == ["ANON003"]
    assert _anon003("async def d(*, bot: Bot) -> None: ...") == ["ANON003"]


def test_the_suffix_convention_counts_without_enumerating_every_type() -> None:
    assert _anon003("def a(store: MemoryStore) -> None: ...") == ["ANON003"]
    assert _anon003("def b(log: RevisionLog) -> None: ...") == ["ANON003"]


def test_a_method_is_not_flagged_because_it_is_already_bound() -> None:
    """The fix for ANON003 is "make it a method" — flagging methods would close the only exit."""
    assert _anon003("class C:\n    def take(self, database: AsyncDatabase) -> None: ...\n") == []


def test_making_the_dependency_optional_does_not_silence_it() -> None:
    """Otherwise ` | None` is a one-token way to shut the rule up without changing the design — and
    an optional dependency is a worse smell, not an exemption.

    All four spellings, not just the fashionable one: unwrapping some and not others leaves the
    others as the escape hatch, which is the same hole with a longer name.
    """
    assert _anon003("def a(store: MemoryStore | None) -> None: ...") == ["ANON003"]
    assert _anon003("def b(store: Optional[MemoryStore]) -> None: ...") == ["ANON003"]
    assert _anon003('def c(store: "MemoryStore | None") -> None: ...') == ["ANON003"]
    assert _anon003("def d(store: Union[MemoryStore, None]) -> None: ...") == ["ANON003"]
    assert _anon003("def e(store: Annotated[MemoryStore, 'injected']) -> None: ...") == ["ANON003"]


def test_a_dependency_arriving_as_varargs_counts() -> None:
    """A collaborator reaches a function the same way through `*args` / `**kwargs`, and ANON001/002
    in this same file already check both — skipping them here would be an inconsistency, silently."""
    assert _anon003("def a(*stores: AsyncDatabase) -> None: ...") == ["ANON003"]
    assert _anon003("def b(**stores: MemoryStore) -> None: ...") == ["ANON003"]


def test_generic_and_quoted_spellings_count() -> None:
    """`"X"` under TYPE_CHECKING and `X[Any]` are the other two ways the same parameter is written."""
    assert _anon003('def a(store: "MemoryStore") -> None: ...') == ["ANON003"]
    assert _anon003("def b(db: AsyncDatabase[Any]) -> None: ...") == ["ANON003"]


def test_a_pure_helper_over_primitives_is_left_alone() -> None:
    """The rule must not push stateless helpers into classes — that would trade duplication for
    ceremony, which is a worse deal."""
    assert _anon003("def split_message(text: str, limit: int = 4096) -> list[str]: ...") == []


def test_one_finding_per_function_even_with_several_dependencies() -> None:
    """The fix is the same whichever parameter tripped it; three lines of noise per function would
    train the eye to skip the rule."""
    assert _anon003("def fire(database: AsyncDatabase, bot: Bot, store: MemoryStore) -> None: ...") == ["ANON003"]


def test_a_type_outside_this_projects_config_is_not_flagged() -> None:
    """The type list is per-project — a name this project never configured is just a name."""
    assert _anon003("def a(questions: PendingQuestions) -> None: ...") == []


def test_noqa_suppresses_it_on_the_def_line_of_a_wrapped_signature() -> None:
    """A documented exception has to be expressible, or the rule is deleted the first time it is
    inconvenient — and signatures wrap, so the `def` line and the offending parameter differ."""
    assert _anon003("def fire(  # noqa: ANON003 — wraps one library call\n    db: AsyncDatabase,\n) -> None: ...\n") == []


def test_the_noqa_reason_may_be_written_any_way_and_still_suppresses() -> None:
    """A noqa is expected to carry a reason, so the reason text must not be parsed as part of the
    code — otherwise the documented form is the one that silently fails to suppress."""
    for comment in ("# noqa: ANON003 — wraps one call", "# noqa: ANON003 wraps one call", "# noqa: ANON003"):
        assert _anon003(f"def f(db: AsyncDatabase) -> None:  {comment}\n    ...\n") == [], comment


# ── the CLI, which is the only thing `make lint` reads ──


def test_the_cli_exits_nonzero_and_names_the_spot(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Every other test calls `lint_source` directly and would stay green with `main` returning 0
    unconditionally — the "green forever, catches nothing" failure this file exists to stop.

    Uses ANON003 so the built-in dependency defaults are exercised too: `tmp_path` has no
    `pyproject.toml`, so this is the path a project that configures nothing takes.
    """
    violator = tmp_path / "violator.py"
    violator.write_text("def f(database: AsyncDatabase) -> None: ...\n", encoding="utf-8")

    exit_code = main([str(violator)])

    assert exit_code == 1
    assert f"{violator}:1:0: ANON003" in capsys.readouterr().out


def test_the_cli_exits_zero_on_clean_input(tmp_path: Path) -> None:
    """The other half: a rule that fails on everything gets switched off within a day."""
    clean = tmp_path / "clean.py"
    clean.write_text("def f(text: str) -> str: ...\n", encoding="utf-8")

    assert main([str(clean)]) == 0


# ── per-project configuration, the reason ANON003 can live in a shared library at all ──


def _project(tmp_path: Path, table: str, source: str) -> Path:
    (tmp_path / "pyproject.toml").write_text(f"[tool.anon_lint]\n{table}\n", encoding="utf-8")
    module = tmp_path / "m.py"
    module.write_text(source, encoding="utf-8")
    return module


def test_a_project_can_name_its_own_collaborators(tmp_path: Path) -> None:
    """A shared library cannot know a consumer's own types, so without this the rule would only ever
    catch third-party clients — which are the minority of any project's dependencies."""
    module = _project(tmp_path, 'extend_dependency_types = ["Widget"]', "def f(w: Widget) -> None: ...\n")

    assert main([str(module)]) == 1


def test_extending_keeps_the_built_in_defaults(tmp_path: Path) -> None:
    """`extend_` must add to the defaults, not silently replace them — otherwise naming one type
    turns the third-party checks off without saying so."""
    module = _project(tmp_path, 'extend_dependency_types = ["Widget"]', "def f(db: AsyncDatabase) -> None: ...\n")

    assert main([str(module)]) == 1


def test_setting_the_list_outright_replaces_the_defaults(tmp_path: Path) -> None:
    """The escape hatch for a project whose stack shares none of the default names."""
    module = _project(tmp_path, 'dependency_types = ["Widget"]', "def f(db: AsyncDatabase) -> None: ...\n")

    assert main([str(module)]) == 0


def test_a_malformed_config_exits_differently_from_a_finding(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A build reads the exit code and nothing else. Reporting "your config is broken" as the same 1
    as "your code has a violation" sends the reader hunting the wrong file."""
    (tmp_path / "pyproject.toml").write_text("this is not = toml [[[\n", encoding="utf-8")
    module = tmp_path / "m.py"
    module.write_text("def f(text: str) -> str: ...\n", encoding="utf-8")

    assert main([str(module)]) == 2
    assert "unreadable" in capsys.readouterr().err


def test_the_linter_does_not_import_the_library_it_ships_with() -> None:
    """It is a build-time tool. While it lived at `baski.lint`, importing it ran `baski/__init__`,
    which pulls fastapi, pymongo and google-cloud: 1702 modules and ~1s on every run, against 372us
    for the checker itself — and a hard failure wherever those runtime deps are absent, with an exit
    code a build cannot tell apart from a real finding. A subprocess is the only honest check: this
    test module has already imported plenty by the time it runs.
    """
    probe = "import baski_lint, sys; print('baski' in sys.modules)"

    result = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, check=True)

    assert result.stdout.strip() == "False", "importing the linter dragged in the runtime library"
