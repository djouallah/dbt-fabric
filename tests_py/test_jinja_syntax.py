"""Every model and singular test must be syntactically valid Jinja.

dbt only reports this from a `dbt parse`, which needs that engine's dbt installed -- and one
environment cannot hold both dbt majors, so half the repo's templates can only be parsed in
CI. This test closes that gap offline for all five engines at once: it does not need dbt, an
adapter, credentials or a warehouse, only Jinja's own lexer.

WHAT IT ACTUALLY CATCHES, and why it exists: a Jinja comment cannot live inside a `{{ ... }}`
expression. Putting one between two arguments of `{{ config(...) }}` is silently fine to the
eye and fails at parse with "Failed to render SQL syntax error: unexpected character"
(MacroSyntaxInvalid, dbt1502) pointing at a column, not a cause. It cost a CI round trip.
The same lexer pass also catches an unclosed `{% if %}`, a stray `{#` and a `-%}` where the
repo requires `%}`.

This is a SYNTAX check, not a semantic one. It knows nothing about dbt's context, so
`ref()`, `config()`, `is_incremental()` and every project macro are undefined here -- which
is fine, because nothing is rendered.

Run: python -m pytest tests_py/ -q
"""
from __future__ import annotations

import pytest
from jinja2 import Environment
from jinja2.exceptions import TemplateSyntaxError

from _layout import ENGINES, REPO, SHARED_MACROS, models_dir, singular_tests_dir


def _templates():
    for engine in ENGINES:
        for d in (models_dir(engine), singular_tests_dir(engine)):
            for p in sorted(d.rglob("*.sql")):
                yield pytest.param(p, id=f"{engine}/{p.parent.name}/{p.stem}")
    for p in sorted(SHARED_MACROS.rglob("*.sql")):
        yield pytest.param(p, id=f"macros/{p.stem}")
    for engine_macros in sorted((REPO / "dbt1" / "macros").rglob("*.sql")):
        yield pytest.param(engine_macros, id=f"dbt1-macros/{engine_macros.stem}")


@pytest.mark.parametrize("path", list(_templates()))
def test_template_lexes(path):
    try:
        # parse(), not render(): we want the syntax tree, and rendering would need dbt's
        # whole context. Jinja's own delimiters are dbt's, so this is the same lexer dbt uses.
        #
        # The two extensions are the ones dbt turns on, and the macros here rely on both:
        # `{% do parts.append(...) %}` is the do extension, and the loopcontrols extension
        # supplies `{% break %}` / `{% continue %}`. Without them a perfectly good macro
        # fails as "unknown tag 'do'".
        env = Environment(extensions=["jinja2.ext.do", "jinja2.ext.loopcontrols"])
        env.parse(path.read_text(encoding="utf-8"))
    except TemplateSyntaxError as e:
        pytest.fail(
            f"{path.relative_to(REPO)}:{e.lineno} is not valid Jinja: {e.message}\n"
            f"A comment inside a {{{{ ... }}}} expression is the usual cause -- move it above "
            f"the block. dbt reports this as MacroSyntaxInvalid (dbt1502) with a column and no "
            f"explanation."
        )


@pytest.mark.parametrize("path", list(_templates()))
def test_no_jinja_comment_inside_an_expression(path):
    """A `{#` between `{{` and `}}` lexes in isolation but is invalid to dbt.

    Jinja's own parser accepts some of these, so the lexer test above does not always fire.
    The rule is simple and absolute: comments live between statements, never inside one.
    """
    text = path.read_text(encoding="utf-8")
    depth, line = 0, 1
    i = 0
    while i < len(text):
        two = text[i:i + 2]
        if two == "{{":
            depth, i = depth + 1, i + 2
            continue
        if two == "}}" and depth:
            depth, i = depth - 1, i + 2
            continue
        if two == "{#" and depth:
            pytest.fail(
                f"{path.relative_to(REPO)}:{line} has a Jinja comment inside a "
                f"{{{{ ... }}}} expression. dbt fails this at parse with "
                f"'Failed to render SQL syntax error: unexpected character'. Move the comment "
                f"above the block."
            )
        if text[i] == "\n":
            line += 1
        i += 1


@pytest.mark.parametrize("path", list(_templates()))
def test_no_comment_delimiter_inside_a_comment(path):
    """A comment must never QUOTE comment delimiters, because Jinja comments do not nest.

    `{#-- ... {#-- ... --#} ... --#}` does not mean what it looks like: the scanner closes on
    the FIRST `#}` it meets, and everything after it becomes template text that the model
    silently emits. Nothing else catches that -- the file still lexes, `dbt parse` is happy,
    and the damage is prose in the compiled SQL.

    Met writing this suite's own neighbours: a comment explaining the rule above quoted
    `{# #}` to illustrate it and closed itself on the `#}` inside the illustration. AGENTS.md
    lists it under "Jinja traps, all met in this repo"; this is the check for it.
    """
    text = path.read_text(encoding="utf-8")
    i = 0
    while (i := text.find("{#", i)) != -1:
        end = text.find("#}", i + 2)
        line = text[:i].count("\n") + 1
        assert end != -1, (
            f"{path.relative_to(REPO)}:{line} opens a Jinja comment that is never closed"
        )
        assert "{#" not in text[i + 2:end], (
            f"{path.relative_to(REPO)}:{line} has a comment containing another `{{#`. Jinja "
            f"comments do not nest -- this one closed at the first `#}}` and the rest of it "
            f"is being emitted as SQL. Describe the delimiters in words instead of quoting "
            f"them."
        )
        i = end + 2
