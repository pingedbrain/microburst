"""SQL operation detection — the pg analog of detection/operation.py.

``operation`` is the lowercased SQL verb (``select``, ``insert``,
``begin``…); ``resource`` is a best-effort table-ish token — the first
identifier after a ``FROM``/``INTO``/``UPDATE``/``JOIN``/``TABLE``
keyword. Both feed the shared rule matchers (``operation:``,
``resource:``, ``sql:``); the raw text also lands in the fired event's
``path``.

Deliberately shallow: leading comments and parens are skipped, quoting
is not unescaped, and only the first statement of a multi-statement
``Q`` is classified. That covers the verbs rules actually match on —
anything deeper needs a real SQL parser, which is a dependency the MVP
doesn't take.
"""

from __future__ import annotations

import re

_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")

# Keywords whose following identifier is (usually) the target relation.
_TABLE_KEYWORDS = {"from", "into", "update", "join", "table"}

# Modifiers that can sit between the keyword and the relation name.
_TABLE_MODIFIERS = {"if", "not", "exists", "only", "temp", "temporary",
                    "unlogged", "lateral"}


def _strip_leading_noise(sql: str) -> str:
    """Drop leading whitespace, parens, and -- / /* */ comments."""
    text = sql
    while True:
        stripped = text.lstrip(" \t\r\n(")
        if stripped.startswith("--"):
            nl = stripped.find("\n")
            text = stripped[nl + 1:] if nl >= 0 else ""
        elif stripped.startswith("/*"):
            end = stripped.find("*/")
            text = stripped[end + 2:] if end >= 0 else ""
        else:
            return stripped


def sql_facts(sql: str) -> tuple[str | None, str | None]:
    """(verb, table) — both lowercased, either may be None."""
    text = _strip_leading_noise(sql)
    match = _WORD.match(text)
    if match is None:
        return None, None
    verb = match.group(0).lower()
    table = None
    tokens = _WORD.findall(text)
    for i, token in enumerate(tokens[:-1]):
        if token.lower() not in _TABLE_KEYWORDS:
            continue
        for nxt in tokens[i + 1:]:
            if nxt.lower() in _TABLE_MODIFIERS:
                continue
            table = nxt.lower()
            break
        break
    return verb, table
