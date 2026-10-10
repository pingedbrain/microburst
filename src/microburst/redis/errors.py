"""Redis ``-LINE`` error rendering.

A Redis error reply is one line: ``-`` then the code token clients
branch on, then free text. Fidelity is that first token — cluster
clients regex ``MOVED``/``ASK`` into ``(slot, host:port)`` redirects,
``CLUSTERDOWN``/``LOADING``/``READONLY``/``BUSY``/``NOSCRIPT``/``OOM``
each drive distinct client behavior, and ``ERR`` is the generic bucket.

Rule spelling (rules.py ``FaultError``):

* ``error: {code: "MOVED 3999 127.0.0.1:7001"}`` — ``code`` carries the
  whole post-``-`` line, redirect tail included.
* ``error: {code: MOVED, fields: {slot: 3999, target: "h:p"}}`` — the
  same line composed from structured fields (MOVED/ASK only).
* ``error.message`` appends after the code — ``{code: OOM, message:
  "command not allowed ..."}`` → ``-OOM command not allowed ...``.
"""

from __future__ import annotations

# Codes that carry a machine-parsed "slot host:port" tail — cluster
# clients regex it, so it must be whole and first.
_REDIRECT = {"MOVED", "ASK"}


def error_reply(
    *,
    code: str | None = None,
    message: str | None = None,
    fields: dict | None = None,
) -> bytes:
    """``-CODE[ tail][ message]\\r\\n``.

    ``code`` may carry the whole line already (``"MOVED 3999 h:p"``);
    a bare ``MOVED``/``ASK`` composes its tail from ``fields.slot`` /
    ``fields.target`` when both are present. Other ``fields`` keys are
    ignored — real Redis error lines carry no structured members.
    """
    line = str(code or "ERR")
    f = fields or {}
    head = line.split(" ", 1)[0].upper()
    if (
        head in _REDIRECT
        and len(line.split()) == 1
        and "slot" in f
        and "target" in f
    ):
        line = f"{head} {f['slot']} {f['target']}"
    if message:
        line = f"{line} {message}"
    return b"-" + line.encode("utf-8", "replace") + b"\r\n"


def default_message(code: str | None) -> str | None:
    """Message for a rule that set ``code`` but no ``message``.

    Redirect codes get none — trailing text after the ``slot host:port``
    pair risks tripping strict client parsers. Everything else gets the
    shared injected-fault marker.
    """
    head = (code or "").split(" ", 1)[0].upper()
    if head in _REDIRECT:
        return None
    return "injected fault"


# Well-known lines — also handy for the fake upstream in tests.
def moved(slot: int, target: str) -> str:
    return f"MOVED {slot} {target}"


def ask(slot: int, target: str) -> str:
    return f"ASK {slot} {target}"


def clusterdown(message: str = "The cluster is down") -> str:
    return f"CLUSTERDOWN {message}"


def loading(message: str = "LOADING Redis is loading the dataset in memory") -> str:
    return f"LOADING {message}"


def readonly(message: str = "You can't write against a read only replica.") -> str:
    return f"READONLY {message}"


def busy(message: str = "BUSY Redis is busy running a script") -> str:
    return f"BUSY {message}"


def noscript(message: str = "NOSCRIPT No matching script") -> str:
    return f"NOSCRIPT {message}"


def crossslot(message: str = "Keys in request don't hash to the same slot") -> str:
    return f"CROSSSLOT {message}"


def oom(message: str = "OOM command not allowed when used memory > 'maxmemory'.") -> str:
    return f"OOM {message}"
