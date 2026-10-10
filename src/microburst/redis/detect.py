"""Command detection — the redis analog of pg/detect.py.

``operation`` is the lowercased command verb (``get``, ``set``,
``cluster``, …); ``resource`` is a best-effort first key — argv[1] for
the vast majority of commands, with small tables for the families where
it isn't (eval scripts, ``XREAD``'s STREAMS token, numkeys-prefixed
verbs, subcommand-style verbs). Both feed the shared rule matchers
(``operation:``, ``resource:``, ``args:``); the decoded command text
also lands in the fired event's ``path``.

Deliberately shallow: RESP carries no schema, so "the first key" is a
heuristic. Wrong-position hits (an XADD field name surfacing as
``resource``) are accepted — rules match tokens, not parsed semantics.
"""

from __future__ import annotations

# Verbs whose argv[1] exists but is provably not a key or channel —
# subcommands, operands, or credentials (AUTH's argv[1] is a username or
# password and must never surface as a `resource` in the fired log).
_NO_KEY = {
    "acl", "asking", "auth", "bgrewriteaof", "bgsave", "client",
    "cluster", "command", "config", "dbsize", "debug", "discard", "echo",
    "exec", "failover", "flushall", "flushdb", "function", "hello",
    "info", "lastsave", "latency", "lolwut", "memory", "module",
    "monitor", "multi", "ping", "psync", "quit", "randomkey", "replconf",
    "replicaof", "reset", "role", "save", "script", "select", "shutdown",
    "slaveof", "slowlog", "swapdb", "sync", "time", "unwatch", "wait",
    "waitaof",
}

# argv[1] is the script/function name, argv[2] is numkeys, keys start at
# argv[3].
_EVAL_LIKE = {"eval", "evalsha", "eval_ro", "evalsha_ro", "fcall", "fcall_ro"}

# Keys follow a literal STREAMS token: XREAD [GROUP ..] STREAMS k1 k2 …
_STREAMS_KEY = {"xread", "xreadgroup"}

# argv[1] is numkeys, first key is argv[2].
_NUMKEYS = {"lmpop", "zmpop", "sintercard"}

# First key sits at a fixed deeper index: BITOP and|or|xor|not dest k1…
# (argv[2] is the destination — a key token), XINFO <sub> key,
# OBJECT <sub> key, MIGRATE host port key …
_KEY_INDEX = {"xinfo": 2, "object": 2, "migrate": 3, "bitop": 2}


def _decode(arg: bytes) -> str:
    return arg.decode("utf-8", "replace")


def command_facts(args: list[bytes]) -> tuple[str | None, str | None]:
    """(verb, first-key) — both decoded and the verb lowercased."""
    if not args:
        return None, None
    verb = _decode(args[0]).lower()
    key = _first_key(verb, args)
    return verb, key


def _first_key(verb: str | None, args: list[bytes]) -> str | None:
    if verb is None or verb in _NO_KEY:
        return None
    if verb in _EVAL_LIKE:
        if len(args) >= 4 and args[2].isdigit() and int(args[2]) >= 1:
            return _decode(args[3])
        return None
    if verb in _STREAMS_KEY:
        for i, arg in enumerate(args):
            if arg.upper() == b"STREAMS" and i + 1 < len(args):
                return _decode(args[i + 1])
        return None
    if verb in _NUMKEYS:
        if len(args) >= 3 and args[1].isdigit() and int(args[1]) >= 1:
            return _decode(args[2])
        return None
    if verb in _KEY_INDEX:
        idx = _KEY_INDEX[verb]
        return _decode(args[idx]) if len(args) > idx else None
    return _decode(args[1]) if len(args) > 1 else None


def command_text(args: list[bytes]) -> str:
    """Space-joined decoded args — the ``args:`` matcher surface and the
    fired event's ``path``. Pipelines and inline commands render the
    same so a rule matches either spelling."""
    return " ".join(_decode(a) for a in args)
