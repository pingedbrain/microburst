"""ERR_Packet rendering.

An ERR_Packet is (MYSQL DOCS: ERR_Packet)::

    0xFF | errno int<2,LE> | '#' + sqlstate char<5> [if
    CLIENT_PROTOCOL_41] | human-readable message

The ``errno``/``sqlstate`` pair is the fidelity: drivers surface errno
(``pymysql``'s ``err.args[0]``, JDBC's ``getErrorCode()``) and the
SQLSTATE carries the ANSI class clients branch on — ``40001``
(serialization/deadlock) and ``08*`` (connection errors) drive retry
decisions. Both are emitted from the rule's ``error:`` map:

* ``error: {errno: 1064}`` — sqlstate fills from ``ERRNO_TO_SQLSTATE``
  (pairs from the MySQL server error-reference docs — labeled DOCS —
  for the errnos listed; anything unlisted falls back to ``HY000``).
* ``error: {code: "42000"}`` — mysql mode reads ``code`` as the
  SQLSTATE (same canonical slot pg's ``sqlstate:`` alias feeds).
  ``errno`` fills from the reverse map, defaulting to 1105
  (``ER_UNKNOWN_ERROR``, whose documented pair IS ``HY000`` — the
  defensible "unspecified server error" default).
* ``error.severity`` — MySQL's ERR carries no severity field;
  ``FATAL``/``PANIC`` mean "send the ERR then close", the shape of a
  real server dropping the session (mirrors pg mode).

``error.fields`` has no wire slot in ERR_Packet — accepted and ignored.
"""

from __future__ import annotations

# errno → SQLSTATE for the common fault-injection errnos. Evidence:
# MySQL server error-reference documentation (docs-specified pairs).
ERRNO_TO_SQLSTATE: dict[int, str] = {
    1040: "08004",   # ER_CON_COUNT_ERROR — "Too many connections"
    1042: "08S01",   # ER_BAD_HOST_ERROR
    1043: "08S01",   # ER_HANDSHAKE_ERROR — "Bad handshake"
    1044: "42000",   # ER_DBACCESS_DENIED_ERROR
    1045: "28000",   # ER_ACCESS_DENIED_ERROR
    1046: "3D000",   # ER_NO_DB_ERROR
    1049: "42000",   # ER_BAD_DB_ERROR
    1062: "23000",   # ER_DUP_ENTRY
    1064: "42000",   # ER_PARSE_ERROR — syntax error
    1105: "HY000",   # ER_UNKNOWN_ERROR — the canonical pair
    1146: "42S02",   # ER_NO_SUCH_TABLE
    1153: "08S01",   # ER_NET_PACKET_TOO_LARGE
    1161: "08S01",   # ER_NET_WRITE_INTERRUPTED
    1205: "HY000",   # ER_LOCK_WAIT_TIMEOUT
    1213: "40001",   # ER_LOCK_DEADLOCK
    1290: "HY000",   # ER_OPTION_PREVENTS_STATEMENT (read-only)
    1412: "HY000",   # ER_TABLE_DEF_CHANGED
    2006: "HY000",   # CR_SERVER_GONE_ERROR (client-side; sent anyway)
    2013: "HY000",   # CR_SERVER_LOST (client-side; sent anyway)
}

SQLSTATE_TO_ERRNO: dict[str, int] = {}
for _errno, _sqlstate in ERRNO_TO_SQLSTATE.items():
    # first pairing wins — HY000 keeps 1105, ER_UNKNOWN_ERROR
    SQLSTATE_TO_ERRNO.setdefault(_sqlstate, _errno)
del _errno, _sqlstate

# Defensible unspecified-error pair: ER_UNKNOWN_ERROR is 1105 and its
# documented SQLSTATE is HY000 (docs-specified pairing).
DEFAULT_ERRNO = 1105
DEFAULT_SQLSTATE = "HY000"
DEFAULT_MESSAGE = "injected fault"


def resolve_errno_sqlstate(
    errno: int | None, code: str | None
) -> tuple[int, str]:
    """Rule ``error.errno``/``error.code`` → the (errno, sqlstate) pair
    the ERR_Packet carries, filling the missing half from the curated
    maps or the ER_UNKNOWN_ERROR default."""
    if errno is not None:
        sqlstate = code or ERRNO_TO_SQLSTATE.get(errno, DEFAULT_SQLSTATE)
        return errno, sqlstate
    sqlstate = code or DEFAULT_SQLSTATE
    return SQLSTATE_TO_ERRNO.get(sqlstate, DEFAULT_ERRNO), sqlstate


def err_payload(
    *,
    errno: int,
    sqlstate: str,
    message: str,
    protocol_41: bool = True,
) -> bytes:
    """ERR_Packet payload. ``sqlstate`` is coerced to exactly 5 chars —
    truncated or space-padded, never malformed."""
    state = (sqlstate or DEFAULT_SQLSTATE).ljust(5)[:5]
    out = b"\xff" + int(errno).to_bytes(2, "little")
    if protocol_41:
        out += b"#" + state.encode("ascii", "replace")
    return out + message.encode("utf-8", "replace")
