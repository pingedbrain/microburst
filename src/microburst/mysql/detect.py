"""Command + SQL detection — the mysql analog of pg/detect.py.

``operation`` is the lowercased SQL verb for ``COM_QUERY`` (``select``,
``insert``, ``begin``…), the ``stmt_*`` name for extended-protocol
commands, ``com_*`` for the rest, or ``startup`` for the connect
handshake. ``resource`` is a best-effort table-ish token — for
``COM_QUERY``/``COM_STMT_PREPARE`` the first identifier after a
``FROM``/``INTO``/``UPDATE``/``JOIN``/``TABLE`` keyword, reusing pg's
``sql_facts`` (generic SQL lexing, unchanged).

``COM_STMT_EXECUTE`` carries only a statement id — the connection's
``statements`` map (learned from forwarded COM_STMT_PREPARE text, the
pg named-statement analog) resolves it back to SQL so ``sql:`` rules
match executes. ``COM_STMT_FETCH`` and ``COM_STMT_CLOSE`` resolve the
same way for the fired event's ``path``.

Reply-shape knowledge also lives here (``REPLY_KIND``): which commands
get no server reply, which start a streaming sub-protocol the proxy
splices instead of inspecting, and which need the COM_STMT_PREPARE
metadata relay. Evidence: MYSQL DOCS per-command reference.
"""

from __future__ import annotations

from microburst.mysql import proto
from microburst.pg.detect import sql_facts

COMMAND_NAMES: dict[int, str] = {
    proto.COM_SLEEP: "com_sleep",
    proto.COM_QUIT: "com_quit",
    proto.COM_INIT_DB: "com_init_db",
    proto.COM_QUERY: "com_query",
    proto.COM_FIELD_LIST: "com_field_list",
    proto.COM_CREATE_DB: "com_create_db",
    proto.COM_DROP_DB: "com_drop_db",
    proto.COM_REFRESH: "com_refresh",
    proto.COM_SHUTDOWN: "com_shutdown",
    proto.COM_STATISTICS: "com_statistics",
    proto.COM_PROCESS_INFO: "com_process_info",
    proto.COM_CONNECT: "com_connect",
    proto.COM_PROCESS_KILL: "com_process_kill",
    proto.COM_DEBUG: "com_debug",
    proto.COM_PING: "com_ping",
    proto.COM_TIME: "com_time",
    proto.COM_DELAYED_INSERT: "com_delayed_insert",
    proto.COM_CHANGE_USER: "com_change_user",
    proto.COM_BINLOG_DUMP: "com_binlog_dump",
    proto.COM_TABLE_DUMP: "com_table_dump",
    proto.COM_CONNECT_OUT: "com_connect_out",
    proto.COM_REGISTER_SLAVE: "com_register_slave",
    proto.COM_STMT_PREPARE: "stmt_prepare",
    proto.COM_STMT_EXECUTE: "stmt_execute",
    proto.COM_STMT_SEND_LONG_DATA: "stmt_send_long_data",
    proto.COM_STMT_CLOSE: "stmt_close",
    proto.COM_STMT_RESET: "stmt_reset",
    proto.COM_SET_OPTION: "com_set_option",
    proto.COM_STMT_FETCH: "stmt_fetch",
    proto.COM_DAEMON: "com_daemon",
    proto.COM_BINLOG_DUMP_GTID: "com_binlog_dump_gtid",
    proto.COM_RESET_CONNECTION: "com_reset_connection",
}


def command_name(cmd: int) -> str:
    return COMMAND_NAMES.get(cmd, f"com_{cmd:02x}")


# Reply shape per command — what the server sends back, which decides
# how long the relay loop runs:
#
# * ``none`` — no reply at all (MYSQL DOCS). An injected ERR here would
#   be a packet the client never reads, so error rules skip + forward.
# * ``quit`` — COM_QUIT: forward, then close like the server does.
# * ``prepare`` — COM_STMT_PREPARE: prepare-OK + param/column defs.
# * ``splice`` — replication streams (binlog dump family) and cursor
#   fetches (COM_STMT_FETCH binary rows have an ambiguous 0x00 head):
#   relay becomes an unbounded stream — splice, documented passthrough.
# * ``single`` — exactly one reply packet whatever its head:
#   COM_STATISTICS answers a bare status string whose first byte would
#   otherwise be misread as a lenenc column count (MYSQL DOCS). The
#   deprecated internals COM_TIME/COM_DELAYED_INSERT get the same
#   defensive treatment.
# * ``generic`` — first packet classifies the reply: 0x00 OK / 0xFF ERR
#   (single packet), 0xFB LOCAL_INFILE (sub-dialog), anything else a
#   lenenc column count starting a ResultSet.
REPLY_KIND: dict[int, str] = {
    proto.COM_STATISTICS: "single",
    proto.COM_TIME: "single",
    proto.COM_DELAYED_INSERT: "single",
    proto.COM_SLEEP: "none",
    proto.COM_STMT_SEND_LONG_DATA: "none",
    proto.COM_STMT_CLOSE: "none",
    proto.COM_QUIT: "quit",
    proto.COM_STMT_PREPARE: "prepare",
    proto.COM_BINLOG_DUMP: "splice",
    proto.COM_BINLOG_DUMP_GTID: "splice",
    proto.COM_REGISTER_SLAVE: "splice",
    proto.COM_TABLE_DUMP: "splice",
    proto.COM_STMT_FETCH: "splice",
}


def reply_kind(cmd: int | None) -> str:
    return REPLY_KIND.get(cmd, "generic") if cmd is not None else "generic"


def command_facts(
    payload: bytes, statements: dict[int, str]
) -> tuple[str | None, str | None, str | None, int | None]:
    """(operation, resource, sql, stmt_id) for one command payload.

    ``sql`` is the text ``sql:``/``payload:`` matchers see — the query
    itself for COM_QUERY/COM_STMT_PREPARE, the tracked statement text
    for COM_STMT_EXECUTE/FETCH/CLOSE. ``resource`` is the table-ish
    token; COM_INIT_DB's is the database name.
    """
    if not payload:
        return None, None, None, None
    cmd = payload[0]
    sql = None
    stmt_id = None
    if cmd in (proto.COM_QUERY, proto.COM_STMT_PREPARE):
        sql = payload[1:].decode("utf-8", "replace")
        verb, table = sql_facts(sql)
        op = verb if cmd == proto.COM_QUERY else "stmt_prepare"
        return op, table, sql, None
    if cmd in (proto.COM_STMT_EXECUTE, proto.COM_STMT_CLOSE,
               proto.COM_STMT_FETCH):
        stmt_id = proto.stmt_id_of(payload)
        sql = statements.get(stmt_id) if stmt_id is not None else None
        _verb, table = sql_facts(sql) if sql else (None, None)
        return command_name(cmd), table, sql, stmt_id
    if cmd == proto.COM_INIT_DB:
        db = payload[1:].decode("utf-8", "replace").strip("\x00") or None
        return "com_init_db", db, None, None
    return command_name(cmd), None, None, None
