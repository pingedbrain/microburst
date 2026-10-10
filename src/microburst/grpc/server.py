"""gRPC data plane — a sibling transport riding cleartext HTTP/2 (h2c).

gRPC is not request→reply like the AWS pipeline nor a framed chat like
pg/redis: it is multiplexed HTTP/2 streams, one per RPC call. The proxy
therefore runs **two** ``h2.connection.H2Connection`` state machines —
a server-side one facing the client, a client-side one facing the
upstream — and maps streams between them. Both legs are prior-knowledge
h2c (the common local-dev gRPC setup); TLS termination is out of scope
(``grpcs://`` upstreams are rejected at the CLI seam).

Decision unit = **one RPC call**, taken when the client's request
HEADERS arrive: ``:path`` (``/package.Service/Method``) becomes
``operation = "package.service/method"`` (lowercased, leading slash
stripped — ``operation:`` matches it exactly), ``resource``/``args``
carry the raw ``:path``, ``service`` is ``grpc``. A rule decision then
arms faults on that stream:

* ``error: {code: …}`` — *trailers-only* response: one HEADERS frame
  carrying ``:status 200`` + ``content-type: application/grpc`` +
  ``grpc-status`` + ``grpc-message`` + END_STREAM. That is the legal
  shape real gRPC servers send when a call fails before the handler
  produces output; upstream is never contacted for the stream. Codes
  accept name or number (``"UNAVAILABLE"`` or ``14``).
* ``error:`` + ``partial_messages: N`` — **mid-stream error**: N real
  upstream DATA messages relay, then the configured trailers land
  instead of upstream's own ending. Partial data + failed status is
  exactly what a server dying mid-Send looks like to a client.
* ``partial_messages: N`` alone — N messages relay, then RST_STREAM
  (the per-RPC abort shape; the connection survives).
* ``latency`` — delays the request HEADERS forward → the client sees a
  slow first response byte. ``timeout_ms`` — stalls the stream N ms
  then proceeds. ``timeout: true`` — parks the stream forever: nothing
  forwards, inbound DATA goes unacknowledged (real flow-control
  backpressure), and the client's own deadline fires — it sees
  DEADLINE_EXCEEDED client-side, the honest semantics.
* ``reset: true`` — RST_STREAM on the client stream (error code
  INTERNAL_ERROR, the "handler died" shape). Connection-wide death is
  what ``cut_reply``/``cut_upload`` do: they abort the TCP link.
* ``cut_reply: {after_bytes|after_messages}`` — reply DATA dies
  mid-message/mid-stream at TCP level. ``cut_upload:`` — same on the
  client→server direction.

Flow control is **coupled**: inbound DATA is only
``acknowledge_received_data``'d once forwarded, so a stalled stream
drains the peer's window exactly like a stalled real server. Relay
throughput is bounded by the tighter of the two legs' windows; the
excess parks in per-stream pending queues flushed on WINDOW_UPDATE.

Non-gRPC h2 traffic (``content-type`` not ``application/grpc*``)
proxies transparently — transport faults apply, ``error:`` is skipped
with a fired-log note (a grpc-status trailer on a JSON endpoint is not
a real wire shape). HTTP status always stays 200 for grpc-level
errors — that *is* the protocol; ``error.status`` is ignored (noted).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from types import SimpleNamespace

from aiohttp import web
from h2.events import (
    ConnectionTerminated,
    DataReceived,
    InformationalResponseReceived,
    RemoteSettingsChanged,
    RequestReceived,
    ResponseReceived,
    StreamEnded,
    StreamReset,
    TrailersReceived,
    WindowUpdated,
)
from h2.exceptions import ProtocolError, StreamClosedError

from microburst.control import handle_control
from microburst.core.context import RequestContext
from microburst.framing import FramedStream
from microburst.grpc.proto import (
    DEFAULT_MESSAGE,
    RST_ERROR_CODE,
    error_trailers,
    grpc_status,
    headers_dict,
    is_grpc,
    new_client_conn,
    new_server_conn,
    operation_for,
    send_data_chunked,
    trailers_only,
)
from microburst.observe import emit_fired
from microburst.rules import FiredEvent, RuleEngine, describe
from microburst.stats import ProxyStats

logger = logging.getLogger("microburst.grpc")

SERVICE = "grpc"

# RST_STREAM error code sent upstream when the proxy abandons a call it
# injected a fault into — CANCEL (0x8), "we don't want this anymore".
_UPSTREAM_CANCEL = 0x8


@dataclass
class _Stream:
    """Per-RPC state bridging the two h2 state machines.

    ``unacked`` counts flow-controlled bytes received but not yet
    forwarded — the backpressure coupling. ``pending`` holds relay bytes
    that didn't fit the peer's outbound window yet.
    """

    sid: int                            # client-facing stream id
    usid: int | None = None             # upstream-facing stream id
    operation: str = ""
    path: str = ""
    grpc: bool = True                   # content-type application/grpc*
    event: FiredEvent | None = None
    held: bool = False                  # timeout:true — parked forever
    faulted: bool = False               # terminal injected fault sent
    req_ended: bool = False             # client's END_STREAM seen
    u_end_sent: bool = False            # we sent END_STREAM upstream
    resp_ended: bool = False            # the client-bound side was ended
                                        # (upstream trailers/end, our
                                        # injected end, or an RST)
    started: float = 0.0                # monotonic ts at upstream open
    # armed reply-side faults (set at decision time)
    fault_code: str | None = None       # grpc-status for trailer injection
    fault_message: str | None = None
    partial_messages: int | None = None
    cut_bytes: int | None = None        # cut_reply.after_bytes → TCP abort
    cut_msgs: int | None = None         # cut_reply.after_messages → TCP abort
    s2c_cutoff: bool = False            # N messages relayed — fault lands
                                      # on the next s2c frame boundary
    # armed upload-side faults
    up_cut_bytes: int | None = None     # cut_upload.after_bytes
    up_cut_msgs: int | None = None      # cut_upload.after_messages
    # grpc message boundary splitters (armed streams only)
    s2c_split: FramedStream | None = None
    c2s_split: FramedStream | None = None
    s2c_relayed: int = 0                # complete messages sent to client
    # flow-control coupling
    c2s_pending: deque = field(default_factory=deque)
    c2s_unacked: int = 0
    s2c_pending: deque = field(default_factory=deque)
    s2c_unacked: int = 0

    def done(self) -> bool:
        """Safe to GC: a terminal fault landed, the parked call's client
        finished talking, or both directions ended. (The client's
        END_STREAM arrives long before the reply — a unary call is
        ``req_ended`` while the response still streams.)"""
        if self.faulted:
            return True
        if self.held:
            return self.req_ended
        return self.req_ended and self.resp_ended


class GrpcProxy:
    """Decision-plane state for grpc mode — the surface control.py reads.

    Same shape as ``pg.PgProxy`` / ``redis.RedisProxy``:
    ``engine``/``fired``/``fault_counts``/``stats``/``requests_seen``/
    ``_listeners`` behave identically.
    """

    def __init__(
        self,
        upstream_host: str,
        upstream_port: int,
        rules: list[dict] | None = None,
        fired_capacity: int = 2000,
    ) -> None:
        self.upstream_host = upstream_host
        self.upstream_port = upstream_port
        self.stats = ProxyStats()
        self.engine = RuleEngine()
        if rules:
            self.engine.set_rules(rules)
        self.fired: deque[FiredEvent] = deque(maxlen=fired_capacity)
        self.fault_counts: Counter = Counter()
        self._listeners: set[asyncio.Queue] = set()
        self.requests_seen = 0
        # control.py /health reads .upstream.base_url and .resign
        self.upstream = SimpleNamespace(
            base_url=f"grpc://{upstream_host}:{upstream_port}",
            resign=False,
        )

    def subscribe_fired(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=100)
        self._listeners.add(q)
        return q

    def unsubscribe_fired(self, q: asyncio.Queue) -> None:
        self._listeners.discard(q)

    async def handle_control(self, request: web.Request):
        return await handle_control(self, request)


class GrpcConnection:
    """One h2c client connection — two H2Connection state machines
    bridged per stream."""

    def __init__(
        self,
        proxy: GrpcProxy,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self.proxy = proxy
        self.reader = reader            # client → proxy
        self.writer = writer            # proxy → client
        self.ureader: asyncio.StreamReader | None = None
        self.uwriter: asyncio.StreamWriter | None = None
        self.cconn = new_server_conn()  # faces the downstream client
        self.uconn = new_client_conn()  # faces the upstream gRPC server
        self.streams: dict[int, _Stream] = {}
        self.ustreams: dict[int, _Stream] = {}

    # -- lifecycle ------------------------------------------------------------

    async def run(self) -> None:
        try:
            self.ureader, self.uwriter = await asyncio.open_connection(
                self.proxy.upstream_host, self.proxy.upstream_port
            )
        except OSError as e:
            # Upstream unreachable: dead link, no synthesized error —
            # nothing protocol-shaped exists to refuse inside yet.
            logger.debug("grpc upstream connect failed: %r", e)
            return
        self.uconn.initiate_connection()   # client preface + SETTINGS
        self.cconn.initiate_connection()   # server SETTINGS
        await self._flush()
        pumps = [
            asyncio.ensure_future(self._pump_client()),
            asyncio.ensure_future(self._pump_upstream()),
        ]
        _done, pending = await asyncio.wait(
            pumps, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        self.close()
        await asyncio.gather(*pumps, return_exceptions=True)

    def close(self) -> None:
        for w in (self.writer, self.uwriter):
            if w is not None:
                with contextlib.suppress(Exception):
                    w.close()

    async def _flush(self) -> None:
        """Push each state machine's queued bytes to its socket."""
        out = self.cconn.data_to_send()
        if out:
            self.writer.write(out)
        if self.uwriter is not None:
            out = self.uconn.data_to_send()
            if out:
                self.uwriter.write(out)
        with contextlib.suppress(ConnectionError, OSError):
            await self.writer.drain()
        if self.uwriter is not None:
            with contextlib.suppress(ConnectionError, OSError):
                await self.uwriter.drain()

    # -- pumps ------------------------------------------------------------------

    async def _pump_client(self) -> None:
        try:
            while True:
                data = await self.reader.read(65536)
                if not data:
                    return
                try:
                    events = self.cconn.receive_data(data)
                except ProtocolError as e:
                    # h2 already queued a GOAWAY — flush on teardown.
                    logger.debug("grpc client protocol error: %r", e)
                    return
                for ev in events:
                    r = await self._client_event(ev)
                    if r == "abort":
                        # flush the cut prefix to the client first —
                        # the wire shape is "some bytes, then death"
                        await self._flush()
                        self._abort_client()
                        return
                    if r == "closed":
                        await self._flush()
                        return
                await self._flush()
        except (ConnectionError, OSError, asyncio.IncompleteReadError):
            return

    async def _pump_upstream(self) -> None:
        assert self.ureader is not None
        try:
            while True:
                data = await self.ureader.read(65536)
                if not data:
                    return
                try:
                    events = self.uconn.receive_data(data)
                except ProtocolError as e:
                    logger.debug("grpc upstream protocol error: %r", e)
                    return
                for ev in events:
                    r = self._upstream_event(ev)
                    if r == "abort":
                        await self._flush()
                        self._abort_client()
                        return
                    if r == "closed":
                        await self._flush()
                        return
                await self._flush()
        except (ConnectionError, OSError, asyncio.IncompleteReadError):
            return

    # -- client-side events ------------------------------------------------------

    async def _client_event(self, ev) -> str | None:
        if isinstance(ev, RequestReceived):
            return await self._new_stream(ev)
        if isinstance(ev, DataReceived):
            return self._client_data(ev)
        if isinstance(ev, TrailersReceived):
            # Client trailers (rare — legal h2, grpc clients don't
            # normally send them): relay upstream verbatim, ended.
            st = self.streams.get(ev.stream_id)
            if st is not None and st.usid is not None and not st.faulted:
                with contextlib.suppress(StreamClosedError, ProtocolError):
                    self.uconn.send_headers(
                        st.usid, ev.headers, end_stream=True
                    )
                st.u_end_sent = True
        if isinstance(ev, StreamEnded):
            self._client_ended(ev)
        elif isinstance(ev, StreamReset):
            self._client_reset(ev)
        elif isinstance(ev, ConnectionTerminated):
            # Client GOAWAY → propagate upstream, then die.
            with contextlib.suppress(Exception):
                self.uconn.close_connection()
            await self._flush()
            return "closed"
        # WindowUpdated / RemoteSettingsChanged: more outbound room
        # toward the client — flush parked s2c bytes.
        elif isinstance(ev, (WindowUpdated, RemoteSettingsChanged)):
            self._drain_s2c_all(getattr(ev, "stream_id", 0) or 0)
        return None

    async def _new_stream(self, ev: RequestReceived) -> str | None:
        """The decision point: request HEADERS = one RPC call."""
        headers = headers_dict(ev.headers)
        path = headers.get(":path", "/")
        st = _Stream(
            sid=ev.stream_id,
            operation=operation_for(path),
            path=path,
            grpc=is_grpc(ev.headers),
        )
        self.streams[ev.stream_id] = st
        self.proxy.requests_seen += 1
        self.proxy.stats.record_request(SERVICE)

        ctx = RequestContext(
            service=SERVICE,
            operation=st.operation,
            resource=path,
            args=path,
        )
        decision = self.proxy.engine.decide(ctx)
        if decision is None:
            self._open_upstream(st, ev.headers)
            return None

        event = FiredEvent(
            ts=time.time(),
            rule_id=decision.rule.id,
            service=SERVICE,
            operation=st.operation,
            resource=path,
            region=None,
            action=describe(decision),
            path=path,
        )
        st.event = event
        emit_fired(self.proxy, event)

        # Effect order mirrors the other transports: latency → reset →
        # timeout → error → forward (stream faults arm for the reply
        # path). A sleep here parks the whole connection's pump —
        # documented MVP caveat for multiplexed traffic.
        if decision.latency_ms:
            await asyncio.sleep(decision.latency_ms / 1000)
        if decision.reset:
            self._reset_stream(st)
            return None
        if decision.timeout_ms is not None:
            if decision.timeout_ms < 0:
                # `timeout: true` — park the call. Nothing forwards,
                # inbound DATA stays unacknowledged (real backpressure);
                # the client's deadline fires and it sees
                # DEADLINE_EXCEEDED client-side. The honest stall.
                st.held = True
                _note(event, "call parked — never forwarded upstream")
                return None
            await asyncio.sleep(decision.timeout_ms / 1000)

        rule = decision.rule
        if decision.error is not None and not st.grpc:
            _note(
                event,
                "skipped: non-grpc content-type — a grpc-status "
                "trailer isn't a real error shape there",
            )
        elif decision.error is not None and rule.partial_messages is None:
            # trailers-only: the legal early-error shape — upstream is
            # never contacted for this stream.
            code, why = grpc_status(decision.error.code)
            if why:
                _note(event, why)
            if decision.error.status is not None:
                _note(
                    event,
                    "error.status ignored — gRPC errors ride "
                    "grpc-status trailers over HTTP 200",
                )
            msg = decision.error.message or DEFAULT_MESSAGE
            try:
                self.cconn.send_headers(
                    st.sid, trailers_only(code, msg), end_stream=True
                )
            except (StreamClosedError, ProtocolError):
                return None
            st.faulted = True
            _note(event, f"trailers-only grpc-status={code}")
            return None
        elif decision.error is not None:
            # mid-stream error — the trailers land after partial data.
            code, why = grpc_status(decision.error.code)
            if why:
                _note(event, why)
            st.fault_code = code
            st.fault_message = decision.error.message or DEFAULT_MESSAGE

        self._arm(st, decision)
        self._open_upstream(st, ev.headers)
        return None

    def _arm(self, st: _Stream, decision) -> None:
        """Attach the fired rule's stream faults to this RPC."""
        rule = decision.rule
        st.partial_messages = rule.partial_messages
        st.cut_bytes = rule.cut_reply_bytes
        st.cut_msgs = rule.cut_reply_messages
        if st.partial_messages is not None or st.cut_msgs is not None:
            st.s2c_split = FramedStream("grpc")
        if rule.partial_rows is not None:
            _note(st.event, "partial_rows is pg-only — use partial_messages")
        if rule.respond is not None:
            _note(st.event, "respond unsupported in grpc mode")
        if rule.corrupt is not None:
            _note(st.event, "corrupt unsupported in grpc mode")
        if rule.response is not None:
            _note(st.event, "response mutations unsupported in grpc mode")
        # request-side faults: cut_upload arms the upload gate
        xf = decision.request_fault
        if xf is None:
            return
        st.up_cut_bytes = xf.after_bytes
        st.up_cut_msgs = xf.after_messages
        if st.up_cut_msgs is not None and st.c2s_split is None:
            st.c2s_split = FramedStream("grpc")
        unsupported = []
        if xf.rate_kbps:
            unsupported.append("slow_upload")
        if xf.corrupt_at_message is not None:
            unsupported.append("corrupt_upload")
        if xf.after_frac is not None:
            unsupported.append("cut_upload.after_frac")
        if unsupported:
            _note(
                st.event,
                f"{'/'.join(unsupported)} unsupported in grpc mode",
            )

    def _open_upstream(self, st: _Stream, headers) -> None:
        """Create the upstream half of the stream and send HEADERS."""
        assert self.uconn is not None
        st.usid = self.uconn.get_next_available_stream_id()
        self.ustreams[st.usid] = st
        st.started = time.monotonic()
        try:
            self.uconn.send_headers(st.usid, headers)
        except (StreamClosedError, ProtocolError) as e:
            logger.debug("grpc upstream headers refused: %r", e)

    def _client_data(self, ev: DataReceived) -> str | None:
        st = self.streams.get(ev.stream_id)
        if st is not None and st.held:
            # Held calls deliberately DON'T ack — unacknowledged bytes
            # drain the client's window = the stalled-server shape.
            return None
        if st is None or st.usid is None or st.faulted or st.resp_ended:
            # Dead stream — drop the data, but ack it so the
            # connection-level window stays healthy for live calls.
            self._ack(self.cconn, ev.stream_id, ev.flow_controlled_length)
            return None
        st.c2s_unacked += ev.flow_controlled_length
        data, abort = self._gate_upload(st, ev.data)
        if data:
            st.c2s_pending.append(data)
            sent = self._drain(self.uconn, st.usid, st.c2s_pending)
            self._ack_sent(self.cconn, st, sent, "c2s")
        if abort:
            _note(st.event, "cut_upload: link aborted mid-upload")
            return "abort"
        return None

    def _gate_upload(self, st: _Stream, data: bytes) -> tuple[bytes, bool]:
        """Armed cut_upload gates → (payload to relay, abort_link)."""
        if st.up_cut_bytes is not None:
            if len(data) > st.up_cut_bytes:
                allowed, st.up_cut_bytes = data[: st.up_cut_bytes], 0
                return allowed, True
            st.up_cut_bytes -= len(data)
            return data, False
        if st.up_cut_msgs is not None:
            assert st.c2s_split is not None
            out: list[bytes] = []
            for msg in st.c2s_split.feed(data):
                if st.up_cut_msgs <= 0:
                    return b"".join(out), True
                st.up_cut_msgs -= 1
                out.append(msg)
            if st.c2s_split.failed:
                tail = st.c2s_split.drain()
                if tail:
                    out.append(tail)
                st.up_cut_msgs = None
                _note(
                    st.event,
                    "malformed grpc prefix — fell back to bytes",
                )
            return b"".join(out), False
        return data, False

    def _client_ended(self, ev: StreamEnded) -> None:
        st = self.streams.get(ev.stream_id)
        if st is None:
            return
        st.req_ended = True
        if st.usid is not None and not st.u_end_sent:
            st.u_end_sent = True
            with contextlib.suppress(StreamClosedError, ProtocolError):
                self.uconn.end_stream(st.usid)
        self._maybe_gc(st)

    def _client_reset(self, ev: StreamReset) -> None:
        """Client RST (deadline/cancel) — propagate upstream, drop it."""
        st = self.streams.pop(ev.stream_id, None)
        if st is None:
            return
        if st.usid is not None:
            self.ustreams.pop(st.usid, None)
            with contextlib.suppress(StreamClosedError, ProtocolError):
                self.uconn.reset_stream(st.usid)

    def _reset_stream(self, st: _Stream) -> None:
        """`reset: true` → RST_STREAM on the client stream."""
        st.faulted = True
        try:
            self.cconn.reset_stream(st.sid, error_code=RST_ERROR_CODE)
        except (StreamClosedError, ProtocolError):
            return
        _note(st.event, "reset → RST_STREAM(INTERNAL_ERROR)")

    # -- upstream-side events ----------------------------------------------------

    def _upstream_event(self, ev) -> str | None:
        if isinstance(ev, DataReceived):
            return self._upstream_data(ev)
        if isinstance(
            ev,
            (ResponseReceived, TrailersReceived, InformationalResponseReceived),
        ):
            self._upstream_headers(ev)
        elif isinstance(ev, StreamEnded):
            self._upstream_ended(ev)
        elif isinstance(ev, StreamReset):
            self._upstream_reset(ev)
        elif isinstance(ev, ConnectionTerminated):
            # Upstream GOAWAY → propagate downstream, then die.
            with contextlib.suppress(Exception):
                self.cconn.close_connection()
            return "closed"
        elif isinstance(ev, (WindowUpdated, RemoteSettingsChanged)):
            self._drain_c2s_all(getattr(ev, "stream_id", 0) or 0)
        return None

    def _upstream_headers(self, ev) -> None:
        st = self.ustreams.get(ev.stream_id)
        if st is None or st.faulted:
            return
        if st.s2c_cutoff:
            # N messages already relayed — upstream's trailers lose to
            # the injected error (or the call ends if no fault armed).
            self._inject_midstream(st)
            return
        if isinstance(ev, TrailersReceived) and self._partial_armed(st):
            # Upstream ended before the partial_messages threshold —
            # relay its real ending, note the no-op.
            _note(
                st.event,
                f"upstream ended after {st.s2c_relayed} messages — "
                f"partial_messages={st.partial_messages} never reached",
            )
        end = ev.stream_ended is not None
        try:
            self.cconn.send_headers(st.sid, ev.headers, end_stream=end)
        except (StreamClosedError, ProtocolError):
            return
        if end:
            st.resp_ended = True

    def _upstream_data(self, ev: DataReceived) -> str | None:
        st = self.ustreams.get(ev.stream_id)
        if st is None:
            self._ack(self.uconn, ev.stream_id, ev.flow_controlled_length)
            return None
        st.s2c_unacked += ev.flow_controlled_length
        if st.faulted:
            self._ack(self.uconn, ev.stream_id, ev.flow_controlled_length)
            st.s2c_unacked = max(0, st.s2c_unacked - ev.flow_controlled_length)
            return None
        if st.s2c_cutoff:
            # Next boundary after N relayed messages → the injected
            # fault lands instead of upstream's data.
            self._inject_midstream(st)
            self._ack(self.uconn, ev.stream_id, ev.flow_controlled_length)
            st.s2c_unacked = max(0, st.s2c_unacked - ev.flow_controlled_length)
            return None

        data, abort = self._gate_reply(st, ev.data)
        if data:
            st.s2c_pending.append(data)
            sent = self._drain(self.cconn, st.sid, st.s2c_pending)
            self._ack_sent(self.uconn, st, sent, "s2c")
        if abort:
            _note(st.event, "cut_reply: link aborted mid-reply")
            return "abort"
        if st.s2c_split is not None and st.s2c_split.failed:
            _note(st.event, "malformed grpc prefix — fell back to bytes")
            tail = st.s2c_split.drain()
            if tail:
                st.s2c_pending.append(tail)
                sent = self._drain(self.cconn, st.sid, st.s2c_pending)
                self._ack_sent(self.uconn, st, sent, "s2c")
            st.s2c_split = None
            st.partial_messages = None
            st.cut_msgs = None
        return None

    def _gate_reply(self, st: _Stream, data: bytes) -> tuple[bytes, bool]:
        """Armed reply gates → (payload to relay, abort_link)."""
        if st.cut_bytes is not None:
            if len(data) > st.cut_bytes:
                allowed, st.cut_bytes = data[: st.cut_bytes], 0
                return allowed, True
            st.cut_bytes -= len(data)
            return data, False
        if st.s2c_split is None:
            return data, False
        out: list[bytes] = []
        for msg in st.s2c_split.feed(data):
            if st.cut_msgs is not None and st.s2c_relayed >= st.cut_msgs:
                return b"".join(out), True
            if (
                st.partial_messages is not None
                and st.s2c_relayed >= st.partial_messages
            ):
                # Message N+1 → the armed fault lands instead.
                st.s2c_cutoff = True
                break
            st.s2c_relayed += 1
            out.append(msg)
            if st.cut_msgs is not None and st.s2c_relayed >= st.cut_msgs:
                # Nth message relayed — the link dies right after.
                return b"".join(out), True
            if (
                st.partial_messages is not None
                and st.s2c_relayed >= st.partial_messages
            ):
                st.s2c_cutoff = True
        return b"".join(out), False

    def _inject_midstream(self, st: _Stream) -> None:
        """The mid-stream fault: error trailers when armed with
        ``error:``, RST_STREAM otherwise. Upstream's stream gets CANCEL
        so the real server stops working the call."""
        if st.faulted:
            return
        st.faulted = True
        try:
            if st.fault_code is not None:
                self.cconn.send_headers(
                    st.sid,
                    error_trailers(
                        st.fault_code,
                        st.fault_message or DEFAULT_MESSAGE,
                    ),
                    end_stream=True,
                )
                _note(
                    st.event,
                    f"partial_messages: {st.s2c_relayed} relayed then "
                    f"grpc-status={st.fault_code}",
                )
            else:
                self.cconn.reset_stream(st.sid, error_code=RST_ERROR_CODE)
                _note(
                    st.event,
                    f"partial_messages: {st.s2c_relayed} relayed then "
                    "RST_STREAM",
                )
        except (StreamClosedError, ProtocolError):
            pass
        st.resp_ended = True
        st.s2c_pending.clear()
        if st.usid is not None:
            with contextlib.suppress(StreamClosedError, ProtocolError):
                self.uconn.reset_stream(st.usid, error_code=_UPSTREAM_CANCEL)

    def _partial_armed(self, st: _Stream) -> bool:
        return st.partial_messages is not None or st.fault_code is not None

    def _upstream_ended(self, ev: StreamEnded) -> None:
        st = self.ustreams.pop(ev.stream_id, None)
        if st is None:
            return
        if st.s2c_cutoff and not st.faulted:
            self._inject_midstream(st)
            self._maybe_gc(st)
            return
        if not st.resp_ended and not st.faulted:
            # Upstream's last HEADERS already carried END_STREAM when it
            # was trailers-shaped; this covers DATA-carried ends.
            with contextlib.suppress(StreamClosedError, ProtocolError):
                self.cconn.end_stream(st.sid)
            st.resp_ended = True
            self.proxy.stats.record_upstream_ms(
                (time.monotonic() - st.started) * 1000
            )
            self.proxy.stats.record_forward()
        self._maybe_gc(st)

    def _upstream_reset(self, ev: StreamReset) -> None:
        """Upstream RST — propagate the same code to the client."""
        st = self.ustreams.pop(ev.stream_id, None)
        if st is None:
            return
        st.faulted = True
        with contextlib.suppress(StreamClosedError, ProtocolError):
            self.cconn.reset_stream(st.sid, error_code=ev.error_code or 0)
        self.streams.pop(st.sid, None)

    # -- flow-control coupling -----------------------------------------------------

    def _drain(self, conn, sid: int | None, pending: deque) -> int:
        """Send pending chunks through ``conn`` while its outbound
        window allows; returns bytes actually put on the wire."""
        if sid is None:
            return 0
        sent = 0
        while pending:
            chunk = pending[0]
            try:
                n = send_data_chunked(conn, sid, chunk)
            except (StreamClosedError, ProtocolError):
                pending.clear()
                break
            if n == 0:
                break
            sent += n
            if n == len(chunk):
                pending.popleft()
            else:
                pending[0] = chunk[n:]
        return sent

    def _drain_s2c_all(self, sid: int) -> None:
        """A client-side window refresh flushes parked reply bytes."""
        if sid:
            st = self.streams.get(sid)
            if st is not None:
                sent = self._drain(self.cconn, st.sid, st.s2c_pending)
                self._ack_sent(self.uconn, st, sent, "s2c")
            return
        for st in list(self.streams.values()):
            if st.s2c_pending:
                sent = self._drain(self.cconn, st.sid, st.s2c_pending)
                self._ack_sent(self.uconn, st, sent, "s2c")

    def _drain_c2s_all(self, sid: int) -> None:
        """An upstream-side window refresh flushes parked upload bytes."""
        if sid:
            st = self.ustreams.get(sid)
            if st is not None:
                sent = self._drain(self.uconn, st.usid, st.c2s_pending)
                self._ack_sent(self.cconn, st, sent, "c2s")
            return
        for st in list(self.ustreams.values()):
            if st.c2s_pending:
                sent = self._drain(self.uconn, st.usid, st.c2s_pending)
                self._ack_sent(self.cconn, st, sent, "c2s")

    def _ack_sent(self, conn, st: _Stream, sent: int, direction: str) -> None:
        """Acknowledge inbound bytes on the RECEIVING conn once they've
        been forwarded — the backpressure coupling. direction c2s → ack
        on cconn (the client leg); s2c → ack on uconn."""
        if sent <= 0:
            return
        if direction == "c2s":
            n = min(sent, st.c2s_unacked)
            st.c2s_unacked -= n
            self._ack(conn, st.sid, n)
        else:
            n = min(sent, st.s2c_unacked)
            st.s2c_unacked -= n
            self._ack(conn, st.usid, n)

    def _ack(self, conn, sid: int | None, amount: int) -> None:
        if sid is None or amount <= 0:
            return
        with contextlib.suppress(StreamClosedError, ProtocolError, ValueError):
            conn.acknowledge_received_data(amount, sid)

    # -- small helpers ------------------------------------------------------------

    def _maybe_gc(self, st: _Stream) -> None:
        if st.done():
            self.streams.pop(st.sid, None)
            if st.usid is not None:
                self.ustreams.pop(st.usid, None)

    def _abort_client(self) -> None:
        """TCP RST to the client — mirrors reset.apply on the HTTP path."""
        transport = getattr(self.writer, "transport", None)
        if transport is not None:
            transport.abort()


def _note(event: FiredEvent | None, text: str) -> None:
    if event is None:
        return
    event.note = f"{event.note}; {text}" if event.note else text


async def handle_client(
    proxy: GrpcProxy,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    """asyncio.start_server callback — one GrpcConnection per client."""
    conn = GrpcConnection(proxy, reader, writer)
    try:
        await conn.run()
    except (EOFError, ConnectionError, ProtocolError) as e:
        logger.debug("grpc connection closed: %r", e)
    except Exception:  # defensive proxy boundary — never leak a client
        logger.exception("grpc connection handler failed")
    finally:
        conn.close()


async def _serve(
    proxy: GrpcProxy,
    host: str,
    port: int,
    control_port: int,
    watch_config: str | None,
) -> None:
    from microburst.app import make_control_app

    server = await asyncio.start_server(
        lambda r, w: handle_client(proxy, r, w), host, port
    )
    app = make_control_app(proxy, watch_config=watch_config)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, control_port)
    await site.start()

    bound = server.sockets[0].getsockname()[1]
    print(
        f"microburst listening on grpc://{host}:{bound} (h2c) → "
        f"grpc://{proxy.upstream_host}:{proxy.upstream_port}",
        flush=True,
    )
    print(
        f"control API: http://{host}:{control_port}/_microburst/health",
        flush=True,
    )
    print(
        f"point your app: grpcurl -plaintext {host}:{bound} list",
        flush=True,
    )
    async with server:
        await server.serve_forever()


def run_grpc(
    host: str,
    port: int,
    upstream_host: str,
    upstream_port: int,
    control_port: int,
    rules: list[dict] | None,
    watch_config: str | None = None,
) -> int:
    """Entry point for ``microburst --protocol grpc``."""
    proxy = GrpcProxy(upstream_host, upstream_port, rules=rules)
    try:
        asyncio.run(_serve(proxy, host, port, control_port, watch_config))
    except KeyboardInterrupt:
        pass
    return 0
