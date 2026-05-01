"""
backend/app/services/sidecars/blofin_account_sidecar.py
=========================================================
BloFin private WebSocket sidecar for Manager Live data.

What this replaces:
  The /api/manager/live/* read endpoints currently serve from
  exchange_snapshots, which is refreshed every 5 minutes by the
  sync_exchange_snapshots cron. That gives the dashboard 5-minute
  staleness in the worst case. This sidecar replaces the staleness
  ceiling with near-real-time updates from BloFin's WebSocket account
  + positions + orders + orders-algo channels, written into Redis on
  every push. The cron stays in place as a safety net — if the sidecar
  dies, endpoints fall back to the DB-cached snapshot and surface a
  "rest_fallback" stale_source flag.

Architecture (per the build plan agreed with the user):
  * One asyncio process per (venue, connection_id). v1 = BloFin only,
    one connection picked from user_mgmt.exchange_connections.
  * State machine in blofin_ws_state.py drives the connect/auth/
    subscribe lifecycle; this module is the I/O glue layer that
    services the state machine's pending_actions queue.
  * Redis writer with Lua atomic script — account/positions/heartbeat
    update in one transaction so readers never observe a half-state.
  * Reconnect: infinite attempts, exponential backoff with ±25%
    jitter, capped at 60s. Three- and ten-failure escalation log
    markers fire structured lines for the cron-mailer log scraper.
  * REST seed runs on every (re)connection BEFORE subscribe — orders
    and orders-algo channels do not push initial state, so a partial
    seed would lie to the dashboard.

BloFin protocol gotchas locked down:
  * Auth signature is base64(hex_digest) with a nonce field — see
    blofin_ws_protocol.build_login_frame for the exact derivation.
  * Heartbeat is application-level "ping"/"pong" text frames at 15s.
  * Production WS: wss://openapi.blofin.com/ws/private (no version path).

Process lifecycle:
  * Spawned by docker-compose service blofin-sidecar.
  * Gated by LIVE_SIDECAR_ENABLED — when false, the entrypoint logs
    "disabled by config" and exits 0; container restart policy stays
    in 'unless-stopped' but the process is a no-op.
  * Graceful SIGTERM: closes WS, flushes pending Redis writes, exits 0.
  * Auth rejection: exit code 78 with an explicit log line a human
    can act on. Container restart policy will cycle the process; the
    diagnostic loop is intentional.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import signal
import sys
import time
from typing import Any

import psycopg2
import websockets
from psycopg2.extras import RealDictCursor

from ...core.config import settings
from ..encryption import decrypt_key
from .blofin_ws_protocol import (
    PING_INTERVAL_S,
    PING_RECV_TIMEOUT_S,
    PING_TEXT,
    WS_PRIVATE_PROD,
    build_login_frame,
    build_subscribe_frame,
    classify_message,
)
from .blofin_ws_state import (
    Action,
    Event,
    Phase,
    StateMachine,
    SUBSCRIBE_CHANNELS,
    compute_backoff,
)
from .redis_writer import RedisWriter


log = logging.getLogger("blofin_sidecar")


# Exit codes per spec.
EXIT_OK = 0
EXIT_CONFIG_ERROR = 78  # auth rejected, no config-file fix → restart loop


# ── Credential loading ───────────────────────────────────────────────


def _load_active_blofin_connection() -> dict[str, Any] | None:
    """Load the first active BloFin connection. v1 = single-connection
    sidecar, so we pick whichever active row Postgres returns first
    (deterministic per-row but not strictly defined ordering).

    Returns dict with decrypted api_key/api_secret/passphrase plus
    connection_id, or None if no active BloFin connection exists."""
    conn = psycopg2.connect(
        host=os.environ.get("DB_HOST", settings.DB_HOST),
        port=int(os.environ.get("DB_PORT", settings.DB_PORT)),
        dbname=os.environ.get("DB_NAME", settings.DB_NAME),
        user=os.environ.get("DB_USER", settings.DB_USER),
        password=os.environ.get("DB_PASSWORD", settings.DB_PASSWORD),
        connect_timeout=5,
    )
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT connection_id, label, api_key_enc, api_secret_enc, passphrase_enc
              FROM user_mgmt.exchange_connections
             WHERE exchange = 'blofin' AND status = 'active'
             ORDER BY connection_id
             LIMIT 1
        """)
        row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return None
    return {
        "connection_id": str(row["connection_id"]),
        "label": row["label"],
        "api_key": decrypt_key(row["api_key_enc"]) or "",
        "api_secret": decrypt_key(row["api_secret_enc"]) or "",
        "passphrase": decrypt_key(row["passphrase_enc"]) or "",
    }


# ── Push handlers ────────────────────────────────────────────────────


def _on_account_push(payload: dict[str, Any]) -> dict[str, Any]:
    """BloFin pushes the full account state in `data` (not an array).
    Normalize to the same shape the read endpoints consume from
    exchange_snapshots so the Redis-first read path is a drop-in
    substitution.

    Endpoints expect:
      total_equity_usd, available_usd, used_margin_usd, unrealized_pnl
    USDT row of `details[]` carries the per-currency fields; we surface
    them at the top level for the endpoint readers."""
    data = payload.get("data") or {}
    if not isinstance(data, dict):
        return {}
    details = data.get("details") or []
    usdt = next(
        (d for d in details
         if isinstance(d, dict) and (d.get("currency") or "").upper() == "USDT"),
        None,
    )
    available_usd = _to_float(
        (usdt or {}).get("availableEquity") or (usdt or {}).get("available")
    )
    used_margin_usd = _to_float(
        (usdt or {}).get("frozen") or (usdt or {}).get("orderFrozen")
    )
    unrealized_pnl = _to_float((usdt or {}).get("unrealizedPnl"))
    return {
        "ts": data.get("ts"),
        "total_equity_usd": _to_float(data.get("totalEquity")),
        "isolated_equity_usd": _to_float(data.get("isolatedEquity")),
        "available_usd": available_usd,
        "used_margin_usd": used_margin_usd,
        "unrealized_pnl": unrealized_pnl,
        # Keep raw details for any future per-currency view.
        "details": details,
    }


def _on_positions_push(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalize each position so the existing manager_live_state helpers
    handle direction and symbol parsing identically to the REST path."""
    raw = payload.get("data") or []
    if not isinstance(raw, list):
        return []
    out = []
    for p in raw:
        if not isinstance(p, dict):
            continue
        # Net-mode: BloFin returns positionSide='net' and the sign of
        # `positions` carries direction. The existing
        # manager_live_state.derive_position_side() helper does this
        # conversion at the read endpoint, so we pass the raw fields
        # through unchanged. The trader_blofin path uses the same.
        out.append({
            "instId": p.get("instId"),
            "symbol": (p.get("instId") or "").replace("-", ""),
            "side": p.get("positionSide"),
            "positions": p.get("positions"),
            "available_positions": p.get("availablePositions"),
            "size": _to_float(p.get("positions")),
            "entry_price": _to_float(p.get("averagePrice")),
            "mark_price": _to_float(p.get("markPrice")),
            "unrealized_pnl": _to_float(p.get("unrealizedPnl")),
            "unrealized_pnl_ratio": _to_float(p.get("unrealizedPnlRatio")),
            "leverage": _to_float(p.get("leverage")),
            "liquidation_price": _to_float(p.get("liquidationPrice")),
            "margin_mode": p.get("marginMode"),
            "initial_margin": _to_float(p.get("initialMargin")),
            "margin": _to_float(p.get("margin")),
            "margin_ratio": _to_float(p.get("marginRatio")),
            "maintenance_margin": _to_float(p.get("maintenanceMargin")),
            "adl": p.get("adl"),
            "create_time": p.get("createTime"),
            "update_time": p.get("updateTime"),
        })
    return out


def _on_orders_push(payload: dict[str, Any]) -> list[dict[str, Any]]:
    raw = payload.get("data") or []
    if not isinstance(raw, list):
        return []
    return [o for o in raw if isinstance(o, dict)]


def _to_float(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ── Sidecar driver ───────────────────────────────────────────────────


class BlofinAccountSidecar:
    """Glues the StateMachine to a real WebSocket connection and Redis
    writer. One instance per connection_id."""

    def __init__(self, *, conn: dict[str, Any]):
        self.api_key = conn["api_key"]
        self.api_secret = conn["api_secret"]
        self.passphrase = conn["passphrase"]
        self.connection_id = conn["connection_id"]
        self.label = conn.get("label") or "blofin"

        self.sm = StateMachine()
        self.writer = RedisWriter(connection_id=self.connection_id)
        self.ws: Any = None
        self.shutdown_event = asyncio.Event()
        self._rng = random.Random()

        # Diagnostics counters for the periodic health log line.
        self._stats = {
            "messages_received": 0,
            "redis_writes": 0,
            "reconnects": 0,
            "connected_at": None,  # set on entering Phase.CONNECTED
        }

    # ── Public entrypoint ────────────────────────────────────────────

    async def run(self) -> int:
        """Run forever (until SIGTERM or auth-fatal). Returns the
        process exit code."""
        # Hook SIGTERM/SIGINT so the asyncio event loop can shut down
        # gracefully — close WS, flush Redis, return 0.
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._request_shutdown)

        log.info(
            "sidecar starting: connection_id=%s label=%s",
            self.connection_id, self.label,
        )

        # Periodic structured health line every 60s — visible in
        # docker logs without grepping.
        health_task = asyncio.create_task(self._health_log_loop())

        try:
            self.sm.start()
            while not self.shutdown_event.is_set():
                exit_code = await self._service_pending_actions()
                if exit_code is not None:
                    return exit_code
                if self.sm.phase is Phase.CONNECTED:
                    await self._steady_state_loop()
                else:
                    # Brief yield so we don't busy-loop while waiting
                    # on an action whose completion arrives via callback.
                    await asyncio.sleep(0.05)
        finally:
            health_task.cancel()
            await self._cleanup()

        log.info("sidecar shutdown complete")
        return EXIT_OK

    # ── Action dispatch ──────────────────────────────────────────────

    async def _service_pending_actions(self) -> int | None:
        """Drain the StateMachine's pending_actions queue, performing
        each action's I/O. Returns a non-None exit code only on
        EXIT_FATAL (auth rejection)."""
        actions = self.sm.drain_actions()
        for action in actions:
            if action is Action.OPEN_TRANSPORT:
                await self._open_ws()
            elif action is Action.SEND_LOGIN:
                await self._send_login()
            elif action is Action.DO_REST_SEED:
                await self._rest_seed()
            elif action is Action.SEND_SUBSCRIBES:
                await self._send_subscribes()
            elif action is Action.CLOSE_TRANSPORT:
                await self._close_ws()
            elif action is Action.SCHEDULE_BACKOFF:
                await self._sleep_backoff()
            elif action is Action.LOG_ESCALATION:
                self._log_escalation()
            elif action is Action.EXIT_FATAL:
                self._log_fatal()
                return EXIT_CONFIG_ERROR
        return None

    async def _open_ws(self) -> None:
        try:
            log.info("connecting to %s", WS_PRIVATE_PROD)
            self.ws = await websockets.connect(
                WS_PRIVATE_PROD,
                ping_interval=None,  # we use our own application-level "ping"
                ping_timeout=None,
                close_timeout=5,
                max_size=2**21,  # 2 MB; account snapshots can be large
            )
            self.sm.feed(Event.TRANSPORT_OPENED)
            self._stats["reconnects"] += 1
        except Exception as e:
            log.warning("ws connect failed: %s", e)
            self.sm.feed(Event.TRANSPORT_CLOSED)

    async def _send_login(self) -> None:
        try:
            frame, _ts = build_login_frame(
                api_key=self.api_key,
                api_secret=self.api_secret,
                passphrase=self.passphrase,
            )
            await self.ws.send(frame)
            # Wait for login response with timeout. The state machine
            # will move to SEEDING_REST on LOGIN_OK or FATAL on
            # LOGIN_REJECTED; transport drop in the meantime is
            # caught as TRANSPORT_CLOSED.
            try:
                resp = await asyncio.wait_for(self.ws.recv(), timeout=10.0)
            except asyncio.TimeoutError:
                log.warning("login response timeout; treating as drop")
                await self._close_ws()
                self.sm.feed(Event.TRANSPORT_CLOSED)
                return
            classified = classify_message(resp)
            if classified.kind == "login_success":
                log.info("login ok")
                self.sm.feed(Event.LOGIN_OK)
            elif classified.kind == "login_failure":
                self.sm.feed(Event.LOGIN_REJECTED, code=classified.code)
            else:
                log.warning("unexpected login response: kind=%s raw=%s",
                            classified.kind, classified.raw[:200])
                await self._close_ws()
                self.sm.feed(Event.TRANSPORT_CLOSED)
        except Exception as e:
            log.warning("send_login failed: %s", e)
            self.sm.feed(Event.TRANSPORT_CLOSED)

    async def _rest_seed(self) -> None:
        """Seed Redis with current account/positions/tpsl from REST
        BEFORE subscribing. Orders and orders-algo channels do NOT push
        initial state — without this seed the dashboard would show
        empty SL/TP cells until the first user-triggered order event.
        """
        # The REST helpers live in app.api.routes.allocator. Importing
        # them at sidecar startup pulls FastAPI machinery; defer the
        # import to the call site to keep startup time low.
        try:
            from ...api.routes.allocator import (
                _fetch_live_blofin,
                fetch_blofin_tpsl_orders,
            )
        except ImportError as e:
            log.warning("REST seed imports failed: %s", e)
            self.sm.feed(Event.REST_SEED_FAILED)
            return

        creds = dict(
            api_key=self.api_key,
            api_secret=self.api_secret,
            passphrase=self.passphrase,
        )
        try:
            live = await asyncio.to_thread(_fetch_live_blofin, **creds)
            tpsl = await asyncio.to_thread(fetch_blofin_tpsl_orders, **creds)
        except Exception as e:
            log.warning("REST seed failed: %s", e)
            self.sm.feed(Event.REST_SEED_FAILED)
            return

        # Convert REST shape → the same shape WS pushes will produce,
        # so downstream readers see a consistent schema regardless of
        # whether the value was seeded or pushed.
        account_view = {
            "ts": str(int(time.time() * 1000)),
            "total_equity_usd": _to_float(live.get("total_equity")),
            "available_usd": _to_float(live.get("available")),
            "used_margin_usd": _to_float(live.get("used_margin")),
            "unrealized_pnl": _to_float(live.get("unrealized_pnl")),
            "details": [{
                "currency": "USDT",
                "available": str(live.get("available") or 0),
                "frozen": str(live.get("used_margin") or 0),
                "unrealized_pnl": str(live.get("unrealized_pnl") or 0),
            }],
            "_seed": True,
        }
        self.writer.write_atomic(account=account_view, positions=live.get("positions") or [])
        self.writer.write_orders_algo(list((tpsl or {}).values()))
        self._stats["redis_writes"] += 3
        log.info(
            "REST seed complete: positions=%d tpsl=%d",
            len(live.get("positions") or []),
            len(tpsl or {}),
        )
        self.sm.feed(Event.REST_SEED_DONE)

    async def _send_subscribes(self) -> None:
        try:
            for channel in SUBSCRIBE_CHANNELS:
                await self.ws.send(build_subscribe_frame(channel))
                # Read the ack inline — keeps the state machine in
                # SUBSCRIBING phase for 4 frames, then transitions to
                # CONNECTED on the 4th ack.
                try:
                    resp = await asyncio.wait_for(self.ws.recv(), timeout=5.0)
                except asyncio.TimeoutError:
                    log.warning("subscribe ack timeout for %s", channel)
                    self.sm.feed(Event.SUBSCRIBE_FAILED)
                    return
                classified = classify_message(resp)
                if classified.kind == "subscribe_success":
                    self.sm.feed(Event.SUBSCRIBE_ACK)
                else:
                    log.warning("subscribe %s failed: kind=%s msg=%s",
                                channel, classified.kind, classified.msg)
                    self.sm.feed(Event.SUBSCRIBE_FAILED)
                    return
            self._stats["connected_at"] = time.time()
            log.info("all channels subscribed; entering steady state")
        except Exception as e:
            log.warning("send_subscribes failed: %s", e)
            self.sm.feed(Event.TRANSPORT_CLOSED)

    async def _close_ws(self) -> None:
        if self.ws is None:
            return
        try:
            await self.ws.close()
        except Exception:
            pass
        self.ws = None

    async def _sleep_backoff(self) -> None:
        delay = compute_backoff(self.sm.consecutive_failures, rng=self._rng)
        log.info(
            "backoff %.2fs (consecutive_failures=%d)",
            delay, self.sm.consecutive_failures,
        )
        # Sleep in chunks so SIGTERM cuts through quickly.
        end = time.monotonic() + delay
        while time.monotonic() < end and not self.shutdown_event.is_set():
            await asyncio.sleep(min(0.5, end - time.monotonic()))
        if not self.shutdown_event.is_set():
            self.sm.feed(Event.BACKOFF_ELAPSED)

    # ── Steady-state recv + heartbeat ────────────────────────────────

    async def _steady_state_loop(self) -> None:
        """In CONNECTED phase, run the recv loop and the heartbeat
        loop concurrently. Either task ending (transport drop, server
        timeout) returns control to the outer loop, which feeds
        TRANSPORT_CLOSED and lets the state machine schedule reconnect.
        """
        try:
            recv_task = asyncio.create_task(self._recv_loop())
            ping_task = asyncio.create_task(self._heartbeat_loop())
            done, pending = await asyncio.wait(
                {recv_task, ping_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for t in pending:
                t.cancel()
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
        finally:
            if self.sm.phase is Phase.CONNECTED:
                # Either task finishing means the connection is no
                # longer usable; signal the state machine.
                self.sm.feed(Event.TRANSPORT_CLOSED)

    async def _recv_loop(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                msg = await asyncio.wait_for(
                    self.ws.recv(), timeout=PING_RECV_TIMEOUT_S * 3,
                )
            except asyncio.TimeoutError:
                log.warning("recv timeout — server silent for too long")
                return
            except websockets.exceptions.ConnectionClosed:
                log.info("ws closed by peer")
                return
            self._stats["messages_received"] += 1
            await self._dispatch_message(msg)

    async def _heartbeat_loop(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                await self.ws.send(PING_TEXT)
            except Exception as e:
                log.warning("ping send failed: %s", e)
                return
            # Cheap heartbeat: rotate the standalone heartbeat key
            # alongside every ping so a quiet hour (no pushes) doesn't
            # let the heartbeat TTL expire and falsely trip stale flags.
            self.writer.write_heartbeat()
            self._stats["redis_writes"] += 1
            await asyncio.sleep(PING_INTERVAL_S)

    async def _dispatch_message(self, raw: str) -> None:
        classified = classify_message(raw)
        if classified.kind == "pong":
            return
        if classified.kind == "push" and classified.parsed:
            payload = classified.parsed
            channel = classified.channel
            if channel == "account":
                acct = _on_account_push(payload)
                self.writer.write_atomic(account=acct)
                self._stats["redis_writes"] += 1
            elif channel == "positions":
                positions = _on_positions_push(payload)
                self.writer.write_atomic(positions=positions)
                self._stats["redis_writes"] += 1
            elif channel == "orders":
                orders = _on_orders_push(payload)
                self.writer.write_orders(orders)
                # Also fire the one-shot order_event hook so future
                # frontend toast logic has a key to watch.
                self.writer.write_order_event({
                    "channel": "orders",
                    "action": payload.get("action"),
                    "count": len(orders),
                    "ts": int(time.time() * 1000),
                })
                self._stats["redis_writes"] += 2
            elif channel == "orders-algo":
                algos = _on_orders_push(payload)
                self.writer.write_orders_algo(algos)
                self.writer.write_order_event({
                    "channel": "orders-algo",
                    "action": payload.get("action"),
                    "count": len(algos),
                    "ts": int(time.time() * 1000),
                })
                self._stats["redis_writes"] += 2
            else:
                log.debug("push on unsubscribed channel %s", channel)
        elif classified.kind in ("error", "subscribe_failure"):
            log.warning(
                "server error frame: code=%s msg=%s",
                classified.code, classified.msg,
            )
        elif classified.kind == "unknown":
            log.debug("unknown frame: %s", classified.raw[:200])

    # ── Diagnostics ──────────────────────────────────────────────────

    async def _health_log_loop(self) -> None:
        while not self.shutdown_event.is_set():
            await asyncio.sleep(60)
            connected_at = self._stats.get("connected_at")
            uptime = (
                f"{int(time.time() - connected_at)}s"
                if connected_at else "n/a"
            )
            log.info(
                "[SIDECAR_HEALTH_OK] phase=%s msgs=%d redis_writes=%d "
                "reconnects=%d uptime=%s",
                self.sm.phase.value,
                self._stats["messages_received"],
                self._stats["redis_writes"],
                self._stats["reconnects"],
                uptime,
            )

    def _log_escalation(self) -> None:
        n = self.sm.consecutive_failures
        severity = "ALERT" if n >= 10 else "WARN"
        log.error(
            "[SIDECAR_HEALTH_FAIL] severity=%s consecutive_failures=%d phase=%s",
            severity, n, self.sm.phase.value,
        )

    def _log_fatal(self) -> None:
        # The 3am-debugger log line called out by the user.
        log.error(
            "AUTH_REJECTED_FATAL: BloFin returned %s on login. "
            "Process exiting with code %d. Fix credentials and restart container.",
            self.sm.fatal_reason or "unknown error",
            EXIT_CONFIG_ERROR,
        )

    # ── Shutdown ─────────────────────────────────────────────────────

    def _request_shutdown(self) -> None:
        log.info("shutdown requested via signal")
        self.shutdown_event.set()

    async def _cleanup(self) -> None:
        await self._close_ws()
        self.writer.close()


# ── Module entrypoint ────────────────────────────────────────────────


async def amain() -> int:
    if not settings.LIVE_SIDECAR_ENABLED:
        log.info(
            "blofin sidecar disabled by config (LIVE_SIDECAR_ENABLED=false); "
            "exiting cleanly."
        )
        return EXIT_OK

    conn = await asyncio.to_thread(_load_active_blofin_connection)
    if not conn:
        log.error(
            "no active BloFin exchange_connection found — cannot start sidecar; "
            "exiting cleanly to let supervisor decide."
        )
        return EXIT_OK

    if not (conn["api_key"] and conn["api_secret"] and conn["passphrase"]):
        log.error(
            "AUTH_REJECTED_FATAL: BloFin connection_id=%s has empty decrypted "
            "credentials. Process exiting with code %d. Verify encryption keys.",
            conn["connection_id"], EXIT_CONFIG_ERROR,
        )
        return EXIT_CONFIG_ERROR

    sidecar = BlofinAccountSidecar(conn=conn)
    return await sidecar.run()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    try:
        return asyncio.run(amain())
    except KeyboardInterrupt:
        return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
