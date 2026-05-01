"""
backend/app/services/sidecars/redis_writer.py
==============================================
Redis writes for the BloFin sidecar. All hot-path writes go through a
Lua script (EVAL) so account state, positions list, and heartbeat
update atomically — readers never observe a half-state where the
position list reflects the new push but the snapshot hasn't been
written yet.

Key shapes (must match what /api/manager/live/* readers expect):

    account:blofin:{cid}:snapshot      JSON object  TTL 60s
    positions:blofin:{cid}:list        JSON array   TTL 60s
    orders:blofin:{cid}:open           JSON array   TTL 60s
    orders-algo:blofin:{cid}:open      JSON array   TTL 60s
    sidecar:blofin:{cid}:heartbeat     unix epoch s TTL 30s
    order_event:blofin:{cid}           JSON object  TTL 10s (one-shot)

A Redis outage is logged but never crashes the sidecar — the next
push after recovery resyncs the state automatically because each
push carries the full state for that key, not a delta.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import redis as redis_lib

from ...core.config import settings


log = logging.getLogger("blofin_sidecar.redis")


# ── TTLs (per spec) ──────────────────────────────────────────────────


SNAPSHOT_TTL_S = 60
POSITIONS_TTL_S = 60
ORDERS_TTL_S = 60
HEARTBEAT_TTL_S = 30
ORDER_EVENT_TTL_S = 10


# ── Lua script — atomic update of account+positions+heartbeat ─────────
#
# KEYS[1] = account snapshot key
# KEYS[2] = positions list key
# KEYS[3] = heartbeat key
# ARGV[1] = account JSON (string; "" = skip update)
# ARGV[2] = positions JSON (string; "" = skip update)
# ARGV[3] = heartbeat unix epoch seconds (string)
# ARGV[4] = snapshot TTL seconds
# ARGV[5] = positions TTL seconds
# ARGV[6] = heartbeat TTL seconds
#
# Returns: array of 1/0 indicating which keys were written.
ATOMIC_WRITE_SCRIPT = """
local results = {0, 0, 0}
if ARGV[1] ~= "" then
  redis.call('SETEX', KEYS[1], ARGV[4], ARGV[1])
  results[1] = 1
end
if ARGV[2] ~= "" then
  redis.call('SETEX', KEYS[2], ARGV[5], ARGV[2])
  results[2] = 1
end
redis.call('SETEX', KEYS[3], ARGV[6], ARGV[3])
results[3] = 1
return results
"""


# ── Key helpers ──────────────────────────────────────────────────────


def account_key(connection_id: str) -> str:
    return f"account:blofin:{connection_id}:snapshot"


def positions_key(connection_id: str) -> str:
    return f"positions:blofin:{connection_id}:list"


def orders_key(connection_id: str) -> str:
    return f"orders:blofin:{connection_id}:open"


def orders_algo_key(connection_id: str) -> str:
    return f"orders-algo:blofin:{connection_id}:open"


def heartbeat_key(connection_id: str) -> str:
    return f"sidecar:blofin:{connection_id}:heartbeat"


def order_event_key(connection_id: str) -> str:
    return f"order_event:blofin:{connection_id}"


# ── Writer ────────────────────────────────────────────────────────────


class RedisWriter:
    """Thin wrapper around a redis client + Lua script handle.

    All write methods catch RedisError and log; never raise — a Redis
    blip should not bring down the sidecar. The next push will retry
    naturally because BloFin pushes full state, not deltas.
    """

    def __init__(self, *, connection_id: str, redis_url: str | None = None):
        self.connection_id = connection_id
        self.redis = redis_lib.Redis.from_url(
            redis_url or settings.REDIS_URL, decode_responses=True,
        )
        self._atomic_script = self.redis.register_script(ATOMIC_WRITE_SCRIPT)

    # ── Write paths ──────────────────────────────────────────────────

    def write_atomic(
        self,
        *,
        account: dict[str, Any] | None = None,
        positions: list[dict[str, Any]] | None = None,
    ) -> None:
        """Atomic write of (account, positions, heartbeat) in one EVAL.
        Either field can be None to skip its update; heartbeat always
        rotates so any successful write proves liveness.

        On Redis error: log + return. The caller (sidecar driver) is
        already retry-safe because the next push carries full state.
        """
        try:
            self._atomic_script(
                keys=[
                    account_key(self.connection_id),
                    positions_key(self.connection_id),
                    heartbeat_key(self.connection_id),
                ],
                args=[
                    json.dumps(account) if account is not None else "",
                    json.dumps(positions) if positions is not None else "",
                    str(int(time.time())),
                    str(SNAPSHOT_TTL_S),
                    str(POSITIONS_TTL_S),
                    str(HEARTBEAT_TTL_S),
                ],
            )
        except redis_lib.RedisError as e:
            log.warning("redis_writer: atomic write failed: %s", e)

    def write_orders(self, orders: list[dict[str, Any]]) -> None:
        """Open SL/TP-pending orders. Independent key — orders update at
        a different cadence than account/positions, and they don't
        require atomicity with the account snapshot."""
        try:
            self.redis.setex(
                orders_key(self.connection_id),
                ORDERS_TTL_S,
                json.dumps(orders),
            )
        except redis_lib.RedisError as e:
            log.warning("redis_writer: orders write failed: %s", e)

    def write_orders_algo(self, algos: list[dict[str, Any]]) -> None:
        """Algo (TP/SL) orders — separate channel, separate key."""
        try:
            self.redis.setex(
                orders_algo_key(self.connection_id),
                ORDERS_TTL_S,
                json.dumps(algos),
            )
        except redis_lib.RedisError as e:
            log.warning("redis_writer: orders-algo write failed: %s", e)

    def write_heartbeat(self) -> None:
        """Standalone heartbeat — used by the heartbeat task between
        pushes so a quiet hour (no account/position changes) doesn't
        let the heartbeat key TTL out."""
        try:
            self.redis.setex(
                heartbeat_key(self.connection_id),
                HEARTBEAT_TTL_S,
                str(int(time.time())),
            )
        except redis_lib.RedisError as e:
            log.warning("redis_writer: heartbeat write failed: %s", e)

    def write_order_event(self, event: dict[str, Any]) -> None:
        """One-shot key for "an order just changed" notifications. v1
        frontend doesn't render these — they're hooks for a later toast
        feature. TTL 10s so a missed read doesn't pile up."""
        try:
            self.redis.setex(
                order_event_key(self.connection_id),
                ORDER_EVENT_TTL_S,
                json.dumps(event),
            )
        except redis_lib.RedisError as e:
            log.warning("redis_writer: order_event write failed: %s", e)

    # ── Cleanup ──────────────────────────────────────────────────────

    def close(self) -> None:
        try:
            self.redis.close()
        except Exception:
            pass
