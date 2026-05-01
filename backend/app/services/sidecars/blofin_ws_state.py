"""
backend/app/services/sidecars/blofin_ws_state.py
==================================================
Reconnect / auth state machine for the BloFin private WebSocket
sidecar. Pure Python — no asyncio, no I/O. The driver in
`blofin_account_sidecar.py` advances this machine by feeding it
events (transport-level connect/disconnect, parsed protocol messages,
timer ticks); the machine emits Actions the driver carries out.

This split makes the failure modes the user explicitly called out
unit-testable without spinning up a real WebSocket server:

  * Disconnect during connecting (TCP handshake fails)
  * Disconnect during authing (TLS up, login frame sent, no response)
  * Disconnect during subscribing (auth ok, sub ack pending)
  * Disconnect during steady-state (mid-message-stream drop)
  * Authentication rejection (login response code != 0) — fatal exit
  * 3 / 10 consecutive failure thresholds — escalation log markers
"""

from __future__ import annotations

import enum
import random
from dataclasses import dataclass, field
from typing import Literal


# ── States ────────────────────────────────────────────────────────────


class Phase(str, enum.Enum):
    """High-level phase of the WebSocket lifecycle.

    Linear forward progress on a healthy connect:
        Disconnected → Connecting → Authing → SeedingRest →
        Subscribing → Connected
    Any drop returns to Disconnected; the driver applies backoff and
    re-enters Connecting.
    """
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    AUTHING = "authing"
    SEEDING_REST = "seeding_rest"
    SUBSCRIBING = "subscribing"
    CONNECTED = "connected"
    FATAL = "fatal"  # auth rejected — process should exit non-zero


# ── Events the driver feeds in ────────────────────────────────────────


class Event(str, enum.Enum):
    TRANSPORT_OPENED = "transport_opened"
    TRANSPORT_CLOSED = "transport_closed"
    LOGIN_OK = "login_ok"
    LOGIN_REJECTED = "login_rejected"
    REST_SEED_DONE = "rest_seed_done"
    REST_SEED_FAILED = "rest_seed_failed"
    SUBSCRIBE_ACK = "subscribe_ack"
    SUBSCRIBE_FAILED = "subscribe_failed"
    BACKOFF_ELAPSED = "backoff_elapsed"


# ── Actions the machine asks the driver to perform ────────────────────


class Action(str, enum.Enum):
    OPEN_TRANSPORT = "open_transport"
    SEND_LOGIN = "send_login"
    DO_REST_SEED = "do_rest_seed"
    SEND_SUBSCRIBES = "send_subscribes"
    CLOSE_TRANSPORT = "close_transport"
    SCHEDULE_BACKOFF = "schedule_backoff"
    EXIT_FATAL = "exit_fatal"
    LOG_ESCALATION = "log_escalation"  # emitted at 3 and 10 consecutive failures


# Channels we subscribe to on every (re)connection. Order is irrelevant
# for correctness — each subscribe is independently ack'd — but we list
# them in the priority sequence the dashboard depends on most heavily.
SUBSCRIBE_CHANNELS = ("account", "positions", "orders", "orders-algo")


# ── Backoff parameters ────────────────────────────────────────────────


BASE_BACKOFF_S = 1.0
MAX_BACKOFF_S = 60.0
JITTER_FRACTION = 0.25  # ±25% on the exponential value
ESCALATION_THRESHOLDS = (3, 10)  # log [SIDECAR_HEALTH_FAIL] at these counts


def compute_backoff(consecutive_failures: int, *, rng: random.Random | None = None) -> float:
    """Exponential backoff with jitter, capped at MAX_BACKOFF_S.
    Uses an injectable RNG so tests can pin jitter to zero."""
    r = rng if rng is not None else random
    base = min(MAX_BACKOFF_S, BASE_BACKOFF_S * (2 ** max(0, consecutive_failures)))
    jitter = base * JITTER_FRACTION
    return max(0.0, base + r.uniform(-jitter, jitter))


# ── Machine ───────────────────────────────────────────────────────────


@dataclass
class StateMachine:
    """Drives transitions on Event input; exposes `pending_actions` the
    driver should perform. Tests check (phase, action_sequence,
    consecutive_failures) after a sequence of events.

    The machine is intentionally allocator-free in steady state — every
    `feed()` call mutates in place and appends to `pending_actions`; the
    driver drains the queue.
    """

    phase: Phase = Phase.DISCONNECTED
    consecutive_failures: int = 0
    subscribe_acks_received: int = 0
    pending_actions: list[Action] = field(default_factory=list)
    fatal_reason: str | None = None

    # Test-only knobs.
    _required_subscribe_count: int = len(SUBSCRIBE_CHANNELS)

    # ── Public driver API ─────────────────────────────────────────────

    def start(self) -> None:
        """Initial action — open the transport."""
        if self.phase is Phase.DISCONNECTED:
            self._goto(Phase.CONNECTING)
            self.pending_actions.append(Action.OPEN_TRANSPORT)

    def feed(self, event: Event, *, code: str | None = None) -> None:
        """Advance the machine by one event. `code` carries the BloFin
        rejection code on LOGIN_REJECTED, used in the fatal log line."""
        handler = self._handlers.get((self.phase, event))
        if handler is None:
            # Stray events (e.g. SUBSCRIBE_ACK after we've already moved
            # to CONNECTED, or TRANSPORT_CLOSED while already closed)
            # are tolerated as no-ops — the driver should not crash on
            # unexpected ordering. Counted against `consecutive_failures`
            # only when the event signals an actual fault.
            return
        handler(self, code=code)

    def drain_actions(self) -> list[Action]:
        out = list(self.pending_actions)
        self.pending_actions.clear()
        return out

    # ── Transition helpers ────────────────────────────────────────────

    def _goto(self, phase: Phase) -> None:
        self.phase = phase

    def _on_failure(self) -> None:
        """Common path for any transient failure — increment, escalate
        at thresholds, schedule backoff."""
        self.consecutive_failures += 1
        if self.consecutive_failures in ESCALATION_THRESHOLDS:
            self.pending_actions.append(Action.LOG_ESCALATION)
        self.subscribe_acks_received = 0
        self._goto(Phase.DISCONNECTED)
        self.pending_actions.append(Action.CLOSE_TRANSPORT)
        self.pending_actions.append(Action.SCHEDULE_BACKOFF)

    def _on_success_reset(self) -> None:
        self.consecutive_failures = 0
        self.subscribe_acks_received = 0

    # ── Per-(phase, event) handlers ───────────────────────────────────

    def _h_connecting_opened(self, *, code=None) -> None:
        self._goto(Phase.AUTHING)
        self.pending_actions.append(Action.SEND_LOGIN)

    def _h_connecting_closed(self, *, code=None) -> None:
        # Drop during TCP/TLS handshake. Backoff and retry.
        self._on_failure()

    def _h_authing_login_ok(self, *, code=None) -> None:
        # Auth succeeded; before subscribing, REST-seed account/positions/
        # orders/tpsl so the dashboard has a complete picture even before
        # the first WS event fires for orders/orders-algo.
        self._goto(Phase.SEEDING_REST)
        self.pending_actions.append(Action.DO_REST_SEED)

    def _h_authing_login_rejected(self, *, code=None) -> None:
        # Bad credentials — supervisor restart is unlikely to help. Exit
        # with a diagnostic code; container restart policy will cycle the
        # process indefinitely until creds are fixed, which is the
        # desired diagnostic loop.
        self._goto(Phase.FATAL)
        self.fatal_reason = f"login rejected, code={code}"
        self.pending_actions.append(Action.EXIT_FATAL)

    def _h_authing_closed(self, *, code=None) -> None:
        # Connection dropped after we sent login but before response.
        self._on_failure()

    def _h_seeding_done(self, *, code=None) -> None:
        self._goto(Phase.SUBSCRIBING)
        self.pending_actions.append(Action.SEND_SUBSCRIBES)

    def _h_seeding_failed(self, *, code=None) -> None:
        # REST seed failure is treated as a transient connection
        # failure: close, backoff, full retry. Not fatal.
        self._on_failure()

    def _h_seeding_closed(self, *, code=None) -> None:
        self._on_failure()

    def _h_subscribing_ack(self, *, code=None) -> None:
        self.subscribe_acks_received += 1
        if self.subscribe_acks_received >= self._required_subscribe_count:
            self._goto(Phase.CONNECTED)
            self._on_success_reset()

    def _h_subscribing_failed(self, *, code=None) -> None:
        self._on_failure()

    def _h_subscribing_closed(self, *, code=None) -> None:
        self._on_failure()

    def _h_connected_closed(self, *, code=None) -> None:
        self._on_failure()

    def _h_disconnected_backoff_elapsed(self, *, code=None) -> None:
        self._goto(Phase.CONNECTING)
        self.pending_actions.append(Action.OPEN_TRANSPORT)

    # Dispatch table — keys are (phase, event), value is the bound method.
    _handlers = {
        (Phase.CONNECTING,    Event.TRANSPORT_OPENED):    _h_connecting_opened,
        (Phase.CONNECTING,    Event.TRANSPORT_CLOSED):    _h_connecting_closed,
        (Phase.AUTHING,       Event.LOGIN_OK):            _h_authing_login_ok,
        (Phase.AUTHING,       Event.LOGIN_REJECTED):      _h_authing_login_rejected,
        (Phase.AUTHING,       Event.TRANSPORT_CLOSED):    _h_authing_closed,
        (Phase.SEEDING_REST,  Event.REST_SEED_DONE):      _h_seeding_done,
        (Phase.SEEDING_REST,  Event.REST_SEED_FAILED):    _h_seeding_failed,
        (Phase.SEEDING_REST,  Event.TRANSPORT_CLOSED):    _h_seeding_closed,
        (Phase.SUBSCRIBING,   Event.SUBSCRIBE_ACK):       _h_subscribing_ack,
        (Phase.SUBSCRIBING,   Event.SUBSCRIBE_FAILED):    _h_subscribing_failed,
        (Phase.SUBSCRIBING,   Event.TRANSPORT_CLOSED):    _h_subscribing_closed,
        (Phase.CONNECTED,     Event.TRANSPORT_CLOSED):    _h_connected_closed,
        (Phase.DISCONNECTED,  Event.BACKOFF_ELAPSED):     _h_disconnected_backoff_elapsed,
    }
