"""tests/test_blofin_sidecar_state.py
=========================================
State-machine + protocol unit tests for the BloFin private WebSocket
sidecar. The state machine is pure-Python (no asyncio, no I/O), so these
tests run in milliseconds without spinning up a WebSocket server.

Six required cases per the build spec:
  1. Disconnect during connecting (TCP drop pre-auth)
  2. Disconnect during authing (login frame sent, no response)
  3. Disconnect during subscribing (auth ok, sub ack pending)
  4. Disconnect during steady-state (long-running drop)
  5. Authentication rejection → FATAL exit, no retry
  6. Three consecutive failed reconnects → escalation log fires

Plus the auth-success steady-state case the user asked for explicitly.

Bonus: a handful of protocol-layer tests that pin down the BloFin
auth signature divergence from OKX (base64(hex_digest), with a nonce
field). If anyone "fixes" that to OKX style by mistake, this test
fails before the sidecar ever connects.
"""

from __future__ import annotations

import json
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "backend"))

from app.services.sidecars.blofin_ws_protocol import (  # noqa: E402
    build_login_frame,
    build_subscribe_frame,
    classify_message,
)
from app.services.sidecars.blofin_ws_state import (  # noqa: E402
    Action,
    BASE_BACKOFF_S,
    Event,
    MAX_BACKOFF_S,
    Phase,
    StateMachine,
    SUBSCRIBE_CHANNELS,
    compute_backoff,
)


# ── Helpers ───────────────────────────────────────────────────────────


def _drive_to_authing(sm: StateMachine) -> None:
    """Walk the machine to the AUTHING phase: open transport + send
    login. Drains actions so per-test assertions start with a clean
    pending list."""
    sm.start()
    sm.drain_actions()
    sm.feed(Event.TRANSPORT_OPENED)
    sm.drain_actions()


def _drive_to_subscribing(sm: StateMachine) -> None:
    _drive_to_authing(sm)
    sm.feed(Event.LOGIN_OK)
    sm.drain_actions()
    sm.feed(Event.REST_SEED_DONE)
    sm.drain_actions()


def _drive_to_connected(sm: StateMachine) -> None:
    _drive_to_subscribing(sm)
    for _ in SUBSCRIBE_CHANNELS:
        sm.feed(Event.SUBSCRIBE_ACK)
    sm.drain_actions()


# ── State-machine tests (the 6 required + auth-success steady state) ──


def test_initial_start_opens_transport():
    sm = StateMachine()
    sm.start()
    assert sm.phase is Phase.CONNECTING
    assert sm.drain_actions() == [Action.OPEN_TRANSPORT]


def test_auth_success_steady_state():
    """The healthy path: connect → auth → seed → subscribe (4 acks) →
    CONNECTED. consecutive_failures stays at 0."""
    sm = StateMachine()
    sm.start()
    sm.feed(Event.TRANSPORT_OPENED)
    sm.feed(Event.LOGIN_OK)
    sm.feed(Event.REST_SEED_DONE)
    for _ in SUBSCRIBE_CHANNELS:
        sm.feed(Event.SUBSCRIBE_ACK)
    assert sm.phase is Phase.CONNECTED
    assert sm.consecutive_failures == 0
    actions = sm.drain_actions()
    assert Action.SEND_LOGIN in actions
    assert Action.DO_REST_SEED in actions
    assert Action.SEND_SUBSCRIBES in actions


def test_disconnect_during_connecting_schedules_backoff():
    """Case 1: TCP/TLS handshake drops before TRANSPORT_OPENED ever
    arrives. Machine retries via backoff."""
    sm = StateMachine()
    sm.start()
    sm.drain_actions()
    sm.feed(Event.TRANSPORT_CLOSED)
    assert sm.phase is Phase.DISCONNECTED
    assert sm.consecutive_failures == 1
    actions = sm.drain_actions()
    assert Action.CLOSE_TRANSPORT in actions
    assert Action.SCHEDULE_BACKOFF in actions
    # Backoff elapses → next OPEN_TRANSPORT
    sm.feed(Event.BACKOFF_ELAPSED)
    assert sm.phase is Phase.CONNECTING
    assert sm.drain_actions() == [Action.OPEN_TRANSPORT]


def test_disconnect_during_authing_schedules_backoff():
    """Case 2: TLS up, login frame sent, server drops before responding.
    Machine treats as transient — backoff and retry."""
    sm = StateMachine()
    _drive_to_authing(sm)
    assert sm.phase is Phase.AUTHING
    sm.feed(Event.TRANSPORT_CLOSED)
    assert sm.phase is Phase.DISCONNECTED
    assert sm.consecutive_failures == 1
    assert Action.SCHEDULE_BACKOFF in sm.drain_actions()


def test_disconnect_during_subscribing_schedules_backoff():
    """Case 3: auth succeeded, subscribe acks partially received,
    transport drops. Subscribe-ack progress resets so the next
    successful connection re-runs all 4 subscriptions."""
    sm = StateMachine()
    _drive_to_subscribing(sm)
    sm.feed(Event.SUBSCRIBE_ACK)  # 1 of 4 acks received
    assert sm.subscribe_acks_received == 1
    sm.feed(Event.TRANSPORT_CLOSED)
    assert sm.phase is Phase.DISCONNECTED
    assert sm.consecutive_failures == 1
    # Subscribe progress reset so the rebuilt connection re-acks all 4.
    assert sm.subscribe_acks_received == 0


def test_disconnect_during_steady_state_returns_to_disconnected():
    """Case 4: long-running connection drops mid-message-stream. Same
    transient-failure handling as the other phases."""
    sm = StateMachine()
    _drive_to_connected(sm)
    assert sm.phase is Phase.CONNECTED
    assert sm.consecutive_failures == 0
    sm.feed(Event.TRANSPORT_CLOSED)
    assert sm.phase is Phase.DISCONNECTED
    assert sm.consecutive_failures == 1


def test_login_rejected_is_fatal_no_retry():
    """Case 5: BloFin returns event=login with code != 0 (auth rejected).
    Machine moves to FATAL with EXIT_FATAL queued and the rejection
    code captured for the diagnostic log line."""
    sm = StateMachine()
    _drive_to_authing(sm)
    sm.feed(Event.LOGIN_REJECTED, code="60004")  # bad apiKey/secret
    assert sm.phase is Phase.FATAL
    assert sm.fatal_reason and "60004" in sm.fatal_reason
    actions = sm.drain_actions()
    assert Action.EXIT_FATAL in actions
    # No backoff, no transport-reopen — we exit and let the supervisor
    # decide to keep cycling.
    assert Action.SCHEDULE_BACKOFF not in actions


def test_three_consecutive_failures_emits_escalation_marker():
    """Case 6: consecutive_failures hits 3 → LOG_ESCALATION action
    queued for the driver to emit the [SIDECAR_HEALTH_FAIL] line.
    Same again at 10 (the more-severe threshold)."""
    sm = StateMachine()
    sm.start()
    sm.drain_actions()
    for i in range(1, 11):
        sm.feed(Event.TRANSPORT_CLOSED)
        actions = sm.drain_actions()
        if i in (3, 10):
            assert Action.LOG_ESCALATION in actions, (
                f"expected escalation at {i}, got {actions}"
            )
        else:
            assert Action.LOG_ESCALATION not in actions, (
                f"unexpected escalation at {i}: {actions}"
            )
        # Walk through backoff to set up the next failure trial.
        sm.feed(Event.BACKOFF_ELAPSED)
        sm.drain_actions()
    assert sm.consecutive_failures == 10


# ── Backoff-curve tests ───────────────────────────────────────────────


def test_compute_backoff_grows_exponentially_then_caps():
    rng = random.Random(0)
    # Force jitter to zero by patching the RNG to return 0.
    class ZeroRng:
        def uniform(self, a, b):
            return 0.0
    zero = ZeroRng()
    assert compute_backoff(0, rng=zero) == BASE_BACKOFF_S          # 1.0
    assert compute_backoff(1, rng=zero) == BASE_BACKOFF_S * 2      # 2.0
    assert compute_backoff(5, rng=zero) == BASE_BACKOFF_S * 32     # 32.0
    assert compute_backoff(20, rng=zero) == MAX_BACKOFF_S          # capped
    # With a real RNG, jitter stays within ±25% of base.
    for n in range(0, 8):
        b = compute_backoff(n, rng=rng)
        base = min(MAX_BACKOFF_S, BASE_BACKOFF_S * (2 ** n))
        assert 0.75 * base <= b <= 1.25 * base + 1e-9


# ── Protocol-layer tests (auth signature pinning) ─────────────────────


def test_build_login_frame_uses_base64_of_hex_digest():
    """Pin the BloFin auth divergence from OKX. If anyone "simplifies"
    the signature to base64(raw_bytes), this fails before the sidecar
    ever connects."""
    api_key = "test_key"
    api_secret = "test_secret"
    passphrase = "test_passphrase"
    frame_str, ts = build_login_frame(
        api_key=api_key, api_secret=api_secret, passphrase=passphrase,
        timestamp_ms=1727800000000,
    )
    frame = json.loads(frame_str)
    assert frame["op"] == "login"
    args = frame["args"][0]
    assert args["apiKey"] == api_key
    assert args["passphrase"] == passphrase
    assert args["timestamp"] == "1727800000000"
    assert args["nonce"] == "1727800000000"  # nonce == timestamp per SDK

    # Recompute the expected signature independently so we lock the
    # specific encoding (base64 of the hex digest, NOT base64 of raw
    # bytes). msg = path + method + timestamp + nonce + body.
    import base64, hashlib, hmac
    msg = "/users/self/verify" + "GET" + "1727800000000" + "1727800000000" + ""
    expected_hex = hmac.new(
        api_secret.encode(), msg.encode(), hashlib.sha256,
    ).hexdigest()
    expected_sign = base64.b64encode(expected_hex.encode()).decode()
    assert args["sign"] == expected_sign

    # Sanity: the OKX-style "raw bytes → base64" signature would be
    # different. Confirm we're NOT producing that one.
    raw_bytes = hmac.new(
        api_secret.encode(), msg.encode(), hashlib.sha256,
    ).digest()
    okx_style_sign = base64.b64encode(raw_bytes).decode()
    assert args["sign"] != okx_style_sign


def test_build_subscribe_frame_with_and_without_inst_id():
    f = json.loads(build_subscribe_frame("account"))
    assert f == {"op": "subscribe", "args": [{"channel": "account"}]}
    f = json.loads(build_subscribe_frame("positions", inst_id="BTC-USDT"))
    assert f == {
        "op": "subscribe",
        "args": [{"channel": "positions", "instId": "BTC-USDT"}],
    }


def test_classify_message_pong_is_alive_signal():
    c = classify_message("pong")
    assert c.kind == "pong"


def test_classify_message_login_success_and_failure():
    ok = classify_message(json.dumps({"event": "login", "code": "0"}))
    assert ok.kind == "login_success"
    bad = classify_message(json.dumps({
        "event": "login", "code": "60004", "msg": "Invalid sign",
    }))
    assert bad.kind == "login_failure"
    assert bad.code == "60004"


def test_classify_message_subscribe_ack():
    c = classify_message(json.dumps({
        "event": "subscribe", "arg": {"channel": "account"},
    }))
    assert c.kind == "subscribe_success"
    assert c.channel == "account"


def test_classify_message_push_with_data():
    c = classify_message(json.dumps({
        "arg": {"channel": "positions"},
        "data": [{"instId": "BTC-USDT", "positions": "0.05"}],
    }))
    assert c.kind == "push"
    assert c.channel == "positions"


def test_classify_message_invalid_json_is_unknown_not_raise():
    c = classify_message("not-json{")
    assert c.kind == "unknown"
    assert c.parsed is None
