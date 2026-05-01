"""
backend/app/services/sidecars/blofin_ws_protocol.py
=====================================================
Pure-data BloFin WebSocket protocol layer:

  * Auth-frame construction (string-to-sign + base64(hex_digest) signature)
  * Subscribe-frame construction
  * Inbound message classification

Zero I/O — every function is deterministic on its inputs. The state
machine in `blofin_ws_state.py` calls these helpers; the I/O wrapper
in `blofin_account_sidecar.py` glues the state machine to a real
websockets connection.

This split is what makes the reconnect/auth state machine unit-testable
without spinning up a real WebSocket server.

BloFin auth divergences from OKX baseline (verified against the official
BloFin Python SDK at github.com/blofin/blofin-sdk-python):

  1. Signed message = path + method + timestamp + nonce + body
     where path="/users/self/verify", method="GET", body="", and
     nonce is the same string as timestamp. OKX has no nonce in the
     signed message and no nonce field in the args.

  2. Signature is base64(hex_digest_string), NOT base64(raw_bytes).
     The HMAC-SHA256 result is hex-stringified first, then those hex
     chars are base64-encoded. OKX uses raw bytes → base64 directly.
     Getting this wrong silently rejects every auth attempt.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from typing import Any, Literal


WS_PRIVATE_PROD = "wss://openapi.blofin.com/ws/private"
WS_PRIVATE_DEMO = "wss://demo-trading-openapi.blofin.com/ws/private"

# Heartbeat: we send a literal text frame containing the ASCII string
# "ping"; server replies with a literal "pong" text frame. NOT the
# WebSocket protocol-level Ping/Pong opcodes — application-level only.
PING_TEXT = "ping"
PONG_TEXT = "pong"
PING_INTERVAL_S = 15
PING_RECV_TIMEOUT_S = 10  # how long we wait for any frame before flagging silence


# ── Frame builders ────────────────────────────────────────────────────


def build_login_frame(
    *, api_key: str, api_secret: str, passphrase: str,
    timestamp_ms: int | None = None,
) -> tuple[str, str]:
    """Return (json_string, timestamp_ms_string).

    The returned timestamp is also captured by the caller so the state
    machine can correlate the eventual `event=login` response (BloFin
    does not echo the request id; the only correlation is "we last
    sent a login frame at time T").
    """
    ts = str(timestamp_ms if timestamp_ms is not None else int(time.time() * 1000))
    nonce = ts  # SDK-confirmed: nonce equals timestamp
    msg = f"/users/self/verify" f"GET" f"{ts}" f"{nonce}" f""
    hex_sig_bytes = hmac.new(
        api_secret.encode(), msg.encode(), hashlib.sha256
    ).hexdigest().encode()
    sign = base64.b64encode(hex_sig_bytes).decode()
    frame = json.dumps({
        "op": "login",
        "args": [{
            "apiKey": api_key,
            "passphrase": passphrase,
            "timestamp": ts,
            "sign": sign,
            "nonce": nonce,
        }],
    })
    return frame, ts


def build_subscribe_frame(channel: str, *, inst_id: str | None = None) -> str:
    """Subscribe frame for a single channel. Multi-channel subscribe
    frames are valid per BloFin (args is an array) but we send one
    channel at a time for cleaner ack tracking — the state machine
    advances on each ack and any single-channel failure stays isolated."""
    arg: dict[str, Any] = {"channel": channel}
    if inst_id is not None:
        arg["instId"] = inst_id
    return json.dumps({"op": "subscribe", "args": [arg]})


# ── Inbound message classification ────────────────────────────────────


MessageKind = Literal[
    "pong",                # plain text "pong"
    "login_success",       # {"event":"login","code":"0"}
    "login_failure",       # {"event":"login","code":!=0}
    "subscribe_success",   # {"event":"subscribe","arg":{"channel":...}}
    "subscribe_failure",   # {"event":"error","code":...}
    "push",                # has "arg" + "data" — channel data push
    "error",               # generic error frame
    "unknown",             # anything else; logged for diagnostics
]


@dataclass
class ClassifiedMessage:
    kind: MessageKind
    raw: str
    parsed: dict[str, Any] | None
    channel: str | None
    code: str | None
    msg: str | None


def classify_message(raw: str) -> ClassifiedMessage:
    """Categorize an inbound text frame. Never raises — invalid JSON
    produces kind='unknown' with parsed=None.

    Reasoning behind the categories:
      * 'pong' is the bare-text health response; the state machine
        treats it as 'frame received, connection is alive'.
      * login/subscribe events have a recognizable 'event' field.
      * Push frames have an 'arg' object plus 'data'; we identify by
        the presence of 'data' rather than the absence of 'event' so
        future server additions to push frames don't reclassify.
      * 'error' is a server-initiated error frame (auth-level rejection
        comes through as login_failure; this is for runtime errors).
    """
    if raw == PONG_TEXT:
        return ClassifiedMessage(
            kind="pong", raw=raw, parsed=None,
            channel=None, code=None, msg=None,
        )
    try:
        obj = json.loads(raw)
    except json.JSONDecodeError:
        return ClassifiedMessage(
            kind="unknown", raw=raw, parsed=None,
            channel=None, code=None, msg=None,
        )

    event = obj.get("event")
    code = obj.get("code")
    msg = obj.get("msg")
    arg = obj.get("arg") if isinstance(obj.get("arg"), dict) else None
    channel = arg.get("channel") if arg else None

    if event == "login":
        kind: MessageKind = (
            "login_success" if str(code) == "0" else "login_failure"
        )
        return ClassifiedMessage(
            kind=kind, raw=raw, parsed=obj,
            channel=None, code=str(code) if code is not None else None, msg=msg,
        )
    if event == "subscribe":
        return ClassifiedMessage(
            kind="subscribe_success", raw=raw, parsed=obj,
            channel=channel, code=str(code) if code is not None else None,
            msg=msg,
        )
    if event == "error":
        # Subscribe-time errors and runtime errors share this shape;
        # the state machine decides how to react based on its current
        # phase.
        return ClassifiedMessage(
            kind="subscribe_failure" if "subscribe" in (msg or "").lower() else "error",
            raw=raw, parsed=obj, channel=channel,
            code=str(code) if code is not None else None, msg=msg,
        )

    if "data" in obj:
        return ClassifiedMessage(
            kind="push", raw=raw, parsed=obj,
            channel=channel, code=None, msg=None,
        )

    return ClassifiedMessage(
        kind="unknown", raw=raw, parsed=obj,
        channel=channel, code=str(code) if code is not None else None, msg=msg,
    )
