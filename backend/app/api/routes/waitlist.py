"""
backend/app/api/routes/waitlist.py
===================================
Public POST /api/waitlist — records early-access signups to a JSONL file.

Entries include timestamp, email, client IP, user-agent. No auth, no rate
limit yet (low-volume public form). File lives on the `backend_data` volume
so it persists across container recreates.
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from ...core.config import settings

router = APIRouter(prefix="/api/waitlist", tags=["waitlist"])

_WAITLIST_PATH = Path(settings.ADMIN_SESSIONS_FILE).parent / "waitlist.jsonl"
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


class WaitlistSubmission(BaseModel):
    email: str = Field(..., min_length=3, max_length=254)


@router.post("")
def submit_waitlist(body: WaitlistSubmission, request: Request) -> dict:
    email = body.email.strip().lower()
    if not _EMAIL_RE.match(email):
        raise HTTPException(status_code=400, detail="invalid email")

    _WAITLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "ts": time.time(),
        "email": email,
        "ip": request.client.host if request.client else None,
        "user_agent": request.headers.get("user-agent"),
    }
    with _WAITLIST_PATH.open("a") as f:
        f.write(json.dumps(entry) + "\n")

    position = sum(1 for _ in _WAITLIST_PATH.open())
    return {"ok": True, "position": position}
