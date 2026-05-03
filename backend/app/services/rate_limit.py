"""
backend/app/services/rate_limit.py
==================================
Minimal in-memory IP-based token bucket. Used by the auth endpoints to
slow brute-force attempts on /login and /invite/{token}/accept without
adding a dependency on a rate-limiting library.

Limits: 10 requests per 60 seconds per (route_key, IP). Process-local —
not shared across uvicorn workers. Acceptable for v1a single-worker
deployment; revisit when scaling out.

Usage:
    from app.services.rate_limit import RateLimit

    @router.post("/login", dependencies=[Depends(RateLimit("login"))])
    def login(...):
        ...
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from typing import Deque, Dict, Tuple

from fastapi import HTTPException, Request, status

WINDOW_SECONDS = 60.0
MAX_REQUESTS = 10

_buckets: Dict[Tuple[str, str], Deque[float]] = defaultdict(deque)


class RateLimit:
    """FastAPI dependency factory. Construct with a route_key string."""

    def __init__(self, route_key: str):
        self.route_key = route_key

    def __call__(self, request: Request) -> None:
        ip = request.client.host if request.client else "unknown"
        key = (self.route_key, ip)
        now = time.monotonic()
        bucket = _buckets[key]
        cutoff = now - WINDOW_SECONDS
        while bucket and bucket[0] < cutoff:
            bucket.popleft()
        if len(bucket) >= MAX_REQUESTS:
            retry_after = max(1, int(WINDOW_SECONDS - (now - bucket[0])))
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail={"error": "rate_limited", "retry_after": retry_after},
                headers={"Retry-After": str(retry_after)},
            )
        bucket.append(now)
