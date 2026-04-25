import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.routes.jobs import router as jobs_router
from app.api.routes.compiler import router as compiler_router
from app.api.routes.indexer import router as indexer_router
from app.api.routes.admin import router as admin_router
from app.api.routes.auth import router as auth_router
from app.api.routes.allocator import router as allocator_router
from app.api.routes.manager import router as manager_router
from app.api.routes.simulator import router as simulator_router
from app.api.routes.waitlist import router as waitlist_router
from app.core.config import settings

app = FastAPI(title="Benji3m Audit API", version="0.1.0")


@app.exception_handler(RequestValidationError)
async def validation_error_handler(_request: Request, exc: RequestValidationError):
    """Strip the `input` field from every validation error entry.

    FastAPI's default 422 response echoes the full submitted payload back under
    `detail[i].input`. Endpoints that accept secrets (exchanges/keys, auth login,
    etc.) must never expose submitted credentials — this handler is a blanket
    defense regardless of the specific endpoint or client error handling.
    """
    clean_errors = [
        {k: v for k, v in err.items() if k != "input"}
        for err in exc.errors()
    ]
    return JSONResponse(status_code=422, content={"detail": clean_errors})

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("CORS_ORIGINS", "http://localhost:3000").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(jobs_router)
app.include_router(admin_router)
app.include_router(auth_router)
app.include_router(compiler_router)
app.include_router(indexer_router)
app.include_router(allocator_router)
app.include_router(manager_router)
app.include_router(simulator_router)
app.include_router(waitlist_router)


@app.on_event("startup")
def ensure_session_files():
    # Admin sessions still use flat-file JSON. User sessions are DB-backed.
    path = Path(settings.ADMIN_SESSIONS_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text('{"tokens": {}}')


@app.on_event("startup")
def resume_orphaned_traders():
    """On backend container boot, respawn any trader subprocess that was
    killed by the container restart mid-session.

    Scans user_mgmt.allocations for phase=active sessions whose
    runtime_state.updated_at predates this container's start time, and
    issues a resume-only respawn via the trader supervisor. Backoff +
    corrupted-state guardrails apply (see trader_supervisor.py).

    Wrapped so any failure here never blocks the backend from serving
    requests — the periodic supervisor cron picks up any sessions the
    startup hook missed within ~20 min.
    """
    try:
        from app.cli.trader_supervisor import startup_recovery
        startup_recovery()
    except Exception:
        import logging
        logging.getLogger("main").exception(
            "startup trader-recovery failed (non-blocking)"
        )


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/health/trader")
def health_trader():
    """Trader-aware health probe for external monitoring (UptimeRobot etc.).

    Returns 503 if any DB-active allocation has a runtime_state in 'active'
    phase but its updated_at timestamp is older than STALE_MINUTES. This
    catches dead trader subprocesses and stuck-lock recoveries that the
    plain /health endpoint can't surface (FastAPI is up = /health 200, but
    a dead subprocess only shows in the UI).

    Threshold is supervisor's STALE_THRESHOLD_MIN (15) + buffer for the
    next supervisor tick to actually respawn before this probe alerts.
    Allocations whose phase is anything other than 'active' (closed,
    exited_*, etc.) are intentionally idle and not counted as stale.
    """
    from datetime import datetime, timezone
    from app.db import get_worker_conn

    STALE_MINUTES = 20

    try:
        with get_worker_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT allocation_id,
                       runtime_state->>'phase' AS phase,
                       runtime_state->>'updated_at' AS upd
                FROM user_mgmt.allocations
                WHERE status = 'active'
                """
            )
            rows = cur.fetchall()
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"status": "error", "error": f"db query failed: {e!r}"},
        )

    now = datetime.now(timezone.utc)
    stale: list[dict] = []
    healthy: list[dict] = []
    idle: list[dict] = []

    for aid, phase, upd_str in rows:
        short = str(aid)[:8]
        if phase != "active":
            idle.append({"allocation_id": short, "phase": phase})
            continue
        if not upd_str:
            stale.append({"allocation_id": short, "reason": "no updated_at"})
            continue
        try:
            upd = datetime.fromisoformat(upd_str.replace(" ", "T"))
            if upd.tzinfo is None:
                upd = upd.replace(tzinfo=timezone.utc)
            age_min = (now - upd).total_seconds() / 60
            entry = {"allocation_id": short, "age_min": round(age_min, 1)}
            if age_min > STALE_MINUTES:
                stale.append(entry)
            else:
                healthy.append(entry)
        except Exception as e:
            stale.append({"allocation_id": short, "reason": f"unparseable upd: {e!r}"})

    body = {"healthy": healthy, "idle": idle, "stale": stale}
    if stale:
        return JSONResponse(status_code=503, content={"status": "stale", **body})
    return {"status": "ok", **body}
