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


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
