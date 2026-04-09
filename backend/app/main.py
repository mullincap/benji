import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes.jobs import router as jobs_router
from app.api.routes.compiler import router as compiler_router
from app.api.routes.indexer import router as indexer_router
from app.api.routes.admin import router as admin_router
from app.api.routes.allocator import router as allocator_router
from app.api.routes.manager import router as manager_router
from app.core.config import settings

app = FastAPI(title="Benji3m Audit API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("CORS_ORIGINS", "http://localhost:3000").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(jobs_router)
app.include_router(admin_router)
app.include_router(compiler_router)
app.include_router(indexer_router)
app.include_router(allocator_router)
app.include_router(manager_router)


@app.on_event("startup")
def ensure_admin_sessions_file():
    path = Path(settings.ADMIN_SESSIONS_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text('{"tokens": {}}')


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
