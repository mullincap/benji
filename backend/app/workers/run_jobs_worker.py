"""
backend/app/workers/run_jobs_worker.py
=======================================
Generic celery task that runs an arbitrary registered pipeline script and
tracks the run in `market.run_jobs`. Triggered by POST /api/compiler/runs
or POST /api/indexer/runs (the new generic ones — separate from the
indexer-specific backfill_metric task in indexer_backfill_worker.py which
writes to market.indexer_jobs for legacy reasons).

Each entry in SCRIPT_REGISTRY maps a stable script_name (the value the
frontend POSTs) to:
  - module:    'compiler' | 'indexer' (used to filter UI views)
  - cmd_fn:    callable(params dict) → list[str] argv for subprocess
  - env_fn:    callable(env dict) → env dict (lets a script add env vars
               sourced from secrets.env at runtime, e.g. coingecko's API key)

Adding a new button = add an entry to SCRIPT_REGISTRY + a button on the
frontend. No new celery task per script.
"""

import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import psycopg2

from app.core.config import settings
from app.db import get_worker_conn
from app.workers.pipeline_worker import celery_app

_PIPELINE_DIR = Path(settings.PIPELINE_DIR)
_PIPELINE_PYTHON = settings.PIPELINE_PYTHON

_SECRETS_PATH = Path(settings.SECRETS_PATH)


def _load_secrets() -> dict:
    """Read /mnt/quant-data/credentials/secrets.env into a dict. Plain
    KEY=VALUE lines only — no shell interpolation. Returns {} if the file
    doesn't exist (e.g. local dev)."""
    out: dict = {}
    if not _SECRETS_PATH.exists():
        return out
    for line in _SECRETS_PATH.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v.strip()
    return out


# ─── Per-script command builders ────────────────────────────────────────────

def _yesterday_iso() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()


def _cmd_metl(params: dict) -> list[str]:
    """metl.py --start <yesterday>. params unused for now; could accept
    --start / --end overrides in the future."""
    start = params.get("start") or _yesterday_iso()
    return [
        _PIPELINE_PYTHON,
        str(_PIPELINE_DIR / "compiler" / "metl.py"),
        "--start", start,
        "--triggered-by", "ui",
    ]


def _env_metl(env: dict) -> dict:
    # metl.py self-loads AMBER_API_KEY from secrets.env (verified earlier),
    # so no env injection needed. Just inherit.
    return env


def _cmd_coingecko(params: dict) -> list[str]:
    return [
        _PIPELINE_PYTHON,
        str(_PIPELINE_DIR / "compiler" / "coingecko_marketcap.py"),
        "--api-key", os.environ.get("COINGECKO_API_KEY", "MISSING"),
        "--mode", params.get("mode", "daily"),
        "--output-dir", "/mnt/quant-data/raw/coingecko",
    ]


def _env_coingecko(env: dict) -> dict:
    """coingecko_marketcap.py reads --api-key from argv (not env), but the
    celery container doesn't have COINGECKO_API_KEY in its env. Source it
    from secrets.env at task-execution time and inject into the subprocess
    env so the os.environ.get() above resolves correctly."""
    secrets = _load_secrets()
    if "COINGECKO_API_KEY" in secrets:
        env["COINGECKO_API_KEY"] = secrets["COINGECKO_API_KEY"]
    return env


def _cmd_backfill_futures_1m(params: dict) -> list[str]:
    return [
        _PIPELINE_PYTHON,
        str(_PIPELINE_DIR / "db" / "backfill_futures_1m.py"),
    ]


def _env_passthrough(env: dict) -> dict:
    return env


# ─── Script registry ────────────────────────────────────────────────────────

SCRIPT_REGISTRY: dict[str, dict] = {
    "metl": {
        "module": "compiler",
        "label":  "Amberdata ETL (metl.py)",
        "cmd_fn": _cmd_metl,
        "env_fn": _env_metl,
    },
    "coingecko_marketcap": {
        "module": "compiler",
        "label":  "CoinGecko Daily Marketcap",
        "cmd_fn": _cmd_coingecko,
        "env_fn": _env_coingecko,
    },
    "backfill_futures_1m": {
        "module": "compiler",
        "label":  "Backfill futures_1m from master parquet",
        "cmd_fn": _cmd_backfill_futures_1m,
        "env_fn": _env_passthrough,
    },
}


# ─── DB helpers ─────────────────────────────────────────────────────────────

def _job_create(script_name: str, module: str, params: dict, triggered_by: str) -> str:
    conn = get_worker_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO market.run_jobs
            (script_name, module, status, triggered_by, params, started_at, last_heartbeat)
        VALUES (%s, %s, 'running', %s, %s::jsonb, NOW(), NOW())
        RETURNING run_id
        """,
        (script_name, module, triggered_by, psycopg2.extras.Json(params)),
    )
    run_id = str(cur.fetchone()[0])
    conn.commit()
    cur.close()
    conn.close()
    return run_id


def _job_finalize(run_id: str, exit_code: int, stdout_tail: str, stderr_tail: str) -> None:
    status = "complete" if exit_code == 0 else "failed"
    error_msg = None if exit_code == 0 else f"exit={exit_code}"
    conn = get_worker_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE market.run_jobs
        SET status=%s,
            completed_at=NOW(),
            last_heartbeat=NOW(),
            exit_code=%s,
            error_msg=%s,
            stdout_tail=%s,
            stderr_tail=%s
        WHERE run_id=%s
        """,
        (status, exit_code, error_msg, stdout_tail, stderr_tail, run_id),
    )
    conn.commit()
    cur.close()
    conn.close()


# psycopg2 Json adapter import — kept here so the top-level imports are
# minimal
import psycopg2.extras  # noqa: E402


# ─── Celery task ────────────────────────────────────────────────────────────

@celery_app.task(bind=True, name="run_jobs_worker.run_script")
def run_script(self, script_name: str, params: dict | None = None, triggered_by: str = "ui") -> dict:
    """Run a registered script and track in market.run_jobs."""
    params = params or {}
    if script_name not in SCRIPT_REGISTRY:
        raise ValueError(f"unknown script: {script_name!r}")

    entry = SCRIPT_REGISTRY[script_name]
    run_id = _job_create(script_name, entry["module"], params, triggered_by)

    cmd = entry["cmd_fn"](params)
    env = entry["env_fn"](os.environ.copy())

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        stdout_tail = (proc.stdout or "")[-2000:]
        stderr_tail = (proc.stderr or "")[-2000:]
        _job_finalize(run_id, proc.returncode, stdout_tail, stderr_tail)
        return {
            "run_id":     run_id,
            "exit_code":  proc.returncode,
            "status":     "complete" if proc.returncode == 0 else "failed",
        }
    except Exception as exc:
        _job_finalize(run_id, -1, "", f"task crashed: {exc}")
        raise
