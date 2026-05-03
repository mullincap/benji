"""
backend/app/db.py
=================
Thin TimescaleDB connection layer for FastAPI routers (compiler, indexer, etc).

Local dev workflow:
  1. SSH tunnel: ssh -L 5432:127.0.0.1:5432 mcap -N
  2. Set DB_PASSWORD in .env at the project root
  3. uvicorn app.main:app --reload --port 8000

Production: the backend runs on the same host as TimescaleDB and reaches
127.0.0.1:5432 directly with no tunnel needed.

The pipeline scripts use pipeline/db/connection.py which loads from
/mnt/quant-data/credentials/secrets.env. This module deliberately does NOT
do that — backend connection settings come from the FastAPI Settings object
(which itself reads .env via pydantic-settings) so the backend has a single
configuration source.
"""

import os
from typing import Generator

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import HTTPException

from .core.config import settings


def _connect(**overrides):
    """Low-level psycopg2 connect using settings + any overrides."""
    return psycopg2.connect(
        host=overrides.get("host", os.environ.get("DB_HOST", settings.DB_HOST)),
        port=int(overrides.get("port", os.environ.get("DB_PORT", settings.DB_PORT))),
        dbname=overrides.get("dbname", os.environ.get("DB_NAME", settings.DB_NAME)),
        user=overrides.get("user", os.environ.get("DB_USER", settings.DB_USER)),
        password=overrides.get("password", os.environ.get("DB_PASSWORD", settings.DB_PASSWORD)),
        connect_timeout=overrides.get("connect_timeout", 5),
    )


def get_conn():
    """Open a new psycopg2 connection (FastAPI context). Caller is responsible for closing."""
    if not settings.DB_PASSWORD and not os.environ.get("DB_PASSWORD"):
        raise HTTPException(
            status_code=503,
            detail="DB_PASSWORD not set. Configure in backend .env (local dev requires "
                   "SSH tunnel: ssh -L 5432:127.0.0.1:5432 mcap -N)",
        )
    return _connect()


def get_worker_conn():
    """Open a psycopg2 connection for Celery workers (no HTTPException)."""
    return _connect()


def get_cursor() -> Generator:
    """
    FastAPI dependency yielding a RealDictCursor.

    Transaction lifecycle contract:
      Commits on:
        - clean return (success path)
        - HTTPException raise — 4xx responses can carry intentional state
          mutations (e.g. failed-login bookkeeping in auth.py: the counter
          increment must persist even though the response is 401/423).
          The mutation persists; the response status is the side-channel.
      Rolls back on:
        - psycopg2.Error (translated to a 500 response)
        - any other Python exception (unhandled bug → propagates up)

    Implication: a route that needs to record a mutation alongside a
    non-2xx response should just raise HTTPException after the mutation;
    no manual conn.commit() needed. A route that needs the OLD
    rollback-on-non-2xx behavior must validate up-front (raise BEFORE
    mutating). All existing routes in this codebase already validate
    up-front, so this contract change should be regression-safe — but
    if you add a route that mutates then raises 4xx and expected the
    mutation to be discarded, refactor it to validate first.
    """
    try:
        conn = get_conn()
    except psycopg2.OperationalError as e:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {e}")

    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        yield cur
        conn.commit()
    except HTTPException:
        # 4xx responses can carry intentional state mutations (e.g.
        # failed-login bookkeeping). Persist them, then re-raise.
        conn.commit()
        raise
    except psycopg2.Error as e:
        conn.rollback()
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
