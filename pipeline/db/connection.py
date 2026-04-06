"""
pipeline/db/connection.py
=========================
Shared database connection helper for all pipeline scripts.
Reads credentials from env vars or /mnt/quant-data/credentials/secrets.env.
"""

import os
import psycopg2
from pathlib import Path


def _load_secrets():
    path = Path("/mnt/quant-data/credentials/secrets.env")
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


def get_conn():
    _load_secrets()
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "127.0.0.1"),
        port=int(os.environ.get("DB_PORT", 5432)),
        dbname=os.environ.get("DB_NAME", "marketdata"),
        user=os.environ.get("DB_USER", "quant"),
        password=os.environ["DB_PASSWORD"],
    )
