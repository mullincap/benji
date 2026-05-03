"""
backend/app/cli/dedupe_exchange_connections.py
==============================================
One-shot backfill + dedupe for migration 026 (per-user duplicate
exchange-connection prevention).

Run once after applying 026_exchange_connection_dedup.sql:

    docker compose exec -T backend python -m app.cli.dedupe_exchange_connections

What it does:
    1. SELECT every exchange_connections row with api_key_hash IS NULL
       (i.e. pre-migration data).
    2. For each, decrypt api_key_enc via the application's Fernet key,
       sha256-hash the plaintext, UPDATE the row to set api_key_hash.
    3. Within each (user_id, exchange) cluster of *active* rows that
       end up sharing the same hash, mark all but the most-recently-
       created one as 'revoked'. The kept row is the freshest, which
       most users would expect ("my latest link wins").
    4. Print a summary so the operator can verify the outcome.

Idempotent — re-running after the first pass is a no-op (no NULL
hashes left, no active duplicates left). Safe to run dry-first via
--dry-run.

Cross-user key reuse stays legal: the dedupe is partitioned by
user_id. Two different users with the same Binance key are not
touched.
"""

from __future__ import annotations

import argparse
import hashlib
import sys

from psycopg2.extras import RealDictCursor

from ..db import get_conn
from ..services.encryption import decrypt_key


def _hash_plaintext(plaintext: str) -> str:
    return hashlib.sha256(plaintext.encode()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="dedupe_exchange_connections",
        description="One-shot backfill + dedupe for migration 026.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute changes but don't write. Prints what would be updated.",
    )
    args = parser.parse_args()

    conn = get_conn()
    try:
        # RealDictCursor → row["column_name"] access. The default psycopg2
        # cursor returns tuples, which would force positional indexing
        # throughout the dedupe logic. get_conn() doesn't pre-configure
        # the factory; we set it on the cursor here.
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Stage 1 — backfill api_key_hash for any row that doesn't
            # have one yet. This includes both single-key rows AND
            # pre-existing duplicates; the dedupe pass below filters
            # those further.
            cur.execute(
                """
                SELECT connection_id, user_id, exchange, api_key_enc, status, created_at
                  FROM user_mgmt.exchange_connections
                 WHERE api_key_hash IS NULL
                 ORDER BY user_id, exchange, created_at
                """
            )
            rows = cur.fetchall()
            print(f"Stage 1: {len(rows)} rows missing api_key_hash")

            backfills: list[tuple[str, str]] = []
            for row in rows:
                connection_id = str(row["connection_id"])
                ciphertext = row["api_key_enc"]
                plaintext = decrypt_key(ciphertext)
                if plaintext is None:
                    print(
                        f"  WARN connection_id={connection_id} api_key_enc decrypt failed; "
                        "row left with NULL hash (Fernet key rotation? legacy unencrypted?)",
                        file=sys.stderr,
                    )
                    continue
                h = _hash_plaintext(plaintext)
                backfills.append((h, connection_id))

            if args.dry_run:
                print(f"  DRY RUN — would UPDATE {len(backfills)} rows with api_key_hash")
            else:
                for h, connection_id in backfills:
                    cur.execute(
                        "UPDATE user_mgmt.exchange_connections SET api_key_hash = %s WHERE connection_id = %s::uuid",
                        (h, connection_id),
                    )
                conn.commit()
                print(f"  UPDATE: backfilled {len(backfills)} rows")

            # Stage 2 — find and revoke duplicates among ACTIVE rows.
            # Partition by (user_id, exchange, api_key_hash); keep the
            # row with the latest created_at; revoke all others.
            cur.execute(
                """
                SELECT connection_id, user_id, exchange, api_key_hash, created_at,
                       ROW_NUMBER() OVER (
                           PARTITION BY user_id, exchange, api_key_hash
                           ORDER BY created_at DESC
                       ) AS rn
                  FROM user_mgmt.exchange_connections
                 WHERE api_key_hash IS NOT NULL
                   AND status != 'revoked'
                """
            )
            ranked = cur.fetchall()
            duplicates = [r for r in ranked if r["rn"] > 1]
            print(f"Stage 2: {len(duplicates)} duplicate active rows to revoke")

            if args.dry_run:
                for r in duplicates:
                    print(
                        f"  DRY RUN — would REVOKE connection_id={str(r['connection_id'])[:8]}… "
                        f"user_id={str(r['user_id'])[:8]}… exchange={r['exchange']} "
                        f"created_at={r['created_at']} (rn={r['rn']})"
                    )
            else:
                for r in duplicates:
                    cur.execute(
                        """
                        UPDATE user_mgmt.exchange_connections
                           SET status = 'revoked',
                               last_error_msg = 'Auto-revoked by migration 026 dedupe (duplicate of newer row)',
                               updated_at = NOW()
                         WHERE connection_id = %s::uuid
                        """,
                        (str(r["connection_id"]),),
                    )
                conn.commit()
                print(f"  UPDATE: revoked {len(duplicates)} duplicate rows")

        print("Done.")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
