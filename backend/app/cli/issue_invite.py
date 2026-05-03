"""
backend/app/cli/issue_invite.py
================================
Admin-only invite issuer for Phase 1a. There is no UI for issuing
invitations in v1 — J runs this script manually and sends the resulting
URL to the invitee out-of-band (email, signal, etc).

Token convention:
- 32 random bytes, encoded as URL-safe base64 (~43 chars).
- The full token only ever exists in the printed URL; the database
  stores the SHA-256 hash. Same convention reset tokens will use in
  Phase 1b — sets the pattern now.

Usage:
    python -m app.cli.issue_invite \\
        --email rob@colonial-capital.com \\
        --inviter-email j@mullincap.com \\
        --inviter-name "John Mullin" \\
        --inviter-firm Mullincap \\
        [--days 7] \\
        [--base-url https://mullincap.com]

The script:
- looks up the inviter user by email and refuses to proceed if not found,
- refuses to issue a duplicate pending invite for the same email,
- prints exactly one line on success: the full /auth/invite?token=… URL,
- exits 0 on success, 1 on any error.

Spec note:
- The Phase 1a spec listed --email / --inviter-email / --firm. We added
  --inviter-name (the schema's inviter_name column is NOT NULL) and
  renamed --firm to --inviter-firm for clarity, since the column is
  the inviter's firm, not the invitee's. Default --days is 7.
"""

from __future__ import annotations

import argparse
import hashlib
import secrets
import sys
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import RealDictCursor

from ..core.config import settings


def _get_conn():
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        connect_timeout=5,
    )


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="issue_invite",
        description="Issue a 3M platform invitation token.",
    )
    parser.add_argument("--email", required=True, help="invitee email address")
    parser.add_argument("--inviter-email", required=True, help="email of the inviting user (must exist in user_mgmt.users)")
    parser.add_argument("--inviter-name", required=True, help="display name shown on the invite banner")
    parser.add_argument("--inviter-firm", required=True, help="firm name shown on the invite banner")
    parser.add_argument("--days", type=int, default=7, help="invite TTL in days (default: 7)")
    parser.add_argument("--base-url", default="https://mullincap.com", help="base URL for the invite link")
    args = parser.parse_args()

    if args.days <= 0:
        print("error: --days must be positive", file=sys.stderr)
        return 1

    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Resolve inviter
            cur.execute(
                "SELECT user_id FROM user_mgmt.users WHERE email = %s",
                (args.inviter_email,),
            )
            inviter = cur.fetchone()
            if not inviter:
                print(
                    f"error: no user found with email {args.inviter_email!r}",
                    file=sys.stderr,
                )
                return 1
            inviter_user_id = inviter["user_id"]

            # Reject duplicate pending invites for the same email
            cur.execute(
                """
                SELECT invitation_id FROM user_mgmt.invitations
                WHERE invited_email = %s AND accepted_at IS NULL AND expires_at > NOW()
                """,
                (args.email,),
            )
            existing = cur.fetchone()
            if existing:
                print(
                    f"error: a pending unexpired invite already exists for "
                    f"{args.email!r} (invitation_id={existing['invitation_id']})",
                    file=sys.stderr,
                )
                return 1

            # Reject if a user with that email already exists
            cur.execute(
                "SELECT user_id FROM user_mgmt.users WHERE email = %s",
                (args.email,),
            )
            if cur.fetchone():
                print(
                    f"error: a user with email {args.email!r} already exists",
                    file=sys.stderr,
                )
                return 1

            # Mint token + insert
            token = secrets.token_urlsafe(32)
            token_hash = _hash_token(token)
            expires_at = datetime.now(timezone.utc) + timedelta(days=args.days)

            cur.execute(
                """
                INSERT INTO user_mgmt.invitations
                    (token_hash, invited_email, inviter_user_id,
                     inviter_name, inviter_firm, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    token_hash,
                    args.email,
                    inviter_user_id,
                    args.inviter_name,
                    args.inviter_firm,
                    expires_at,
                ),
            )
        conn.commit()
    finally:
        conn.close()

    base = args.base_url.rstrip("/")
    print(f"{base}/auth/invite?token={token}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
