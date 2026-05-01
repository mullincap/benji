"""
backend/app/cli/blofin_sidecar.py
==================================
Container entrypoint for the BloFin private WebSocket sidecar process.

Trivially thin: delegates to
backend/app/services/sidecars/blofin_account_sidecar.py:main(). Lives
under app/cli/ because the docker-compose service uses
`python -m app.cli.blofin_sidecar` to start it, matching the convention
of every other CLI entrypoint in this project (sync_exchange_snapshots,
account_anchor_writer, etc.).
"""

from app.services.sidecars.blofin_account_sidecar import main


if __name__ == "__main__":
    raise SystemExit(main())
