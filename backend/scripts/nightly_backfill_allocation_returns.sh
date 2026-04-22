#!/bin/bash
# Nightly safety-net for execution telemetry completeness.
#
# Runs inside the backend container to backfill yesterday's
# user_mgmt.allocation_returns row for any allocation whose live writer
# missed fill_rate / avg_entry_slip_bps / avg_exit_slip_bps (typically
# because of a reconcile ordering bug or a transient DB hiccup).
#
# Invoked by host cron at 00:10 UTC. The 10-minute buffer lets the
# trader's post-close writers finish; the backfill then fills in
# whatever the writer missed. COALESCE-style UPDATE never overwrites
# a value the writer did persist.
#
# Scheduled via /etc/crontab on the host; pulled in by `git pull` via
# the standard deploy workflow — no container rebuild required when
# this script changes.

set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-/root/benji/docker-compose.yml}"
LOG_FILE="${LOG_FILE:-/mnt/quant-data/logs/trader/backfill_cron.log}"
# Yesterday in UTC — session close happens at 23:55 UTC, so by 00:10
# the date we care about is definitively in the past UTC-wise.
DATE="$(date -u -d "yesterday" +%Y-%m-%d)"

mkdir -p "$(dirname "$LOG_FILE")"

{
  echo "── $(date -u +%Y-%m-%dT%H:%M:%SZ) | backfill_allocation_returns | date=$DATE ──"
  docker compose -f "$COMPOSE_FILE" exec -T backend \
    python -m app.cli.backfill_allocation_returns \
      --date "$DATE" \
      --all
  echo
} >> "$LOG_FILE" 2>&1
