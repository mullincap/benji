#!/bin/bash
# =============================================================================
# run_backfill.sh
# =============================================================================
# Applies the v2 schema and runs all backfill scripts in order.
# Run from the repo root on the production server:
#
#   bash run_backfill.sh
#
# All steps are idempotent — safe to re-run if anything fails midway.
# =============================================================================

set -e  # Exit on any error

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="/mnt/quant-data/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

mkdir -p "$LOG_DIR"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

log "=================================================="
log "  Benji3m — Schema v2 + Backfill"
log "  Started: $TIMESTAMP"
log "=================================================="

# ── Step 1: Apply schema ──────────────────────────────────────────────────────
log "STEP 1: Applying schema v2 to TimescaleDB..."
docker exec -i timescaledb psql -U quant -d marketdata < "$REPO_DIR/schema.sql" \
    2>&1 | tee "$LOG_DIR/schema_apply_$TIMESTAMP.log"

log "Verifying schemas..."
docker exec timescaledb psql -U quant -d marketdata -c "\dt market.*"
docker exec timescaledb psql -U quant -d marketdata -c "\dt audit.*"
docker exec timescaledb psql -U quant -d marketdata -c "\dt user_mgmt.*"
log "✅ Schema applied"

# ── Step 2: Seed symbols ──────────────────────────────────────────────────────
log ""
log "STEP 2: Seeding market.symbols..."
python3 "$REPO_DIR/pipeline/db/seed_symbols.py" \
    2>&1 | tee "$LOG_DIR/seed_symbols_$TIMESTAMP.log"
log "✅ Symbols seeded"

# ── Step 3: Backfill marketcap ────────────────────────────────────────────────
log ""
log "STEP 3: Backfilling market.marketcap_daily from CoinGecko parquet..."
python3 "$REPO_DIR/pipeline/db/backfill_marketcap.py" \
    2>&1 | tee "$LOG_DIR/backfill_marketcap_$TIMESTAMP.log"
log "✅ Marketcap backfill complete"

# ── Step 4: Backfill futures_1m ───────────────────────────────────────────────
log ""
log "STEP 4: Migrating market_data_1m → market.futures_1m..."
log "        (This is the largest step — may take 10-30 minutes)"
python3 "$REPO_DIR/pipeline/db/backfill_futures_1m.py" \
    2>&1 | tee "$LOG_DIR/backfill_futures_1m_$TIMESTAMP.log"
log "✅ futures_1m migration complete"

# ── Step 5: Backfill leaderboards ─────────────────────────────────────────────
log ""
log "STEP 5: Backfilling market.leaderboards from parquet files..."
python3 "$REPO_DIR/pipeline/db/backfill_leaderboards.py" \
    2>&1 | tee "$LOG_DIR/backfill_leaderboards_$TIMESTAMP.log"
log "✅ Leaderboards backfill complete"

# ── Final summary ─────────────────────────────────────────────────────────────
log ""
log "=================================================="
log "  All steps complete. Row counts:"
docker exec timescaledb psql -U quant -d marketdata -c "
    SELECT 'market.symbols'         AS table_name, COUNT(*) AS rows FROM market.symbols
    UNION ALL
    SELECT 'market.futures_1m',     COUNT(*) FROM market.futures_1m
    UNION ALL
    SELECT 'market.marketcap_daily', COUNT(*) FROM market.marketcap_daily
    UNION ALL
    SELECT 'market.leaderboards',   COUNT(*) FROM market.leaderboards
    ORDER BY table_name;
"
log "=================================================="
log "Logs saved to $LOG_DIR/"
