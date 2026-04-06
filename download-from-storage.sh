#!/bin/bash
set -euo pipefail

# =============================================================================
# download-from-storage.sh
# =============================================================================
# Downloads all historical seed data from Hetzner Object Storage to the server.
# Called automatically by setup.sh on new server provision.
# Safe to re-run — AWS CLI sync skips already-downloaded files.
#
# Usage:
#   bash download-from-storage.sh
# =============================================================================

BUCKET="benji3m-data"
ENDPOINT="https://hel1.your-objectstorage.com"
DATA_ROOT="/mnt/quant-data"
REPO_DIR="/root/benji"

# Load credentials
SECRETS="$DATA_ROOT/credentials/secrets.env"
if [ -f "$SECRETS" ]; then
    set +u
    source <(grep -E "^S3_(ACCESS_KEY|SECRET_KEY)" "$SECRETS")
    set -u
fi

export AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY:-}"
export AWS_SECRET_ACCESS_KEY="${S3_SECRET_KEY:-}"
export AWS_DEFAULT_REGION="eu-central-1"

if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "❌ S3_ACCESS_KEY or S3_SECRET_KEY not set in $SECRETS"
    echo "   Add them and re-run: bash download-from-storage.sh"
    exit 1
fi

S3="aws s3 --endpoint-url $ENDPOINT"

echo "═══════════════════════════════════════════════════════"
echo "  Benji3m — Download seed data from Object Storage"
echo "  Bucket: s3://$BUCKET"
echo "═══════════════════════════════════════════════════════"
echo ""

# Verify connectivity
echo "→ Verifying connection to bucket..."
$S3 ls "s3://$BUCKET/" > /dev/null 2>&1 && echo "  ✓ Connected" || {
    echo "  ❌ Cannot reach bucket — check credentials and endpoint"
    exit 1
}

# Ensure directories exist
mkdir -p \
    "$DATA_ROOT/raw/coingecko" \
    "$DATA_ROOT/raw/amberdata" \
    "$DATA_ROOT/leaderboards/price" \
    "$DATA_ROOT/leaderboards/open_interest" \
    "$DATA_ROOT/leaderboards/volume" \
    "$DATA_ROOT/raw/binance/futures" \
    "$DATA_ROOT/raw/blofin"

# ── [1] CoinGecko marketcap ────────────────────────────────────────────────
echo ""
echo "→ [1/5] Downloading CoinGecko marketcap..."
$S3 sync "s3://$BUCKET/raw/coingecko/" "$DATA_ROOT/raw/coingecko/" \
    --no-progress
echo "  ✓ Done — $(ls $DATA_ROOT/raw/coingecko/*.parquet 2>/dev/null | wc -l) parquet files"

# ── [2] Master data parquet ───────────────────────────────────────────────
echo ""
echo "→ [2/5] Downloading master_data_table.parquet (large)..."
$S3 sync "s3://$BUCKET/raw/amberdata/" "$DATA_ROOT/raw/amberdata/" \
    --no-progress
echo "  ✓ Done — $(du -sh $DATA_ROOT/raw/amberdata/ 2>/dev/null | cut -f1) total"

# ── [3] Leaderboard parquets ──────────────────────────────────────────────
echo ""
echo "→ [3/5] Downloading leaderboard parquets..."
$S3 sync "s3://$BUCKET/leaderboards/" "$DATA_ROOT/leaderboards/" \
    --no-progress
PRICE_COUNT=$(ls "$DATA_ROOT/leaderboards/price/"*.parquet 2>/dev/null | wc -l)
OI_COUNT=$(ls "$DATA_ROOT/leaderboards/open_interest/"*.parquet 2>/dev/null | wc -l)
echo "  ✓ Done — price: $PRICE_COUNT files, open_interest: $OI_COUNT files"

# ── [4] Pipeline working files ────────────────────────────────────────────
echo ""
echo "→ [4/5] Downloading pipeline working files..."
$S3 sync "s3://$BUCKET/pipeline/" "$REPO_DIR/" \
    --no-progress
echo "  ✓ Done"

# ── [5] Raw exchange data ─────────────────────────────────────────────────
echo ""
echo "→ [5/5] Downloading raw exchange data..."
$S3 sync "s3://$BUCKET/raw/binance/" "$DATA_ROOT/raw/binance/" \
    --no-progress
$S3 sync "s3://$BUCKET/raw/blofin/" "$DATA_ROOT/raw/blofin/" \
    --no-progress
echo "  ✓ Done"

# ── Summary ───────────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Download complete. Data is ready for backfill."
echo ""
echo "  Next step:"
echo "    cd /root/benji && bash run_backfill.sh"
echo "═══════════════════════════════════════════════════════"
