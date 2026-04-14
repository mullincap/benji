#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# upload-to-storage.sh
# =============================================================================
# Pushes all benji3m historical data from your local Mac to
# Hetzner Object Storage (benji3m-data bucket, Helsinki).
#
# Run once from your Mac after a new data export or server provision.
# Safe to re-run — AWS CLI sync skips already-uploaded files.
#
# Requirements:
#   brew install awscli
#
# Usage:
#   bash upload-to-storage.sh
# =============================================================================

BUCKET="benji3m-data"
ENDPOINT="https://hel1.your-objectstorage.com"

# Load credentials from .env if present, otherwise expect env vars
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
    set +u
    source <(grep -E "^S3_(ACCESS_KEY|SECRET_KEY)" "$SCRIPT_DIR/.env")
    set -u
fi

export AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY:?S3_ACCESS_KEY not set — add to .env or export it}"
export AWS_SECRET_ACCESS_KEY="${S3_SECRET_KEY:?S3_SECRET_KEY not set — add to .env or export it}"
export AWS_DEFAULT_REGION="eu-central-1"

# Local source paths
BASE="$HOME/Desktop/desk/benji3m"
OI_RAW="$HOME/Desktop/desk/import/oi_logger/backfills/1raws/oi_raw"
MARKETCAP_SRC="$BASE/binetl/data/marketcap"

S3="aws s3 --endpoint-url $ENDPOINT"

echo "═══════════════════════════════════════════════════════"
echo "  Benji3m — Upload to Hetzner Object Storage"
echo "  Bucket: s3://$BUCKET"
echo "  Endpoint: $ENDPOINT"
echo "═══════════════════════════════════════════════════════"
echo ""

# Verify AWS CLI is installed
if ! command -v aws &>/dev/null; then
    echo "❌ AWS CLI not found. Install with: brew install awscli"
    exit 1
fi

# Verify credentials work
echo "→ Verifying credentials..."
$S3 ls "s3://$BUCKET" > /dev/null 2>&1 && echo "  ✓ Connected to bucket" || {
    echo "  ❌ Could not connect to bucket — check credentials"
    exit 1
}

# ── [1] CoinGecko marketcap ────────────────────────────────────────────────
echo ""
echo "→ [1/5] Uploading CoinGecko marketcap data..."
if [ -d "$MARKETCAP_SRC" ]; then
    $S3 sync "$MARKETCAP_SRC/" "s3://$BUCKET/raw/coingecko/" \
        --exclude "*.DS_Store" \
        --no-progress
    echo "  ✓ Done"
else
    echo "  ⚠ $MARKETCAP_SRC not found — skipping"
fi

# ── [2] Master data parquet (Amberdata) ───────────────────────────────────
echo ""
echo "→ [2/5] Uploading master_data_table.parquet (large file)..."
MASTER="$OI_RAW/master_data_table.parquet"
if [ -f "$MASTER" ]; then
    SIZE=$(du -h "$MASTER" | cut -f1)
    echo "  Size: $SIZE — uploading with multipart..."
    $S3 cp "$MASTER" "s3://$BUCKET/raw/amberdata/master_data_table.parquet" \
        --no-progress
    echo "  ✓ Done"
else
    # Try alternate locations
    for alt in \
        "$BASE/master_data_table.parquet" \
        "$HOME/Desktop/desk/import/oi_logger/backfills/oi_raw/master_data_table.parquet"
    do
        if [ -f "$alt" ]; then
            echo "  Found at $alt — uploading..."
            $S3 cp "$alt" "s3://$BUCKET/raw/amberdata/master_data_table.parquet" \
                --no-progress
            echo "  ✓ Done"
            break
        fi
    done
    echo "  ⚠ master_data_table.parquet not found in expected locations"
fi

# ── [3] Leaderboard parquets ──────────────────────────────────────────────
echo ""
echo "→ [3/5] Uploading leaderboard parquets..."

LB_UPLOADED=0
LB_MISSING=0

upload_lb() {
    local metric="$1"
    local f="$2"
    local filepath="$BASE/$f"
    if [ -f "$filepath" ]; then
        $S3 cp "$filepath" "s3://$BUCKET/leaderboards/$metric/$f" --no-progress
        echo "  ✓ $metric/$f"
        LB_UPLOADED=$((LB_UPLOADED + 1))
    else
        echo "  ⚠ Not found: $f"
        LB_MISSING=$((LB_MISSING + 1))
    fi
}

# Price leaderboards
upload_lb "price" "intraday_pct_leaderboard_price_top333_anchor0000_ALL.parquet"
upload_lb "price" "leaderboard_price_top100_filtered_0M.parquet"
upload_lb "price" "leaderboard_price_top100_filtered_0M_5m.parquet"
upload_lb "price" "leaderboard_price_top100_filtered_0M_max2000M.parquet"
upload_lb "price" "leaderboard_price_top100_filtered_0M_max2000M_5m.parquet"

# Open interest leaderboards
upload_lb "open_interest" "intraday_pct_leaderboard_open_interest_top333_anchor0000_ALL.parquet"
upload_lb "open_interest" "leaderboard_open_interest_top100_filtered_0M.parquet"
upload_lb "open_interest" "leaderboard_open_interest_top100_filtered_0M_5m.parquet"
upload_lb "open_interest" "leaderboard_open_interest_top100_filtered_0M_max2000M.parquet"
upload_lb "open_interest" "leaderboard_open_interest_top100_filtered_0M_max2000M_5m.parquet"

echo "  Leaderboards: $LB_UPLOADED uploaded, $LB_MISSING not found"

# ── [4] Pipeline working files ────────────────────────────────────────────
echo ""
echo "→ [4/5] Uploading pipeline working files..."
WORKING_FILES=(
    "portfolio_matrix_gated.csv"
    "dispersion_cache.csv"
    "dispersion_dynamic_returns_cache.csv"
    "adv_per_symbol.csv"
)
for f in "${WORKING_FILES[@]}"; do
    if [ -f "$BASE/$f" ]; then
        $S3 cp "$BASE/$f" "s3://$BUCKET/pipeline/$f" --no-progress
        echo "  ✓ $f"
    else
        echo "  ⚠ $f not found"
    fi
done

# ── [5] Any Binance/BloFin raw data ──────────────────────────────────────
echo ""
echo "→ [5/5] Syncing raw exchange data directories..."
for dir in binance blofin; do
    local_dir="$BASE/binetl/data/$dir"
    if [ -d "$local_dir" ]; then
        $S3 sync "$local_dir/" "s3://$BUCKET/raw/$dir/" \
            --exclude "*.DS_Store" \
            --no-progress
        echo "  ✓ $dir synced"
    else
        echo "  ⚠ No $dir data directory found"
    fi
done

# ── Summary ───────────────────────────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Bucket contents:"
$S3 ls "s3://$BUCKET" --recursive --human-readable --summarize | tail -5
echo "═══════════════════════════════════════════════════════"
echo "  Upload complete."
echo "  Next: ssh mcap && cd /root/benji && bash run_backfill.sh"
echo "═══════════════════════════════════════════════════════"
