#!/usr/bin/env bash
set -euo pipefail

# Uploads leaderboard parquets and pipeline CSVs to Hetzner Object Storage.
# Run this after the master parquet is already uploaded.

BUCKET="benji3m-data"
ENDPOINT="https://hel1.your-objectstorage.com"
BASE="$HOME/Desktop/desk/benji3m"

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
S3="aws s3 --endpoint-url $ENDPOINT"

START=$(date +%s)
UPLOADED=0
SKIPPED=0
TOTAL=10

upload_lb() {
    local n="$1"
    local metric="$2"
    local f="$3"
    local filepath="$BASE/$f"
    local elapsed=$(( $(date +%s) - START ))
    if [ -f "$filepath" ]; then
        SIZE=$(du -h "$filepath" | cut -f1)
        echo "  [$n/$TOTAL] $metric/$f ($SIZE) — ${elapsed}s elapsed"
        $S3 cp "$filepath" "s3://$BUCKET/leaderboards/$metric/$f"
        echo "    ✓ done"
        UPLOADED=$((UPLOADED + 1))
    else
        echo "  [$n/$TOTAL] ⚠ NOT FOUND: $f"
        SKIPPED=$((SKIPPED + 1))
    fi
}

echo "═══════════════════════════════════════════════════════"
echo "  Uploading leaderboard parquets ($TOTAL files)"
echo "  Bucket: s3://$BUCKET/leaderboards/"
echo "═══════════════════════════════════════════════════════"
echo ""

upload_lb  1 "price"         "intraday_pct_leaderboard_price_top333_anchor0000_ALL.parquet"
upload_lb  2 "price"         "leaderboard_price_top100_filtered_0M.parquet"
upload_lb  3 "price"         "leaderboard_price_top100_filtered_0M_5m.parquet"
upload_lb  4 "price"         "leaderboard_price_top100_filtered_0M_max2000M.parquet"
upload_lb  5 "price"         "leaderboard_price_top100_filtered_0M_max2000M_5m.parquet"
upload_lb  6 "open_interest" "intraday_pct_leaderboard_open_interest_top333_anchor0000_ALL.parquet"
upload_lb  7 "open_interest" "leaderboard_open_interest_top100_filtered_0M.parquet"
upload_lb  8 "open_interest" "leaderboard_open_interest_top100_filtered_0M_5m.parquet"
upload_lb  9 "open_interest" "leaderboard_open_interest_top100_filtered_0M_max2000M.parquet"
upload_lb 10 "open_interest" "leaderboard_open_interest_top100_filtered_0M_max2000M_5m.parquet"

echo ""
echo "Uploading pipeline working files..."
for f in portfolio_matrix_gated.csv dispersion_dynamic_returns_cache.csv dispersion_cache.csv; do
    if [ -f "$BASE/$f" ]; then
        echo "  → $f"
        $S3 cp "$BASE/$f" "s3://$BUCKET/pipeline/$f"
        echo "    ✓ done"
    else
        echo "  ⚠ $f not found"
    fi
done

ELAPSED=$(( $(date +%s) - START ))
echo ""
echo "═══════════════════════════════════════════════════════"
echo "  Done in ${ELAPSED}s — $UPLOADED uploaded, $SKIPPED not found"
echo "  Bucket summary:"
$S3 ls "s3://$BUCKET" --recursive --human-readable --summarize | tail -3
echo "═══════════════════════════════════════════════════════"
