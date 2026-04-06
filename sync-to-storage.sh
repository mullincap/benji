#!/bin/bash
set -euo pipefail

# =============================================================================
# sync-to-storage.sh
# =============================================================================
# Syncs freshly compiled data from the server to Hetzner Object Storage.
# Run nightly via cron after the compiler finishes (e.g. 02:00 UTC).
#
# Cron entry (add to /root/benji/setup.sh cron section):
#   0 2 * * * cd /root/benji && bash sync-to-storage.sh >> /mnt/quant-data/logs/sync-storage.log 2>&1
#
# Syncs:
#   - /mnt/quant-data/raw/coingecko/     → s3://benji3m-data/raw/coingecko/
#   - /mnt/quant-data/leaderboards/      → s3://benji3m-data/leaderboards/
#   - /mnt/quant-data/raw/amberdata/     → s3://benji3m-data/raw/amberdata/
#   - Working pipeline files             → s3://benji3m-data/pipeline/
# =============================================================================

BUCKET="benji3m-data"
ENDPOINT="https://hel1.your-objectstorage.com"
DATA_ROOT="/mnt/quant-data"
REPO_DIR="/root/benji"

# Load credentials from secrets.env
SECRETS="$DATA_ROOT/credentials/secrets.env"
if [ -f "$SECRETS" ]; then
    export $(grep -E "^S3_(ACCESS_KEY|SECRET_KEY)" "$SECRETS" | xargs)
fi

export AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY:-}"
export AWS_SECRET_ACCESS_KEY="${S3_SECRET_KEY:-}"
export AWS_DEFAULT_REGION="eu-central-1"

if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "❌ S3_ACCESS_KEY or S3_SECRET_KEY not set in secrets.env — aborting"
    exit 1
fi

S3="aws s3 --endpoint-url $ENDPOINT"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

echo "[$TIMESTAMP] Starting storage sync..."

# Sync CoinGecko
$S3 sync "$DATA_ROOT/raw/coingecko/" "s3://$BUCKET/raw/coingecko/" \
    --exclude "*.log" --quiet
echo "  ✓ coingecko synced"

# Sync leaderboards
$S3 sync "$DATA_ROOT/leaderboards/" "s3://$BUCKET/leaderboards/" \
    --exclude "*.log" --quiet
echo "  ✓ leaderboards synced"

# Sync amberdata raw (master parquet grows nightly)
$S3 sync "$DATA_ROOT/raw/amberdata/" "s3://$BUCKET/raw/amberdata/" \
    --exclude "*.log" --quiet
echo "  ✓ amberdata synced"

# Sync pipeline working files from repo
for f in portfolio_matrix_gated.csv dispersion_dynamic_returns_cache.csv; do
    if [ -f "$REPO_DIR/$f" ]; then
        $S3 cp "$REPO_DIR/$f" "s3://$BUCKET/pipeline/$f" --quiet
    fi
done
echo "  ✓ pipeline files synced"

echo "  [$(date '+%Y-%m-%d %H:%M:%S')] Sync complete"
