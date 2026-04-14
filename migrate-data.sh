#!/bin/bash
set -euo pipefail

# ──────────────────────────────────────────────────────
# Benji3m — Migrate data files to EC2
# Run this from your Mac
# ──────────────────────────────────────────────────────

# ═══ CONFIGURE THESE ═══════════════════════════════════
EC2_HOST="root@157.180.23.162"          # ← replace with your EC2 IP
SSH_KEY="/Users/johnmullin/Desktop/.ssh/benji-key.pem"            # ← replace with your key path
# ═══════════════════════════════════════════════════════

# Local source paths
BASE="/Users/johnmullin/Desktop/desk/benji3m"
OI_RAW="/Users/johnmullin/Desktop/desk/import/oi_logger/backfills/1raws/oi_raw"

SSH_OPTS="-i $SSH_KEY -o StrictHostKeyChecking=no"
SCP="scp $SSH_OPTS"
SSH="ssh $SSH_OPTS"

echo "═══════════════════════════════════════════"
echo "  Benji3m — Data Migration"
echo "═══════════════════════════════════════════"
echo "  Target: $EC2_HOST"
echo ""

# Ensure remote directories exist
echo "→ Creating remote directories..."
$SSH $EC2_HOST "sudo mkdir -p /data/oi_raw /data/marketcap && sudo chown -R \$USER:\$USER /data"

# # 1. Master parquet (6 GB — this will take a while)
echo ""
echo "→ [1/7] Uploading master_data_table.parquet (6 GB)..."
echo "         This will take 10-30 minutes depending on upload speed."
$SCP "$OI_RAW/master_data_table.parquet" "$EC2_HOST:/data/oi_raw/"

# 2. Market cap files
echo "→ [2/7] Uploading marketcap_daily.parquet (11 MB)..."
$SCP "$BASE/binetl/data/marketcap/marketcap_daily.parquet" "$EC2_HOST:/data/marketcap/"

echo "→ [3/7] Uploading coins_universe.parquet..."
$SCP "$BASE/binetl/data/marketcap/coins_universe.parquet" "$EC2_HOST:/data/marketcap/"

# Verify
echo ""
echo "→ Verifying remote files..."
$SSH $EC2_HOST "echo '  /data/oi_raw:' && ls -lh /data/oi_raw/ && echo '  /data/marketcap:' && ls -lh /data/marketcap/ && echo '  /data/*.csv:' && ls /data/*.csv | wc -l && echo ' CSV files uploaded'"

echo ""
echo "═══════════════════════════════════════════"
echo "  Migration complete!"
echo "═══════════════════════════════════════════"
echo ""
echo "  Next: cd benji && docker compose up -d --build"
echo ""
