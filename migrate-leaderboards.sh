#!/bin/bash
set -euo pipefail

SCP="scp -i ~/.ssh/hetzner -o StrictHostKeyChecking=no"
HOST="root@157.180.23.162"
SRC="/Users/johnmullin/Desktop/desk/benji3m"

FILES=(
    "intraday_pct_leaderboard_open_interest_top333_anchor0000_ALL.parquet"
    "intraday_pct_leaderboard_price_top333_anchor0000_ALL.parquet"
    "leaderboard_open_interest_top100_filtered_0M.parquet"
    "leaderboard_open_interest_top100_filtered_0M_5m.parquet"
    "leaderboard_open_interest_top100_filtered_0M_max2000M.parquet"
    "leaderboard_open_interest_top100_filtered_0M_max2000M_5m.parquet"
    "leaderboard_price_top100_filtered_0M.parquet"
    "leaderboard_price_top100_filtered_0M_5m.parquet"
    "leaderboard_price_top100_filtered_0M_max2000M.parquet"
    "leaderboard_price_top100_filtered_0M_max2000M_5m.parquet"
)

TOTAL=${#FILES[@]}
COUNT=0

echo "═══════════════════════════════════════════"
echo "  Uploading $TOTAL leaderboard files"
echo "═══════════════════════════════════════════"
echo ""

for f in "${FILES[@]}"; do
    COUNT=$((COUNT + 1))
    SIZE=$(du -h "$SRC/$f" | cut -f1)
    echo "[$COUNT/$TOTAL] $f ($SIZE)"
    cp "$SRC/$f" "/tmp/$f"
    $SCP "/tmp/$f" "$HOST:/data/"
    echo "  ✓ done"
    echo ""
done

echo "═══════════════════════════════════════════"
echo "  All $TOTAL leaderboard files uploaded"
echo "═══════════════════════════════════════════"
