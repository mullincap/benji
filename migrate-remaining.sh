#!/bin/bash
set -euo pipefail

SCP="scp -i ~/.ssh/hetzner -o StrictHostKeyChecking=no"
HOST="root@157.180.23.162"
SRC="/Users/johnmullin/Desktop/desk/benji3m"
TMP="/tmp/benji3m_migrate"
CHECKPOINT="$TMP/.checkpoint"

mkdir -p "$TMP"
touch "$CHECKPOINT"

already_done() {
    grep -qxF "$1" "$CHECKPOINT" 2>/dev/null
}

mark_done() {
    echo "$1" >> "$CHECKPOINT"
}

echo "═══════════════════════════════════════════"
echo "  Migrating remaining data files"
echo "═══════════════════════════════════════════"
echo ""

# ── Single files ──────────────────────────────────────
SINGLES=(
    "portfolio_matrix_gated.csv"
    "dispersion_cache.csv"
    "adv_per_symbol.csv"
)

for f in "${SINGLES[@]}"; do
    if already_done "$f"; then
        echo "  ✓ $f (already uploaded)"
    else
        echo "  → Copying $f to /tmp..."
        cp "$SRC/$f" "$TMP/$f"
        echo "  → Uploading $f..."
        $SCP "$TMP/$f" "$HOST:/data/"
        mark_done "$f"
        echo "  ✓ $f done"
    fi
done

# ── Deploy CSVs (one at a time with checkpointing) ───
echo ""
echo "→ Uploading deploys_*.csv files..."

TOTAL=$(ls "$SRC"/deploys_*.csv 2>/dev/null | wc -l | tr -d ' ')
COUNT=0
SKIPPED=0

for filepath in "$SRC"/deploys_*.csv; do
    fname=$(basename "$filepath")
    COUNT=$((COUNT + 1))

    if already_done "$fname"; then
        SKIPPED=$((SKIPPED + 1))
        continue
    fi

    printf "  [%d/%d] %s..." "$COUNT" "$TOTAL" "$fname"
    cp "$filepath" "$TMP/$fname"
    $SCP "$TMP/$fname" "$HOST:/data/" >/dev/null 2>&1
    mark_done "$fname"
    echo " ✓"
done

echo ""
echo "  Uploaded: $((COUNT - SKIPPED))  Skipped: $SKIPPED  Total: $TOTAL"

# ── Verify ────────────────────────────────────────────
echo ""
echo "→ Verifying remote files..."
ssh -i ~/.ssh/hetzner -o StrictHostKeyChecking=no "$HOST" \
    "echo '  /data/oi_raw:' && ls -lh /data/oi_raw/ && echo '  /data/marketcap:' && ls -lh /data/marketcap/ && echo '  /data CSV count:' && ls /data/*.csv 2>/dev/null | wc -l"

echo ""
echo "═══════════════════════════════════════════"
echo "  Migration complete!"
echo "═══════════════════════════════════════════"
echo "  Next: ssh into EC2 and run:"
echo "    cd benji && docker compose up -d --build"
echo ""
