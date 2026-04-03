#!/bin/bash
set -euo pipefail

cd ~/benji
echo "→ Pulling latest..."
git pull

echo "→ Rebuilding and restarting..."
docker compose up -d --build

echo "→ Cleaning old images..."
docker image prune -f

echo "✓ Deploy complete"
docker compose ps
