#!/bin/bash
set -euo pipefail

# ──────────────────────────────────────────────────────
# Benji3m — EC2 deploy script
# Supports Amazon Linux 2023 and Ubuntu 24.04
# ──────────────────────────────────────────────────────

echo "═══════════════════════════════════════════"
echo "  Benji3m — Server Setup"
echo "═══════════════════════════════════════════"

# Detect OS
if command -v dnf &>/dev/null; then
    PKG="dnf"
elif command -v yum &>/dev/null; then
    PKG="yum"
elif command -v apt-get &>/dev/null; then
    PKG="apt"
else
    echo "ERROR: Unsupported package manager" && exit 1
fi

echo "→ Detected package manager: $PKG"

# 1. Install system packages
echo "→ Installing system dependencies..."
if [ "$PKG" = "apt" ]; then
    sudo apt-get update -qq
    sudo apt-get install -y -qq docker.io docker-compose-v2 git
elif [ "$PKG" = "dnf" ] || [ "$PKG" = "yum" ]; then
    sudo $PKG install -y docker git
    # Install docker compose plugin
    sudo mkdir -p /usr/local/lib/docker/cli-plugins
    sudo curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-$(uname -m)" \
        -o /usr/local/lib/docker/cli-plugins/docker-compose
    sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
fi

# 2. Start Docker
echo "→ Starting Docker..."
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -aG docker "$USER"

# 3. Create data directory
echo "→ Creating data directory..."
sudo mkdir -p /data/oi_raw
sudo mkdir -p /data/marketcap
sudo chown -R "$USER:$USER" /data

echo ""
echo "═══════════════════════════════════════════"
echo "  System ready. Next steps:"
echo "═══════════════════════════════════════════"
echo ""
echo "  1. Upload your data files (from your Mac):"
echo "     scp -i key.pem master_data_table.parquet  ec2-user@<IP>:/data/oi_raw/"
echo "     scp -i key.pem marketcap_daily.parquet    ec2-user@<IP>:/data/marketcap/"
echo "     scp -i key.pem coins_universe.parquet     ec2-user@<IP>:/data/marketcap/"
echo "     scp -i key.pem deploys_*.csv              ec2-user@<IP>:/data/"
echo "     scp -i key.pem portfolio_matrix_gated.csv ec2-user@<IP>:/data/"
echo "     scp -i key.pem dispersion_cache.csv       ec2-user@<IP>:/data/"
echo "     scp -i key.pem adv_per_symbol.csv         ec2-user@<IP>:/data/"
echo ""
echo "  2. Deploy:"
echo "     cd benji"
echo "     docker compose up -d --build"
echo ""
echo "  3. Check status:"
echo "     docker compose ps"
echo "     docker compose logs -f"
echo ""
echo "  4. Open in browser:"
echo "     http://<EC2-PUBLIC-IP>"
echo ""
echo "  NOTE: Log out and back in for Docker group to take effect,"
echo "  or run: newgrp docker"
echo ""
