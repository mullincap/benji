#!/usr/bin/env bash
# =============================================================================
# setup.sh — Benji3m full server bootstrap
# =============================================================================
# Brings a fresh Hetzner Ubuntu 22.04/24.04 instance to fully operational.
# Run as root after cloning the repo and filling in .env.
#
# Usage:
#   git clone https://github.com/YOUR_ORG/benji3m.git && cd benji3m
#   cp .env.example .env && nano .env
#   bash setup.sh
#
# Two modes:
#   With DOMAIN set in .env    → full SSL setup, app at https://domain
#   Without DOMAIN             → HTTP-only, app at http://SERVER_IP immediately
#
# To add SSL after pointing DNS:
#   Add DOMAIN and SSL_EMAIL to .env then re-run: bash setup.sh
#
# Steps:
#    1. System packages
#    2. Docker + Docker Compose
#    3. Hetzner Volume — detect, format, mount at /mnt/quant-data
#    4. DATA_ROOT environment variable (system-wide, persistent)
#    5. TimescaleDB (Docker, data on volume)
#    6. TimescaleDB schema
#    7. Python venv + pipeline dependencies
#    8. quant-data directory structure
#    9. Credentials + secrets.env for cron
#   10. nginx config (HTTP-only or full SSL)
#   11. Certbot SSL (skipped if no domain)
#   12. .env.production (updated paths for new filesystem)
#   13. docker-compose.yml volume mount patch
#   14. App stack (docker compose up --build)
#   15. Pipeline cron jobs
#   16. Post-install verification
# =============================================================================

set -euo pipefail

# ─── Colour helpers ───────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
BLUE='\033[0;34m'; BOLD='\033[1m'; RESET='\033[0m'

log()     { echo -e "${BLUE}${BOLD}[setup]${RESET} $*"; }
success() { echo -e "${GREEN}${BOLD}[✓]${RESET} $*"; }
warn()    { echo -e "${YELLOW}${BOLD}[!]${RESET} $*"; }
error()   { echo -e "${RED}${BOLD}[✗]${RESET} $*"; exit 1; }

# ─── Flags ────────────────────────────────────────────────────────────────────
SSL_ONLY=false
NO_DOMAIN=false
SKIP_CRONS=false
for arg in "$@"; do
    [[ "$arg" == "--ssl-only" ]]   && SSL_ONLY=true
    [[ "$arg" == "--no-domain" ]]  && NO_DOMAIN=true
    [[ "$arg" == "--skip-crons" ]] && SKIP_CRONS=true
done

# ─── Paths ────────────────────────────────────────────────────────────────────
REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_ROOT="/mnt/quant-data"
TIMESCALE_VERSION="2.14.2-pg16"
ENV_FILE="$REPO_DIR/.env"
PIPELINE_DIR="$REPO_DIR/pipeline"
VENV_DIR="$PIPELINE_DIR/.venv"

# Auto-detect Python — prefer 3.12, fall back to 3.11, then system python3
if command -v python3.12 &>/dev/null; then
    PYTHON_VERSION="3.12"
elif command -v python3.11 &>/dev/null; then
    PYTHON_VERSION="3.11"
else
    PYTHON_VERSION=$(python3 --version 2>&1 | grep -oP '3\.\d+' | head -1)
    [[ -n "$PYTHON_VERSION" ]] || { echo "No Python 3 found"; exit 1; }
fi

# Auto-detect Hetzner Volume — stable by-id path, falls back to /dev/sdb
VOLUME_DEVICE=$(ls /dev/disk/by-id/scsi-0HC_Volume_* 2>/dev/null | head -1)
[[ -z "$VOLUME_DEVICE" ]] && VOLUME_DEVICE="/dev/sdb"

echo ""
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "${BOLD}  Benji3m — Server Bootstrap${RESET}"
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# 0. Pre-flight
# ─────────────────────────────────────────────────────────────────────────────
log "Pre-flight checks..."
[[ "$(id -u)" -eq 0 ]] || error "Run as root or with sudo."
[[ -f "$ENV_FILE" ]] || error ".env not found. Copy .env.example to .env and fill in secrets."

source "$ENV_FILE"
[[ -n "${DB_PASSWORD:-}" ]]       || error "DB_PASSWORD not set in .env"
[[ -n "${AMBER_API_KEY:-}" ]]     || error "AMBER_API_KEY not set in .env"
[[ -n "${COINGECKO_API_KEY:-}" ]] || error "COINGECKO_API_KEY not set in .env"

DB_NAME="${DB_NAME:-marketdata}"
DB_USER="${DB_USER:-quant}"
DOMAIN="${DOMAIN:-}"

if [[ "$NO_DOMAIN" == true ]] || [[ -z "$DOMAIN" ]]; then
    NO_DOMAIN=true
    warn "No DOMAIN set — HTTP-only mode. App will be at http://$(curl -s ifconfig.me 2>/dev/null || curl -s api.ipify.org 2>/dev/null || hostname -I | awk '{print $1}')"
    warn "To add SSL later: set DOMAIN + SSL_EMAIL in .env, then run: bash setup.sh --ssl-only"
else
    [[ -n "${SSL_EMAIL:-}" ]] || error "SSL_EMAIL not set in .env (required for Certbot)"
    log "Domain: $DOMAIN — will obtain SSL certificate"
fi

success "Pre-flight passed"

# ─────────────────────────────────────────────────────────────────────────────
# 1. System packages
# ─────────────────────────────────────────────────────────────────────────────
if [[ "$SSL_ONLY" == false ]]; then
    log "Updating system packages..."
    apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get upgrade -y -qq
    apt-get install -y -qq \
        curl wget git unzip \
        python3 python3-pip python3-venv \
        python3-dateutil \
        postgresql-client \
        cron \
        htop ncdu tree \
        ca-certificates gnupg lsb-release
    # Certbot — prefer snap for latest version, fall back to apt
    if ! command -v certbot &>/dev/null; then
        if command -v snap &>/dev/null; then
            snap install --classic certbot 2>/dev/null \
                && ln -sf /snap/bin/certbot /usr/bin/certbot \
                && success "Certbot installed via snap" \
                || { apt-get install -y -qq certbot && success "Certbot installed via apt"; }
        else
            apt-get install -y -qq certbot
            success "Certbot installed via apt"
        fi
    else
        success "Certbot already installed ($(certbot --version 2>&1))"
    fi
    success "System packages installed"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 2. Docker + Docker Compose
# ─────────────────────────────────────────────────────────────────────────────
if [[ "$SSL_ONLY" == false ]]; then
    if command -v docker &>/dev/null; then
        success "Docker already installed ($(docker --version))"
    else
        log "Installing Docker..."
        install -m 0755 -d /etc/apt/keyrings
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
            | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
        chmod a+r /etc/apt/keyrings/docker.gpg
        echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
            https://download.docker.com/linux/ubuntu $(lsb_release -cs 2>/dev/null || . /etc/os-release && echo $VERSION_CODENAME) stable" \
            > /etc/apt/sources.list.d/docker.list
        apt-get update -qq
        apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-compose-plugin
        systemctl enable docker
        systemctl start docker
        success "Docker installed"
    fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# 3. Hetzner Volume — detect, format, mount
# ─────────────────────────────────────────────────────────────────────────────
if [[ "$SSL_ONLY" == false ]]; then
    log "Setting up data volume at $DATA_ROOT..."
    if [[ -b "$VOLUME_DEVICE" ]]; then
        FS_TYPE=$(blkid -o value -s TYPE "$VOLUME_DEVICE" 2>/dev/null || echo "")
        if [[ -z "$FS_TYPE" ]]; then
            log "Formatting $VOLUME_DEVICE as ext4..."
            mkfs.ext4 -F "$VOLUME_DEVICE"
            success "Volume formatted"
        else
            success "Volume already formatted ($FS_TYPE)"
        fi
        mkdir -p "$DATA_ROOT"
        if ! mountpoint -q "$DATA_ROOT"; then
            mount "$VOLUME_DEVICE" "$DATA_ROOT"
            success "Volume mounted at $DATA_ROOT"
        else
            success "Volume already mounted"
        fi
        VOLUME_UUID=$(blkid -s UUID -o value "$VOLUME_DEVICE")
        grep -q "$VOLUME_UUID" /etc/fstab 2>/dev/null || \
            echo "UUID=$VOLUME_UUID $DATA_ROOT ext4 discard,nofail 0 0" >> /etc/fstab
        success "Volume persists across reboots (/etc/fstab)"
    else
        warn "Volume device $VOLUME_DEVICE not found — using local disk at $DATA_ROOT"
        warn "Attach a Hetzner Volume and re-run setup.sh to move data to the volume"
        mkdir -p "$DATA_ROOT"
    fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# 4. DATA_ROOT — system-wide persistent env var
# ─────────────────────────────────────────────────────────────────────────────
if [[ "$SSL_ONLY" == false ]]; then
    log "Setting DATA_ROOT=$DATA_ROOT system-wide..."
    grep -q "DATA_ROOT" /etc/environment 2>/dev/null || \
        echo "DATA_ROOT=$DATA_ROOT" >> /etc/environment
    export DATA_ROOT="$DATA_ROOT"
    printf 'export DATA_ROOT=%s\n' "$DATA_ROOT" > /etc/profile.d/quant.sh
    success "DATA_ROOT set system-wide"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 5. TimescaleDB
# ─────────────────────────────────────────────────────────────────────────────
if [[ "$SSL_ONLY" == false ]]; then
    log "Starting TimescaleDB..."
    TSDB_DATA_DIR="$DATA_ROOT/.timescaledb"
    mkdir -p "$TSDB_DATA_DIR"

    if docker ps -a --format '{{.Names}}' | grep -q "^timescaledb$"; then
        docker start timescaledb 2>/dev/null || true
        success "TimescaleDB container already running"
    else
        docker run -d \
            --name timescaledb \
            --restart unless-stopped \
            -e POSTGRES_DB="$DB_NAME" \
            -e POSTGRES_USER="$DB_USER" \
            -e POSTGRES_PASSWORD="$DB_PASSWORD" \
            -v "$TSDB_DATA_DIR:/var/lib/postgresql/data" \
            -p 127.0.0.1:5432:5432 \
            timescale/timescaledb:${TIMESCALE_VERSION}

        log "Waiting for TimescaleDB to be ready..."
        for i in $(seq 1 30); do
            docker exec timescaledb pg_isready -U "$DB_USER" -d "$DB_NAME" &>/dev/null \
                && { success "TimescaleDB ready"; break; } || sleep 2
            [[ $i -eq 30 ]] && error "TimescaleDB failed to start after 60s"
        done
    fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# 6. TimescaleDB schema
# ─────────────────────────────────────────────────────────────────────────────
if [[ "$SSL_ONLY" == false ]]; then
    log "Applying TimescaleDB schema..."

    # Step 1: Install the extension. TimescaleDB restarts postgres internally
    # after CREATE EXTENSION on first run — this is normal and expected.
    docker exec -i timescaledb psql -U "$DB_USER" -d "$DB_NAME"         -c "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;" 2>/dev/null || true

    # Step 2: Wait for TimescaleDB to come back up after the internal restart
    log "Waiting for TimescaleDB to recover after extension install..."
    sleep 5
    for i in $(seq 1 20); do
        docker exec timescaledb pg_isready -U "$DB_USER" -d "$DB_NAME" &>/dev/null             && { success "TimescaleDB ready for schema"; break; } || sleep 3
        [[ $i -eq 20 ]] && error "TimescaleDB did not recover — check: docker logs timescaledb"
    done

    # Step 3: Apply the full schema now that TimescaleDB is stable
    docker exec -i timescaledb psql -U "$DB_USER" -d "$DB_NAME" << 'SQL'
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE IF NOT EXISTS market_data_1m (
    timestamp_utc         TIMESTAMPTZ      NOT NULL,
    symbol                TEXT             NOT NULL,
    price                 DOUBLE PRECISION,
    open                  DOUBLE PRECISION,
    high                  DOUBLE PRECISION,
    low                   DOUBLE PRECISION,
    volume                DOUBLE PRECISION,
    quote_volume          DOUBLE PRECISION,
    trades                INTEGER,
    taker_buy_base_vol    DOUBLE PRECISION,
    taker_buy_quote_vol   DOUBLE PRECISION,
    open_interest         DOUBLE PRECISION,
    funding_rate          DOUBLE PRECISION,
    long_short_ratio      DOUBLE PRECISION,
    trade_delta           DOUBLE PRECISION,
    long_liqs             DOUBLE PRECISION,
    short_liqs            DOUBLE PRECISION,
    last_bid_depth        DOUBLE PRECISION,
    last_ask_depth        DOUBLE PRECISION,
    last_depth_imbalance  DOUBLE PRECISION,
    last_spread_pct       DOUBLE PRECISION,
    spread_pct            DOUBLE PRECISION,
    bid_ask_imbalance     DOUBLE PRECISION,
    basis_pct             DOUBLE PRECISION,
    market_cap_usd        DOUBLE PRECISION,
    market_cap_rank       INTEGER,
    PRIMARY KEY (timestamp_utc, symbol)
);

SELECT create_hypertable('market_data_1m', 'timestamp_utc', if_not_exists => TRUE);

ALTER TABLE market_data_1m SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby   = 'timestamp_utc DESC'
);

SELECT add_compression_policy('market_data_1m', INTERVAL '7 days', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_market_data_symbol_ts
    ON market_data_1m (symbol, timestamp_utc DESC);

CREATE TABLE IF NOT EXISTS marketcap_daily (
    date             DATE    NOT NULL,
    symbol           TEXT    NOT NULL,
    coin_id          TEXT,
    market_cap_usd   DOUBLE PRECISION,
    market_cap_rank  INTEGER,
    price_usd        DOUBLE PRECISION,
    volume_usd       DOUBLE PRECISION,
    PRIMARY KEY (date, symbol)
);
SQL
    success "TimescaleDB schema applied"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 7. Python venv + pipeline dependencies
# ─────────────────────────────────────────────────────────────────────────────
if [[ "$SSL_ONLY" == false ]]; then
    log "Setting up Python virtual environment..."
    python3 -m venv "$VENV_DIR"
    "$VENV_DIR/bin/pip" install --upgrade pip -q
    "$VENV_DIR/bin/pip" install -r "$PIPELINE_DIR/requirements.txt" -q
    # awscli installed system-wide (needed outside venv for storage scripts)
    pip install awscli --break-system-packages -q 2>/dev/null || true
    success "Python venv ready at $VENV_DIR"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 8. quant-data directory structure
# ─────────────────────────────────────────────────────────────────────────────
if [[ "$SSL_ONLY" == false ]]; then
    log "Initialising quant-data directory structure..."
    DATA_ROOT="$DATA_ROOT" "$VENV_DIR/bin/python" "$PIPELINE_DIR/config.py"
    success "Directory structure created"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 9. Credentials
# ─────────────────────────────────────────────────────────────────────────────
if [[ "$SSL_ONLY" == false ]]; then
    log "Writing credentials..."
    CREDS_DIR="$DATA_ROOT/credentials"
    mkdir -p "$CREDS_DIR"

    cat > "$CREDS_DIR/secrets.env" << EOF
AMBER_API_KEY=${AMBER_API_KEY}
COINGECKO_API_KEY=${COINGECKO_API_KEY}
DB_PASSWORD=${DB_PASSWORD}
DB_HOST=${DB_HOST:-127.0.0.1}
DB_NAME=${DB_NAME}
DB_USER=${DB_USER}
DATA_ROOT=${DATA_ROOT}
S3_ACCESS_KEY=${S3_ACCESS_KEY:-}
S3_SECRET_KEY=${S3_SECRET_KEY:-}
S3_BUCKET=${S3_BUCKET:-benji3m-data}
S3_ENDPOINT=${S3_ENDPOINT:-https://hel1.your-objectstorage.com}
EOF
    chmod 600 "$CREDS_DIR/secrets.env"

    success "Credentials written to $CREDS_DIR"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 9b. Download seed data from Object Storage + run backfill
# ─────────────────────────────────────────────────────────────────────────────
if [[ "$SSL_ONLY" == false && -n "${S3_ACCESS_KEY:-}" && -n "${S3_SECRET_KEY:-}" ]]; then
    log "Downloading historical seed data from Object Storage..."
    bash "$REPO_DIR/download-from-storage.sh"         && success "Seed data downloaded"         || warn "Seed data download failed — run manually: bash $REPO_DIR/download-from-storage.sh"

    log "Running backfill scripts..."
    bash "$REPO_DIR/run_backfill.sh"         && success "Backfill complete"         || warn "Backfill failed — run manually: bash $REPO_DIR/run_backfill.sh"
else
    if [[ "$SSL_ONLY" == false ]]; then
        warn "S3_ACCESS_KEY/S3_SECRET_KEY not set — skipping seed data download"
        warn "To download manually: add S3 credentials to .env, then run: bash $REPO_DIR/download-from-storage.sh"
    fi
fi

# ─────────────────────────────────────────────────────────────────────────────
# 10. nginx config
# ─────────────────────────────────────────────────────────────────────────────
log "Writing nginx config..."

if [[ "$NO_DOMAIN" == true ]]; then
    # HTTP-only: no SSL, no redirect. Works immediately on any IP.
    cat > "$REPO_DIR/nginx.conf" << 'NGINX'
server {
    listen 80;
    server_name _;

    client_max_body_size 100M;

    location /api/ {
        proxy_pass http://backend:8000/api/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 300s;
    }

    location /health {
        proxy_pass http://backend:8000/health;
    }

    location / {
        proxy_pass http://frontend:3000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
NGINX
    success "HTTP-only nginx config written"

else
    # Full SSL config
    cat > "$REPO_DIR/nginx.conf" << NGINX
server {
    listen 80;
    server_name ${DOMAIN} www.${DOMAIN};

    location /.well-known/acme-challenge/ {
        root /var/www/certbot;
    }

    location / {
        return 301 https://\$host\$request_uri;
    }
}

server {
    listen 443 ssl;
    server_name ${DOMAIN} www.${DOMAIN};

    ssl_certificate /etc/letsencrypt/live/${DOMAIN}/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/${DOMAIN}/privkey.pem;

    client_max_body_size 100M;

    location /api/ {
        proxy_pass http://backend:8000/api/;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_read_timeout 300s;
    }

    location /health {
        proxy_pass http://backend:8000/health;
    }

    location / {
        proxy_pass http://frontend:3000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
NGINX
    success "SSL nginx config written for $DOMAIN"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 11. Certbot SSL
# Runs automatically when DOMAIN is set in .env and cert doesn't exist yet.
# Re-running setup.sh after adding DOMAIN to .env is all that's needed —
# no --ssl-only flag required.
# ─────────────────────────────────────────────────────────────────────────────
CERT_PATH="/etc/letsencrypt/live/${DOMAIN}/fullchain.pem"

if [[ "$NO_DOMAIN" == false ]]; then
    if [[ -f "$CERT_PATH" ]]; then
        success "SSL certificate already exists for $DOMAIN — skipping Certbot"
        # Ensure compose file is patched even if cert already existed
        sed -i "s|      - certbot_www:/var/www/certbot:ro|      - /var/www/certbot:/var/www/certbot:ro|g" "$REPO_DIR/docker-compose.yml"
        sed -i "s|      - certbot_certs:/etc/letsencrypt:ro|      - /etc/letsencrypt:/etc/letsencrypt:ro|g" "$REPO_DIR/docker-compose.yml"
    else
        log "Obtaining SSL certificate for $DOMAIN..."

        # Stop nginx temporarily so Certbot can bind port 80
        docker compose -f "$REPO_DIR/docker-compose.yml" stop nginx 2>/dev/null || true

        certbot certonly \
            --standalone \
            --non-interactive \
            --agree-tos \
            --email "$SSL_EMAIL" \
            -d "$DOMAIN" \
            -d "www.$DOMAIN"

        success "SSL certificate obtained for $DOMAIN"

        # Patch docker-compose.yml certbot volumes to use host paths
        # (must happen before nginx restarts so it can find the certs)
        sed -i "s|      - certbot_www:/var/www/certbot:ro|      - /var/www/certbot:/var/www/certbot:ro|g" "$REPO_DIR/docker-compose.yml"
        sed -i "s|      - certbot_certs:/etc/letsencrypt:ro|      - /etc/letsencrypt:/etc/letsencrypt:ro|g" "$REPO_DIR/docker-compose.yml"
    fi

    # Force-recreate nginx so it picks up the patched volumes and cert files
    log "Restarting nginx with SSL certificate..."
    docker compose -f "$REPO_DIR/docker-compose.yml" up -d --force-recreate nginx
    sleep 3
    docker ps --filter name=nginx --format "{{.Names}}  →  {{.Status}}" | grep -q "Up" \
        && success "nginx restarted with SSL" \
        || warn "nginx may still be starting — check: docker logs \$(docker ps -qf name=nginx)"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 12. .env.production — updated for new filesystem layout
# ─────────────────────────────────────────────────────────────────────────────
if [[ "$SSL_ONLY" == false ]]; then
    log "Writing .env.production..."
    cat > "$REPO_DIR/.env.production" << EOF
# Generated by setup.sh — do not edit manually.
# Data paths — volume mounted as /data inside containers via docker-compose.yml.
BASE_DATA_DIR=/data
PARQUET_PATH=/data/compiled
MARKETCAP_PATH=/data/raw/coingecko/marketcap_daily.parquet
LEADERBOARDS_DIR=/data/leaderboards

# Pipeline
JOBS_DIR=/app/backend/jobs
PIPELINE_DIR=/app/pipeline
PIPELINE_PYTHON=python
NODE_BIN=node

# Redis
REDIS_URL=redis://redis:6379/0

# Database — host-gateway resolves to host machine from inside Docker
DB_HOST=host-gateway
DB_PORT=5432
DB_NAME=${DB_NAME}
DB_USER=${DB_USER}
DB_PASSWORD=${DB_PASSWORD}

# CORS
CORS_ORIGINS=*

# API keys
AMBER_API_KEY=${AMBER_API_KEY}
COINGECKO_API_KEY=${COINGECKO_API_KEY}
EOF
    success ".env.production written"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 13. docker-compose.yml — patch volume mount to use DATA_ROOT
# ─────────────────────────────────────────────────────────────────────────────
if [[ "$SSL_ONLY" == false ]]; then
    log "Patching docker-compose.yml volume mounts..."
    # Patch data volume: /data → actual volume path
    sed -i "s|      - /data:/data|      - ${DATA_ROOT}:/data|g" "$REPO_DIR/docker-compose.yml"
    # Patch certbot volumes: replace Docker-managed volumes with host paths
    # so nginx can read certificates written by certbot on the host
    sed -i "s|      - certbot_www:/var/www/certbot:ro|      - /var/www/certbot:/var/www/certbot:ro|g" "$REPO_DIR/docker-compose.yml"
    sed -i "s|      - certbot_certs:/etc/letsencrypt:ro|      - /etc/letsencrypt:/etc/letsencrypt:ro|g" "$REPO_DIR/docker-compose.yml"
    success "Volume mounts patched"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 14. App stack
# ─────────────────────────────────────────────────────────────────────────────
log "Building and starting app stack (this may take a few minutes)..."
cd "$REPO_DIR"

if [[ "$NO_DOMAIN" == true ]]; then
    # In HTTP-only mode, certbot volumes don't exist yet so we exclude them
    # by overriding with a minimal compose override that removes cert mounts.
    # Simpler approach: bring everything up — nginx will serve HTTP fine
    # since the config no longer references cert paths.
    docker compose up -d --build
else
    docker compose up -d --build
fi

success "App stack running"

# ─────────────────────────────────────────────────────────────────────────────
# 15. Pipeline cron jobs
# ─────────────────────────────────────────────────────────────────────────────
# Use --skip-crons on first provision to defer cron installation until after
# the backfill completes. This prevents metl.py firing at 00:15 UTC and
# competing for memory with the backfill scripts.
# After backfill is verified: run setup.sh again without --skip-crons.
if [[ "$SSL_ONLY" == false && "$SKIP_CRONS" == false ]]; then
    log "Installing pipeline cron jobs..."
    PYTHON="$VENV_DIR/bin/python"
    SECRETS="$DATA_ROOT/credentials/secrets.env"
    LOG="$DATA_ROOT/logs"

    CERTBOT_CRON=""
    if [[ "$NO_DOMAIN" == false ]]; then
        CERTBOT_CRON="0 3 * * * certbot renew --quiet --deploy-hook 'docker compose -f $REPO_DIR/docker-compose.yml restart nginx'"
    fi

    crontab - << EOF
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

# ── Amberdata ETL — 00:15 UTC ─────────────────────────────────────────────────
15 0 * * * . $SECRETS && $PYTHON $PIPELINE_DIR/compiler/metl.py --start \$(date -d "yesterday" +\%Y-\%m-\%d) >> $LOG/amberdata/cron.log 2>&1

# ── CoinGecko daily snapshot — 00:30 UTC ──────────────────────────────────────
30 0 * * * . $SECRETS && $PYTHON $PIPELINE_DIR/compiler/coingecko_marketcap.py --api-key \$COINGECKO_API_KEY --mode daily >> $LOG/coingecko/cron.log 2>&1

# ── Indexer — 01:00 UTC (rebuild leaderboard parquets from DB for yesterday) ──
0  1 * * * . $SECRETS && $PYTHON $PIPELINE_DIR/indexer/build_intraday_leaderboard.py --source db --metric price --start \$(date -d "yesterday" +\%Y-\%m-\%d) --end \$(date -d "yesterday" +\%Y-\%m-\%d) >> $LOG/indexer/cron.log 2>&1
5  1 * * * . $SECRETS && $PYTHON $PIPELINE_DIR/indexer/build_intraday_leaderboard.py --source db --metric open_interest --start \$(date -d "yesterday" +\%Y-\%m-\%d) --end \$(date -d "yesterday" +\%Y-\%m-\%d) >> $LOG/indexer/cron.log 2>&1
10 1 * * * . $SECRETS && $PYTHON $PIPELINE_DIR/indexer/build_intraday_leaderboard.py --source db --metric volume --start \$(date -d "yesterday" +\%Y-\%m-\%d) --end \$(date -d "yesterday" +\%Y-\%m-\%d) >> $LOG/indexer/cron.log 2>&1

# ── Overlap / deploys regeneration — 01:20 UTC (after indexer finishes) ───────
20 1 * * * . $SECRETS && $PYTHON $PIPELINE_DIR/overlap_analysis.py --leaderboard-index 100 --freq-width 20 --freq-cutoff 20 --mode snapshot --deployment-start-hour 6 --sort-lookback 6 --min-mcap 0 >> $LOG/overlap/cron.log 2>&1

${CERTBOT_CRON}

# ── Object Storage sync — 02:00 UTC (after compiler + indexer finish) ─────────
$([ -n "${S3_ACCESS_KEY:-}" ] && echo "0 2 * * * cd $REPO_DIR && bash sync-to-storage.sh >> $LOG/sync-storage.log 2>&1")
EOF

    success "Cron jobs installed"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 16. Post-install verification
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "${BOLD}  Verification${RESET}"
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo ""

SERVER_IP=$(curl -s ifconfig.me 2>/dev/null || curl -s api.ipify.org 2>/dev/null || hostname -I | awk '{print $1}')

echo "  Containers:"
docker ps --format "    {{.Names}}  →  {{.Status}}" \
    | grep -E "timescaledb|backend|celery|frontend|nginx|redis" || true
echo ""

docker exec timescaledb psql -U "$DB_USER" -d "$DB_NAME" \
    -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_name IN ('market_data_1m','marketcap_daily');" \
    2>/dev/null | grep -q "2" \
    && success "TimescaleDB: 2 tables present" \
    || warn "TimescaleDB schema check failed — run: docker exec timescaledb psql -U $DB_USER -d $DB_NAME -c '\dt'"

mountpoint -q "$DATA_ROOT" \
    && success "Volume: mounted at $DATA_ROOT ($(df -h $DATA_ROOT | tail -1 | awk '{print $4}') free)" \
    || warn "Volume: not mounted — data is on local disk"

"$VENV_DIR/bin/python" -c "import pandas, pyarrow, requests, psycopg2" 2>/dev/null \
    && success "Python: all pipeline dependencies installed" \
    || warn "Python: dependency check failed — run: $VENV_DIR/bin/pip install -r $PIPELINE_DIR/requirements.txt"

crontab -l 2>/dev/null | grep -q "metl.py" \
    && success "Cron: pipeline jobs active" \
    || warn "Cron: pipeline jobs not found"

sleep 3
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:80" 2>/dev/null || echo "000")
[[ "$HTTP_CODE" =~ ^(200|301|302)$ ]] \
    && success "nginx: responding (HTTP $HTTP_CODE)" \
    || warn "nginx: not responding on port 80 (got $HTTP_CODE) — check: docker logs \$(docker ps -qf name=nginx)"

echo ""
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo -e "${GREEN}${BOLD}  Setup complete${RESET}"
echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
echo ""

if [[ "$NO_DOMAIN" == true ]]; then
    echo -e "  ${BOLD}App:${RESET}  http://${SERVER_IP}"
    echo ""
    echo "  To add SSL when DNS is ready:"
    echo "    1. Point DNS A records for your domain to $SERVER_IPV4"
    echo "    2. Add to .env:  DOMAIN=yourdomain.com"
    echo "                     SSL_EMAIL=you@email.com"
    echo "    3. Re-run:       bash setup.sh"
else
    echo -e "  ${BOLD}App:${RESET}  https://${DOMAIN}"
fi

echo ""
echo "  Remaining steps:"
echo "  1. Run a historical data backfill:"
echo "     $VENV_DIR/bin/python $PIPELINE_DIR/compiler/binance_downloader.py \\"
echo "         --market futures --symbols 500 --years 2"
echo ""
echo "  2. Watch the first nightly pipeline run (00:15 UTC):"
echo "     tail -f $DATA_ROOT/logs/amberdata/cron.log"
echo ""
