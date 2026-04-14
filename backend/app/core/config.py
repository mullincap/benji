import os
from pathlib import Path

from pydantic_settings import BaseSettings

# .env lives at the project root (parent of backend/)
_ENV_FILE = Path(__file__).resolve().parents[3] / ".env"

class Settings(BaseSettings):
    # Data paths — override in .env
    BASE_DATA_DIR: Path = Path.home() / "Projects/benji3m/pipeline"
    PARQUET_PATH: str = ""
    MARKETCAP_DIR: str = ""

    # Job storage
    JOBS_DIR: Path = Path.home() / "Projects/benji3m/backend/jobs"

    # Pipeline script paths
    PIPELINE_DIR: Path = Path.home() / "Projects/benji3m/pipeline"

    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"

    # Node binary (for report generation)
    NODE_BIN: str = "node"

    # Python binary used to run pipeline scripts (needs pandas, pyarrow, etc.)
    # Override in .env for local dev (e.g. /opt/homebrew/Caskroom/miniforge/base/bin/python)
    PIPELINE_PYTHON: str = "python"

    # ─── TimescaleDB connection (used by compiler/indexer routers) ───────────
    # Local dev: requires an SSH tunnel forwarding remote 5432 to local DB_PORT.
    # Production: backend runs on the server and reaches localhost:5432 directly.
    DB_HOST: str = "127.0.0.1"
    DB_PORT: int = 5432
    DB_NAME: str = "marketdata"
    DB_USER: str = "quant"
    DB_PASSWORD: str = ""  # required for local dev — set in .env or environment

    # ─── Admin auth (compiler + indexer + manager admin pages) ────────────────
    # ADMIN_PASSPHRASE: shared secret the admin types into the login form.
    # ADMIN_SESSIONS_FILE: flat JSON file backing the random-token session
    # store (see backend/app/services/admin_sessions.py). Defaults to
    # backend/data/admin_sessions.json relative to this file's grandparent
    # (the backend/ directory). The file is git-ignored.
    ADMIN_PASSPHRASE: str = ""  # empty = login disabled (returns 503)
    ADMIN_SESSIONS_FILE: Path = Path(__file__).resolve().parents[2] / "data" / "admin_sessions.json"

    # ─── User auth (allocator per-user sessions) ─────────────────────────────
    # Sessions are now DB-backed (user_mgmt.user_sessions table).
    # USER_SESSIONS_FILE is kept as a no-op placeholder — auth.py still passes
    # it to the session functions for signature compatibility, but the value is
    # never read by user_sessions.py.
    USER_SESSIONS_FILE: Path = Path("/dev/null")

    # ─── Fernet encryption key for exchange API keys at rest ────────────────
    # Generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    # REQUIRED — losing this key makes all stored exchange keys unrecoverable.
    FERNET_KEY: str = ""

    # ─── Internal API token (server-to-server, e.g. briefing cron) ──────────
    INTERNAL_API_TOKEN: str = ""

    # ─── Anthropic API (manager chat) ───────────────────────────────────────
    ANTHROPIC_API_KEY: str = ""

    # ─── Server-side secrets.env path ───────────────────────────────────────
    SECRETS_PATH: str = "/mnt/quant-data/credentials/secrets.env"

    # ─── Claude model config ────────────────────────────────────────────────
    CLAUDE_MODEL: str = "claude-sonnet-4-20250514"
    CLAUDE_MAX_TOKENS: int = 1000

    class Config:
        env_file = str(_ENV_FILE)

settings = Settings()


def load_secrets() -> None:
    """Load key=value pairs from the server-side secrets.env into os.environ.

    Safe to call multiple times — uses setdefault so existing env vars win.
    No-op if the secrets file doesn't exist (e.g. local dev).
    """
    secrets_path = Path(settings.SECRETS_PATH)
    if not secrets_path.exists():
        return
    for line in secrets_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())