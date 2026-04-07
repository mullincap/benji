from pydantic_settings import BaseSettings
from pathlib import Path

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
    PIPELINE_PYTHON: str = "/opt/homebrew/Caskroom/miniforge/base/bin/python"

    # ─── TimescaleDB connection (used by compiler/indexer routers) ───────────
    # Local dev: requires an SSH tunnel forwarding remote 5432 to local DB_PORT.
    # Production: backend runs on the server and reaches localhost:5432 directly.
    DB_HOST: str = "127.0.0.1"
    DB_PORT: int = 5432
    DB_NAME: str = "marketdata"
    DB_USER: str = "quant"
    DB_PASSWORD: str = ""  # required for local dev — set in .env or environment

    class Config:
        env_file = str(_ENV_FILE)

settings = Settings()