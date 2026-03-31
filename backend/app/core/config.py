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

    class Config:
        env_file = str(_ENV_FILE)

settings = Settings()