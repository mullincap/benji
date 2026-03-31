import json
import shutil
import time
from pathlib import Path
from typing import Any

from app.core.config import settings


def _job_dir(job_id: str) -> Path:
    return Path(settings.JOBS_DIR) / job_id


def _job_file(job_id: str) -> Path:
    return _job_dir(job_id) / "job.json"


def create_job(job_id: str, params: dict[str, Any]) -> dict[str, Any]:
    job_dir = _job_dir(job_id)
    job_dir.mkdir(parents=True, exist_ok=True)
    job: dict[str, Any] = {
        "id": job_id,
        "status": "queued",
        "stage": None,
        "progress": 0,
        "params": params,
        "results": None,
        "error": None,
        "created_at": time.time(),
        "updated_at": time.time(),
    }
    _job_file(job_id).write_text(json.dumps(job, indent=2))
    return job


def get_job(job_id: str) -> dict[str, Any] | None:
    path = _job_file(job_id)
    if not path.exists():
        return None
    return json.loads(path.read_text())


def update_job(job_id: str, **fields: Any) -> dict[str, Any]:
    job = get_job(job_id) or {}
    job.update(fields)
    job["updated_at"] = time.time()
    _job_file(job_id).write_text(json.dumps(job, indent=2))
    return job


def list_jobs() -> list[dict[str, Any]]:
    jobs_dir = Path(settings.JOBS_DIR)
    if not jobs_dir.exists():
        return []
    jobs = []
    for path in sorted(jobs_dir.glob("*/job.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        try:
            jobs.append(json.loads(path.read_text()))
        except (json.JSONDecodeError, OSError):
            continue
    return jobs


def delete_job(job_id: str) -> bool:
    job_dir = _job_dir(job_id)
    if not job_dir.exists():
        return False
    shutil.rmtree(job_dir)
    return True
