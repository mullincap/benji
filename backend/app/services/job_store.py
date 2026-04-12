import json
import shutil
import time
import uuid
from pathlib import Path
from typing import Any

from app.core.config import settings


# ─── Folders ──────────────────────────────────────────────────────────────────

def _folders_file() -> Path:
    return Path(settings.JOBS_DIR) / "folders.json"


def _load_folders() -> list[dict[str, Any]]:
    path = _folders_file()
    if not path.exists():
        return []
    try:
        return json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return []


def _save_folders(folders: list[dict[str, Any]]) -> None:
    path = _folders_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(folders, indent=2))


def list_folders() -> list[dict[str, Any]]:
    return _load_folders()


def create_folder(name: str) -> dict[str, Any]:
    folders = _load_folders()
    folder = {
        "id": str(uuid.uuid4()),
        "name": name.strip(),
        "created_at": time.time(),
        "position": len(folders),
    }
    folders.append(folder)
    _save_folders(folders)
    return folder


def rename_folder(folder_id: str, name: str) -> dict[str, Any] | None:
    folders = _load_folders()
    for f in folders:
        if f["id"] == folder_id:
            f["name"] = name.strip()
            _save_folders(folders)
            return f
    return None


def delete_folder(folder_id: str) -> bool:
    folders = _load_folders()
    new = [f for f in folders if f["id"] != folder_id]
    if len(new) == len(folders):
        return False
    _save_folders(new)
    # Unset folder_id on any jobs in this folder
    for job in list_jobs():
        if job.get("folder_id") == folder_id:
            update_job(job["id"], folder_id=None)
    return True


# ─── Jobs ─────────────────────────────────────────────────────────────────────

def _job_dir(job_id: str) -> Path:
    return Path(settings.JOBS_DIR) / job_id


def _job_file(job_id: str) -> Path:
    return _job_dir(job_id) / "job.json"


def create_job(job_id: str, params: dict[str, Any]) -> dict[str, Any]:
    job_dir = _job_dir(job_id)
    job_dir.mkdir(parents=True, exist_ok=True)
    job: dict[str, Any] = {
        "id": job_id,
        "display_name": None,
        "folder_id": None,
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
