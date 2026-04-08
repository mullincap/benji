import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (ROOT / path).read_text()


def _frontend_default_keys(frontend_page: str) -> list[str]:
    m = re.search(r"const DEFAULT_PARAMS:.*?= \{(.*?)\n\};", frontend_page, re.S)
    assert m, "Could not find DEFAULT_PARAMS in frontend/app/page.tsx"
    keys: list[str] = []
    for raw in m.group(1).splitlines():
        line = raw.strip()
        if not line or line.startswith("//"):
            continue
        km = re.match(r"([a-zA-Z0-9_]+)\s*:", line)
        if km:
            keys.append(km.group(1))
    return keys


def _worker_param_to_env(worker_py: str) -> dict[str, str]:
    pairs = re.findall(
        r"\"([A-Z0-9_]+)\"\s*:\s*[^\n]*params\.get\(\"([a-z0-9_]+)\"",
        worker_py,
    )
    return {param: env for env, param in pairs}


def _env_reads(*texts: str) -> set[str]:
    reads: set[str] = set()
    for txt in texts:
        reads |= set(re.findall(r'os\.environ\.get\("([A-Z0-9_]+)"', txt))
        reads |= set(re.findall(r'_env_bool\("([A-Z0-9_]+)"', txt))
    return reads


def test_no_config_panel_noops_in_pipeline():
    frontend_page = _read("frontend/app/page.tsx")
    worker_py = _read("backend/app/workers/pipeline_worker.py")
    audit_py = _read("pipeline/audit.py")
    overlap_py = _read("pipeline/overlap_analysis.py")
    rebuild_py = _read("pipeline/rebuild_portfolio_matrix.py")
    # Canonical 1098-line builder. The 414-line ancestor at
    # pipeline/build_intraday_leaderboard.py was deleted on 2026-04-08
    # because it was the OOM-killer the simulator was hitting (it lacked
    # both today's row-group input streaming and ParquetWriter output
    # streaming). The 1098-line successor lives under pipeline/indexer/.
    leaderboard_py = _read("pipeline/indexer/build_intraday_leaderboard.py")

    default_keys = _frontend_default_keys(frontend_page)
    param_to_env = _worker_param_to_env(worker_py)
    env_reads = _env_reads(audit_py, overlap_py, rebuild_py, leaderboard_py)

    missing: list[tuple[str, str]] = []
    for param in default_keys:
        env = param_to_env.get(param)
        if env and env not in env_reads:
            missing.append((param, env))

    assert not missing, (
        "Found config params forwarded by worker but not consumed in pipeline scripts: "
        + ", ".join(f"{p}->{e}" for p, e in missing)
    )
