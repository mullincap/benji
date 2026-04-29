#!/usr/bin/env python3
"""
Diff audit.py env-var defaults vs pipeline_runner.py params.get fallbacks.

Background (2026-04-29 dispersion_universe_mode bug):
  - audit.py read DISPERSION_UNIVERSE_MODE with default 'all'
  - pipeline_runner passed it from params.get('dispersion_universe_mode', 'curated')
  - When the JobRequest didn't include the field, pipeline_runner sent
    DISPERSION_UNIVERSE_MODE='curated' to the audit subprocess, silently
    overriding audit's own 'all' default.

This script flags every other place where the same pattern exists.
Read-only; just prints a report.
"""
import re
import sys
from pathlib import Path

AUDIT_PY  = "/Users/johnmullin/Projects/benji3m/pipeline/audit.py"
RUNNER_PY = "/Users/johnmullin/Projects/benji3m/backend/app/services/audit/pipeline_runner.py"

# Patterns
RX_AUDIT_ENV_GET = re.compile(
    r'os\.environ\.get\(\s*"([A-Z_]+)"\s*,\s*"?([^",\)]+?)"?\s*\)'
)
RX_AUDIT_BOOL = re.compile(
    r'_env_bool\(\s*"([A-Z_]+)"\s*,\s*(True|False|1|0)\s*\)'
)
RX_AUDIT_BOOL_EQ_1 = re.compile(  # `os.environ.get("X", "0") == "1"` form
    r'os\.environ\.get\(\s*"([A-Z_]+)"\s*,\s*"(0|1)"\s*\)\s*==\s*"1"'
)

# pipeline_runner: lines like
#   "ENV_NAME": str(params.get("key", default)),
#   "ENV_NAME": _boolenv(params.get("key", default)),
RX_RUNNER = re.compile(
    r'"([A-Z_]+)"\s*:\s*(?:str|_boolenv|int|float)?\s*\(?\s*'
    r'params\.get\(\s*"([a-z_0-9]+)"\s*,\s*([^)]+?)\)\)?\s*,'
)


def parse_audit_defaults() -> dict[str, tuple[str, str]]:
    """Returns {ENV_NAME: (default_repr, kind)} where kind ∈ {'val','bool'}."""
    out: dict[str, tuple[str, str]] = {}
    text = Path(AUDIT_PY).read_text()
    for m in RX_AUDIT_ENV_GET.finditer(text):
        env, default = m.group(1), m.group(2).strip()
        out.setdefault(env, (default, "val"))
    for m in RX_AUDIT_BOOL.finditer(text):
        env, default = m.group(1), m.group(2)
        normalized = "True" if default in ("True", "1") else "False"
        out.setdefault(env, (normalized, "bool"))
    for m in RX_AUDIT_BOOL_EQ_1.finditer(text):
        env, default = m.group(1), m.group(2)
        normalized = "True" if default == "1" else "False"
        out.setdefault(env, (normalized, "bool"))
    return out


def parse_runner_mappings() -> list[tuple[str, str, str]]:
    """Returns list of (ENV_NAME, params_key, default_repr)."""
    out: list[tuple[str, str, str]] = []
    text = Path(RUNNER_PY).read_text()
    for m in RX_RUNNER.finditer(text):
        env, key, default = m.group(1), m.group(2), m.group(3).strip()
        out.append((env, key, default))
    return out


def normalize(s: str) -> str:
    """Loose comparison: strip quotes, lowercase True/False, drop trailing commas."""
    s = s.strip().rstrip(",").strip()
    s = s.strip("'\"")
    # bool variants
    if s in ("True", "true"):  return "True"
    if s in ("False", "false"): return "False"
    # numeric variants — leave as-is for str compare; numbers like "0.04" vs "0.04" match
    return s


def main() -> int:
    audit_defs = parse_audit_defaults()
    runner_maps = parse_runner_mappings()
    print(f"audit.py env defaults parsed:           {len(audit_defs)}")
    print(f"pipeline_runner params mappings parsed: {len(runner_maps)}")
    print()

    mismatches = []
    runner_only = []
    audit_only_envs = set(audit_defs)
    for env, key, runner_default in runner_maps:
        if env not in audit_defs:
            runner_only.append((env, key, runner_default))
            continue
        audit_only_envs.discard(env)
        a_def, kind = audit_defs[env]
        if normalize(a_def) != normalize(runner_default):
            mismatches.append((env, key, a_def, runner_default, kind))

    if mismatches:
        print("=" * 92)
        print("  ⚠ DEFAULT MISMATCHES — pipeline_runner overrides audit.py default")
        print("=" * 92)
        print(f"  {'ENV':<38} {'audit_default':<20} {'runner_default':<20} kind")
        print(f"  {'-'*38} {'-'*20} {'-'*20} {'-'*4}")
        for env, key, a_def, r_def, kind in mismatches:
            print(f"  {env:<38} {a_def!r:<20} {r_def!r:<20} {kind}")
        print()
    else:
        print("✓ no default mismatches between audit.py and pipeline_runner.py")
        print()

    if runner_only:
        print("=" * 92)
        print("  Runner-only envs (no matching audit.py env_get; usually safe)")
        print("=" * 92)
        for env, key, default in runner_only:
            print(f"  {env:<38} key={key!r}  default={default!r}")
        print()

    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
