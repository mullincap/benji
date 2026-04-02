# Benji3m — Project Context for Claude Code

## What this project is
A web application wrapping a Python quantitative trading risk audit pipeline.
The pipeline backtests a crypto trading strategy and produces institutional-grade analytics.
Target users: crypto fund managers and allocators.

## Tech stack
- Backend: FastAPI + Celery + Redis + flat JSON job store
- Frontend: Next.js + TypeScript + Tailwind CSS
- Pipeline: Pure Python CLI scripts (no framework)
- Report generation: Node.js (generate_audit_report.js)
- Python virtual env: .venv (venv) at ~/Projects/benji3m/.venv

## Rules
- Read only the files needed for the current task.
- Do not load full project context unless required.
- For UI work, read CLAUDE_FRONTEND.md
- For API/backend work, read CLAUDE_BACKEND.md.
- For pipeline/audit work, read CLAUDE_PIPELINE.md.
- Start a fresh session when switching domains.
- Avoid re-reading CLAUDE files if already loaded in this session.
- Prefer working from direct file inspection over loading project context files.