# Deferred work

Items surfaced during normal work that aren't in scope for the current track but shouldn't be lost. Add to top; keep entries terse with enough detail to act on cold.

---

## Environment drift: BASE_DATA_DIR inconsistency between services

**Discovered:** 2026-04-18 during Track 1 Part 10 smoke test.

**Problem:**
- `.env.production` has legacy `BASE_DATA_DIR=/data` (pre-`/mnt/quant-data` migration)
- `docker-compose.yml` celery service explicitly overrides: `BASE_DATA_DIR=/mnt/quant-data`
- `backend` service does NOT override → inherits `/data` from `env_file`
- Pre-stage parquet freshness check against `/data/leaderboards/…` finds nothing → triggers 3-hour full parquet rebuild

**Immediate mitigation (in place):**
- Nightly refresh cron targets the `celery` service (correct env), not `backend`.

**Durable fix (deferred):**
- Option A: update `.env.production` `BASE_DATA_DIR` / `PARQUET_PATH` / `MARKETCAP_DIR` to `/mnt/quant-data`, remove the celery service override in `docker-compose.yml`
- Option B: add explicit `BASE_DATA_DIR=/mnt/quant-data` override to the backend service in `docker-compose.yml`
- Either option aligns all services; Option A is cleaner.

**Blast radius if not fixed:**
- Any future CLI run against the `backend` service that depends on parquet paths will trigger the 3-hour rebuild
- Manual debugging sessions against the backend container will fail silently or slowly
- Only affects deferred maintenance — no active blocker

**Tracking:** carry on open work list, address when next touching env configuration.
