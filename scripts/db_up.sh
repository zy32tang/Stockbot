#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DB_SERVICE="${STOCKBOT_DB_SERVICE:-postgres}"
MAX_WAIT_SEC="${STOCKBOT_DB_MAX_WAIT_SEC:-120}"

echo "[db_up] starting Docker service: ${DB_SERVICE}"
docker compose up -d "$DB_SERVICE"

container_id="$(docker compose ps -q "$DB_SERVICE")"
if [[ -z "$container_id" ]]; then
  echo "[db_up] ERROR: unable to find container id for service '${DB_SERVICE}'" >&2
  exit 1
fi

echo "[db_up] waiting for PostgreSQL healthcheck"
start_ts="$(date +%s)"
while true; do
  status="$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id" 2>/dev/null || true)"
  if [[ "$status" == "healthy" ]]; then
    echo "[db_up] service is healthy"
    break
  fi

  now_ts="$(date +%s)"
  elapsed="$((now_ts - start_ts))"
  if (( elapsed >= MAX_WAIT_SEC )); then
    echo "[db_up] ERROR: service did not become healthy within ${MAX_WAIT_SEC}s (status=${status})" >&2
    docker compose logs --tail=80 "$DB_SERVICE" >&2 || true
    exit 1
  fi

  sleep 2
done

bash "$ROOT_DIR/scripts/db_init.sh"

echo "[db_up] complete"
