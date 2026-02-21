#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DB_SERVICE="${STOCKBOT_DB_SERVICE:-postgres}"
DB_USER="${STOCKBOT_DB_USER:-stockbot}"
DB_NAME="${STOCKBOT_DB_NAME:-stockbot}"

run_sql() {
  local sql_file="$1"
  echo "[db_init] applying ${sql_file}"
  docker compose exec -T "$DB_SERVICE" psql -v ON_ERROR_STOP=1 -U "$DB_USER" -d "$DB_NAME" < "$sql_file"
}

run_sql "$ROOT_DIR/db/001_init.sql"
run_sql "$ROOT_DIR/db/002_tables.sql"
run_sql "$ROOT_DIR/db/003_indexes.sql"

echo "[db_init] complete"
