#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export STOCKBOT_DB_URL="${STOCKBOT_DB_URL:-jdbc:postgresql://localhost:5432/stockbot}"
export STOCKBOT_DB_USER="${STOCKBOT_DB_USER:-stockbot}"
export STOCKBOT_DB_PASS="${STOCKBOT_DB_PASS:-stockbot}"

bash "$ROOT_DIR/scripts/db_up.sh"

if [[ ! -x "$ROOT_DIR/mvnw" ]]; then
  chmod +x "$ROOT_DIR/mvnw"
fi

./mvnw -q -DskipTests package

WATCHLIST_FILE="$ROOT_DIR/watchlist_smoke.txt"
CONFIG_FILE="$ROOT_DIR/config.properties"
BACKUP_FILE="$ROOT_DIR/.config.properties.smoke.bak"

cleanup() {
  rm -f "$WATCHLIST_FILE"
  if [[ -f "$BACKUP_FILE" ]]; then
    mv "$BACKUP_FILE" "$CONFIG_FILE"
  else
    rm -f "$CONFIG_FILE"
  fi
}
trap cleanup EXIT

if [[ -f "$CONFIG_FILE" ]]; then
  cp "$CONFIG_FILE" "$BACKUP_FILE"
fi

cat > "$WATCHLIST_FILE" <<'WL'
7203.T
6758.T
9984.T
WL

cat > "$CONFIG_FILE" <<'CFG'
app.mode=DAILY
app.schedule.enabled=false
email.enabled=false
watchlist.path=watchlist_smoke.txt
scan.threads=1
scan.max_universe_size=3
scan.top_n=3
scan.batch.enabled=false
db.url=jdbc:postgresql://localhost:5432/stockbot
db.user=stockbot
db.pass=stockbot
db.schema=stockbot
CFG

JAR_PATH="$(ls target/stockbot-*.jar | grep -v 'original' | head -n 1)"
if [[ -z "$JAR_PATH" ]]; then
  echo "[smoke_test] ERROR: jar not found in target/" >&2
  exit 1
fi

echo "[smoke_test] running minimal DAILY flow"
java -jar "$JAR_PATH"

signals_count="$(docker compose exec -T postgres psql -U stockbot -d stockbot -tAc "SELECT COUNT(*) FROM stockbot.signals;")"
run_logs_count="$(docker compose exec -T postgres psql -U stockbot -d stockbot -tAc "SELECT COUNT(*) FROM stockbot.run_logs;")"

signals_count="$(echo "$signals_count" | tr -d '[:space:]')"
run_logs_count="$(echo "$run_logs_count" | tr -d '[:space:]')"

if [[ -z "$signals_count" || "$signals_count" -lt 1 ]]; then
  echo "[smoke_test] ERROR: expected stockbot.signals >= 1, got '${signals_count}'" >&2
  exit 1
fi

if [[ -z "$run_logs_count" || "$run_logs_count" -lt 1 ]]; then
  echo "[smoke_test] ERROR: expected stockbot.run_logs >= 1, got '${run_logs_count}'" >&2
  exit 1
fi

echo "[smoke_test] PASS signals=${signals_count} run_logs=${run_logs_count}"
