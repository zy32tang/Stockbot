# JP All-Market Scanner

## Build

```powershell
./mvnw.cmd -DskipTests package
```

## PostgreSQL + pgvector (Docker)

```bash
# Start DB container, wait for health, run db/001~003 init SQL
./scripts/db_up.sh

# Re-run SQL only (idempotent)
./scripts/db_init.sh
```

`docker-compose.yml` uses:
- image: `pgvector/pgvector:pg16`
- port: `5432:5432`
- db/user/pass: `stockbot/stockbot/stockbot`
- volume: `./.pgdata:/var/lib/postgresql/data`

Run full stack (DB + app):

```bash
# Build and start both services
docker compose up -d --build

# Follow app logs
docker compose logs -f app

# Stop services
docker compose down
```

Notes:
- app uses `STOCKBOT_DB_URL=jdbc:postgresql://postgres:5432/stockbot` inside Docker network.
- SQL in `db/001~003` is auto-applied by PostgreSQL only on first init of `./.pgdata`.

## Runtime DB config

Default DB backend is PostgreSQL. Startup will fail fast if DB is unreachable.

Priority order:
1. Environment variables
2. `config.properties` local override
3. `src/main/resources/config.properties` defaults

Supported env vars:
- `STOCKBOT_DB_URL` (example: `jdbc:postgresql://localhost:5432/stockbot`)
- `STOCKBOT_DB_USER`
- `STOCKBOT_DB_PASS`

Sample local override: `config.properties.sample`.

At startup, app prints:
- DB type (`POSTGRES`)
- JDBC URL (password masked)
- schema

## Run modes

```powershell
# Config-driven schedule start (no args, DAILY mode)
java -jar target/stockbot-3.0.0.jar

# One-off migrate old SQLite data into PostgreSQL
java -jar target/stockbot-3.0.0.jar --migrate-sqlite-to-postgres --sqlite-path outputs/stockbot.db

# Run with scheduler loop
java -jar target/stockbot-3.0.0.jar --daemon
```

## Smoke test

```bash
./scripts/smoke_test.sh
```

What it does:
- starts DB and initializes schema
- builds jar
- runs a minimal DAILY flow with 1-3 watchlist tickers
- verifies `stockbot.signals >= 1` and `stockbot.run_logs >= 1`

## SQLite -> PostgreSQL migration

Command:

```powershell
java -jar target/stockbot-3.0.0.jar --migrate-sqlite-to-postgres --sqlite-path <path-to-sqlite-db>
```

Migration output includes counts for:
- `watchlist_count`
- `price_daily_count`
- `signals_count`
- `run_logs_count`

## Common errors

- Port conflict on `5432`
  - Another PostgreSQL instance is using the port. Stop it or change compose mapping.
- Bad credentials / auth failed
  - Check `STOCKBOT_DB_USER`/`STOCKBOT_DB_PASS` and compose env.
- `extension "vector" does not exist`
  - Use `pgvector/pgvector:pg16` image and rerun `./scripts/db_init.sh`.
- DB connection refused
  - Run `./scripts/db_up.sh` and wait for healthy status.

## Outputs

- PostgreSQL DB data (docker volume): `.pgdata/`
- Scanner logs: `outputs/log/stockbot.log`
- HTML reports: `outputs/reports/jp_daily_*.html`
- Run summary: printed to console
