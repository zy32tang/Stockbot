# JP All-Market Scanner

## Build

```powershell
./mvnw.cmd -DskipTests package
```

## Run modes

```powershell
# Config-driven schedule start (no args, DAILY mode)
java -jar target/stockbot-3.0.0.jar

# Reset batch checkpoint + background startup trigger checkpoint
java -jar target/stockbot-3.0.0.jar --reset-batch

# Send test email only (reuse latest market scan, no full-market rescan)
java -jar target/stockbot-3.0.0.jar --test

# Show help
java -jar target/stockbot-3.0.0.jar --help
```

## Windows Task Scheduler (recommended)

1. Open Task Scheduler and create a new task.
2. Trigger: daily at your preferred time.
3. Action: start program `java`.
4. Arguments: `-jar C:\workspace\stockbotjava\target\stockbot-3.0.0.jar`.
5. Start in: `C:\workspace\stockbotjava`.

## Configuration

All settings are in `src/main/resources/config.properties`.

Optional runtime override: create local `config.properties` at project root.
Local file values override defaults in resources.

Batch scanner keys:
- `scan.batch.enabled`: enable segmented scanning.
- `scan.batch.segment_by_market`: split by JPX market field.
- `scan.batch.market_chunk_size`: split large market segments into chunks (`0` = no chunk split).
- `scan.batch.resume_enabled`: persist checkpoint and resume next run.
- `scan.batch.max_segments_per_run`: process only N segments per run (`0` = all segments).
- `scan.batch.checkpoint_key`: metadata key for checkpoint row in SQLite.
- `scan.progress.log_every`: print progress + ETA every N completed tickers in a segment (`0` = only segment start/end logs).

Fast but controllable profile:
- `scan.cache.prefer_enabled=true`: use cache first when fresh enough.
- `scan.cache.fresh_days=2`: cache freshness threshold (calendar days).
- `scan.network.retry_when_cache_exists=false`: if cache exists, skip network retry (retry=0 path).
- `jpx.universe.request_timeout_sec=20`: JPX universe download timeout.
- `yahoo.max_bars_per_ticker=300`: cache/history window per ticker.
- `scan.threads=3`: lower concurrency to reduce rate-limit/timeout risk.
- `scan.batch.max_segments_per_run=0`: each scanner run processes full market segments.
- `scan.upsert.initial_days=300`: first bootstrap writes recent 300 bars.
- `scan.upsert.incremental_recent_days=10`: later runs upsert from `lastDate+1` with recent 10-day overlap.
- `scan.tradable.min_avg_volume_20=50000`: skip low-liquidity names before scoring.

Scheduling behavior:
- no CLI args in DAILY mode start a background scanner thread.
- background scanner checks startup timestamps in metadata and runs all-market scan when:
  - first startup (no previous startup record), or
  - startup gap from previous startup is >= 8 hours.
- while the process keeps running, background scanner also runs one full market scan every 8 hours.
- background scanner scan (when triggered) is a full scan from segment 1 (checkpoint reset before run).
- at configured times (`schedule.times`, e.g. `11:30,15:00`), scheduler runs watchlist analysis, merges latest all-market snapshot, then generates/sends one HTML email.
- if there is no fresh market-scan result for today, scheduler runs one market-scan cycle first, then builds the merged report.

## Outputs

- SQLite DB: `outputs/stockbot.db`
- Scanner logs (console + file): `outputs/log/stockbot.log`
- HTML reports: `outputs/reports/jp_daily_*.html`
- Run summary: printed to console after each run

Daily report/email order:
- all-market snapshot is prepared first by background thread.
- watchlist analysis is executed at report time and merged with all-market reference candidates (`scan.market_reference_top_n`, default `5`).
- trend charts are generated as attachments (not embedded in HTML), including buy-zone upper/lower and defense line.

Watchlist section fields:
- stock display format: `local name + code + (行业中文/IndustryEnglish)`.
- score/rating/risk/pct-change/AI trigger/news source are included.
- AI summary is shown when gate is triggered.

Resilience:
- price history fetch uses Yahoo and falls back to local cache.
