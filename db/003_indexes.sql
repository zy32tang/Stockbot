CREATE UNIQUE INDEX IF NOT EXISTS idx_universe_code ON stockbot.universe (code);
CREATE INDEX IF NOT EXISTS idx_universe_active ON stockbot.universe (active);

CREATE INDEX IF NOT EXISTS idx_price_daily_ticker_date ON stockbot.price_daily (ticker, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_indicators_daily_ticker_date ON stockbot.indicators_daily (ticker, trade_date DESC);

CREATE INDEX IF NOT EXISTS idx_signals_run_ticker ON stockbot.signals (run_id, ticker);
CREATE INDEX IF NOT EXISTS idx_signals_asof ON stockbot.signals (as_of DESC);

CREATE INDEX IF NOT EXISTS idx_run_logs_run_id ON stockbot.run_logs (run_id);
CREATE INDEX IF NOT EXISTS idx_run_logs_started ON stockbot.run_logs (started_at DESC);

CREATE INDEX IF NOT EXISTS docs_ticker_published_idx ON stockbot.docs (ticker, published_at DESC);
CREATE INDEX IF NOT EXISTS docs_embedding_ivfflat ON stockbot.docs USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

CREATE INDEX IF NOT EXISTS idx_runs_started ON stockbot.runs (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_candidates_run_rank ON stockbot.candidates (run_id, rank_no);
CREATE INDEX IF NOT EXISTS idx_candidates_ticker ON stockbot.candidates (ticker);
CREATE INDEX IF NOT EXISTS idx_scan_results_run_ticker ON stockbot.scan_results (run_id, ticker);
CREATE INDEX IF NOT EXISTS idx_scan_results_run_failure ON stockbot.scan_results (run_id, failure_reason);
CREATE INDEX IF NOT EXISTS idx_scan_results_run_insufficient ON stockbot.scan_results (run_id, data_insufficient_reason);
