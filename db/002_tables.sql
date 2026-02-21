CREATE TABLE IF NOT EXISTS stockbot.watchlist (
    id BIGSERIAL PRIMARY KEY,
    ticker TEXT NOT NULL UNIQUE,
    name TEXT NULL,
    market TEXT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stockbot.price_daily (
    id BIGSERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    trade_date DATE NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    source TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ticker, trade_date)
);

CREATE TABLE IF NOT EXISTS stockbot.indicators_daily (
    id BIGSERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    trade_date DATE NOT NULL,
    rsi14 NUMERIC NULL,
    macd NUMERIC NULL,
    macd_signal NUMERIC NULL,
    sma20 NUMERIC NULL,
    sma50 NUMERIC NULL,
    atr14 NUMERIC NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ticker, trade_date)
);

CREATE TABLE IF NOT EXISTS stockbot.signals (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    as_of TIMESTAMPTZ NOT NULL,
    score NUMERIC NULL,
    risk_level TEXT NULL,
    signal_state TEXT NULL,
    position_pct NUMERIC NULL,
    reason TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stockbot.run_logs (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    ended_at TIMESTAMPTZ NULL,
    step TEXT NULL,
    elapsed_ms BIGINT NULL,
    status TEXT NULL,
    message TEXT NULL
);

CREATE TABLE IF NOT EXISTS stockbot.docs (
    id BIGSERIAL PRIMARY KEY,
    doc_type TEXT NOT NULL,
    ticker TEXT NULL,
    title TEXT NULL,
    content TEXT NOT NULL,
    lang TEXT NULL,
    source TEXT NULL,
    published_at TIMESTAMPTZ NULL,
    content_hash TEXT NOT NULL UNIQUE,
    embedding VECTOR(1536) NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stockbot.metadata (
    meta_key TEXT PRIMARY KEY,
    meta_value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stockbot.universe (
    ticker TEXT PRIMARY KEY,
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    market TEXT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    source TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stockbot.runs (
    id BIGSERIAL PRIMARY KEY,
    mode TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NULL,
    status TEXT NOT NULL,
    universe_size INTEGER NOT NULL DEFAULT 0,
    scanned_size INTEGER NOT NULL DEFAULT 0,
    candidate_size INTEGER NOT NULL DEFAULT 0,
    top_n INTEGER NOT NULL DEFAULT 0,
    report_path TEXT NULL,
    notes TEXT NULL
);

CREATE TABLE IF NOT EXISTS stockbot.candidates (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES stockbot.runs(id) ON DELETE CASCADE,
    rank_no INTEGER NOT NULL,
    ticker TEXT NOT NULL,
    code TEXT NOT NULL,
    name TEXT NULL,
    market TEXT NULL,
    score NUMERIC NOT NULL,
    close NUMERIC NULL,
    reasons_json TEXT NOT NULL,
    indicators_json TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stockbot.scan_results (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES stockbot.runs(id) ON DELETE CASCADE,
    ticker TEXT NOT NULL,
    code TEXT NULL,
    market TEXT NULL,
    data_source TEXT NULL,
    price_timestamp DATE NULL,
    bars_count INTEGER NOT NULL DEFAULT 0,
    last_close NUMERIC NULL,
    cache_hit BOOLEAN NOT NULL DEFAULT FALSE,
    fetch_latency_ms BIGINT NOT NULL DEFAULT 0,
    fetch_success BOOLEAN NOT NULL DEFAULT FALSE,
    indicator_ready BOOLEAN NOT NULL DEFAULT FALSE,
    candidate_ready BOOLEAN NOT NULL DEFAULT FALSE,
    data_insufficient_reason TEXT NOT NULL DEFAULT 'NONE',
    failure_reason TEXT NOT NULL DEFAULT 'none',
    request_failure_category TEXT NULL,
    error TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
