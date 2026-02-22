package com.stockbot.jp.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Idempotent PostgreSQL schema migration runner.
 */
public final class MigrationRunner {

    public void run(Database database) throws SQLException {
        try (Connection conn = database.connect(); Statement st = conn.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS stockbot");
            st.execute("SET search_path TO stockbot, public");
            st.execute("CREATE TABLE IF NOT EXISTS metadata (" +
                    "meta_key TEXT PRIMARY KEY," +
                    "meta_value TEXT NOT NULL," +
                    "updated_at TIMESTAMPTZ NOT NULL DEFAULT now()" +
                    ")");

            int currentVersion = readSchemaVersion(conn);
            int targetVersion = 1;
            String lastSql = "";

            try {
                List<String> sqls = buildStatements();
                for (String sql : sqls) {
                    lastSql = sql;
                    st.execute(sql);
                }
                writeSchemaVersion(conn, targetVersion);
            } catch (SQLException e) {
                String summary = summarizeSql(lastSql);
                String detail = "migration_failed: schema_version=" + currentVersion
                        + ", target_version=" + targetVersion
                        + ", failed_sql=" + summary
                        + ", cause=" + safe(e.getMessage());
                System.err.println("ERROR: " + detail);
                throw new SQLException(detail, e.getSQLState(), e.getErrorCode(), e);
            }
        }
    }

    private List<String> buildStatements() {
        List<String> sqls = new ArrayList<>();
        sqls.add("CREATE EXTENSION IF NOT EXISTS vector");
        sqls.add("CREATE SCHEMA IF NOT EXISTS stockbot");
        sqls.add("SET search_path TO stockbot, public");

        sqls.add("CREATE TABLE IF NOT EXISTS watchlist (" +
                "id BIGSERIAL PRIMARY KEY," +
                "ticker TEXT NOT NULL UNIQUE," +
                "name TEXT NULL," +
                "market TEXT NULL," +
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT now()" +
                ")");

        sqls.add("CREATE TABLE IF NOT EXISTS price_daily (" +
                "id BIGSERIAL PRIMARY KEY," +
                "ticker TEXT NOT NULL," +
                "trade_date DATE NOT NULL," +
                "open NUMERIC," +
                "high NUMERIC," +
                "low NUMERIC," +
                "close NUMERIC," +
                "volume NUMERIC," +
                "source TEXT NULL," +
                "created_at TIMESTAMPTZ NOT NULL DEFAULT now()," +
                "UNIQUE (ticker, trade_date)" +
                ")");

        sqls.add("CREATE TABLE IF NOT EXISTS indicators_daily (" +
                "id BIGSERIAL PRIMARY KEY," +
                "ticker TEXT NOT NULL," +
                "trade_date DATE NOT NULL," +
                "rsi14 NUMERIC NULL," +
                "macd NUMERIC NULL," +
                "macd_signal NUMERIC NULL," +
                "sma20 NUMERIC NULL," +
                "sma50 NUMERIC NULL," +
                "atr14 NUMERIC NULL," +
                "created_at TIMESTAMPTZ NOT NULL DEFAULT now()," +
                "UNIQUE (ticker, trade_date)" +
                ")");

        sqls.add("CREATE TABLE IF NOT EXISTS signals (" +
                "id BIGSERIAL PRIMARY KEY," +
                "run_id TEXT NOT NULL," +
                "ticker TEXT NOT NULL," +
                "as_of TIMESTAMPTZ NOT NULL," +
                "score NUMERIC NULL," +
                "risk_level TEXT NULL," +
                "signal_state TEXT NULL," +
                "position_pct NUMERIC NULL," +
                "reason TEXT NULL," +
                "created_at TIMESTAMPTZ NOT NULL DEFAULT now()" +
                ")");

        sqls.add("CREATE TABLE IF NOT EXISTS run_logs (" +
                "id BIGSERIAL PRIMARY KEY," +
                "run_id TEXT NOT NULL," +
                "mode TEXT NOT NULL," +
                "started_at TIMESTAMPTZ NOT NULL," +
                "ended_at TIMESTAMPTZ NULL," +
                "step TEXT NULL," +
                "elapsed_ms BIGINT NULL," +
                "status TEXT NULL," +
                "message TEXT NULL" +
                ")");

        sqls.add("CREATE TABLE IF NOT EXISTS docs (" +
                "id BIGSERIAL PRIMARY KEY," +
                "doc_type TEXT NOT NULL," +
                "ticker TEXT NULL," +
                "title TEXT NULL," +
                "content TEXT NOT NULL," +
                "lang TEXT NULL," +
                "source TEXT NULL," +
                "published_at TIMESTAMPTZ NULL," +
                "content_hash TEXT NOT NULL UNIQUE," +
                "embedding VECTOR(1536) NULL," +
                "created_at TIMESTAMPTZ NOT NULL DEFAULT now()" +
                ")");

        sqls.add("CREATE TABLE IF NOT EXISTS metadata (" +
                "meta_key TEXT PRIMARY KEY," +
                "meta_value TEXT NOT NULL," +
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT now()" +
                ")");

        sqls.add("CREATE TABLE IF NOT EXISTS universe (" +
                "ticker TEXT PRIMARY KEY," +
                "code TEXT NOT NULL," +
                "name TEXT NOT NULL," +
                "market TEXT NULL," +
                "active BOOLEAN NOT NULL DEFAULT TRUE," +
                "source TEXT NOT NULL," +
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT now()" +
                ")");

        sqls.add("CREATE TABLE IF NOT EXISTS runs (" +
                "id BIGSERIAL PRIMARY KEY," +
                "mode TEXT NOT NULL," +
                "started_at TIMESTAMPTZ NOT NULL," +
                "finished_at TIMESTAMPTZ NULL," +
                "status TEXT NOT NULL," +
                "universe_size INTEGER NOT NULL DEFAULT 0," +
                "scanned_size INTEGER NOT NULL DEFAULT 0," +
                "candidate_size INTEGER NOT NULL DEFAULT 0," +
                "top_n INTEGER NOT NULL DEFAULT 0," +
                "report_path TEXT NULL," +
                "notes TEXT NULL" +
                ")");

        sqls.add("CREATE TABLE IF NOT EXISTS candidates (" +
                "id BIGSERIAL PRIMARY KEY," +
                "run_id BIGINT NOT NULL REFERENCES runs(id) ON DELETE CASCADE," +
                "rank_no INTEGER NOT NULL," +
                "ticker TEXT NOT NULL," +
                "code TEXT NOT NULL," +
                "name TEXT NULL," +
                "market TEXT NULL," +
                "score NUMERIC NOT NULL," +
                "close NUMERIC NULL," +
                "reasons_json TEXT NOT NULL," +
                "indicators_json TEXT NOT NULL," +
                "created_at TIMESTAMPTZ NOT NULL DEFAULT now()" +
                ")");

        sqls.add("CREATE TABLE IF NOT EXISTS scan_results (" +
                "id BIGSERIAL PRIMARY KEY," +
                "run_id BIGINT NOT NULL REFERENCES runs(id) ON DELETE CASCADE," +
                "ticker TEXT NOT NULL," +
                "code TEXT NULL," +
                "market TEXT NULL," +
                "data_source TEXT NULL," +
                "price_timestamp DATE NULL," +
                "bars_count INTEGER NOT NULL DEFAULT 0," +
                "last_close NUMERIC NULL," +
                "cache_hit BOOLEAN NOT NULL DEFAULT FALSE," +
                "fetch_latency_ms BIGINT NOT NULL DEFAULT 0," +
                "fetch_success BOOLEAN NOT NULL DEFAULT FALSE," +
                "indicator_ready BOOLEAN NOT NULL DEFAULT FALSE," +
                "candidate_ready BOOLEAN NOT NULL DEFAULT FALSE," +
                "data_insufficient_reason TEXT NOT NULL DEFAULT 'NONE'," +
                "failure_reason TEXT NOT NULL DEFAULT 'none'," +
                "request_failure_category TEXT NULL," +
                "error TEXT NULL," +
                "created_at TIMESTAMPTZ NOT NULL DEFAULT now()" +
                ")");

        sqls.add("CREATE UNIQUE INDEX IF NOT EXISTS idx_universe_code ON universe(code)");
        sqls.add("CREATE INDEX IF NOT EXISTS idx_universe_active ON universe(active)");
        sqls.add("CREATE INDEX IF NOT EXISTS idx_price_daily_ticker_date ON price_daily(ticker, trade_date DESC)");
        sqls.add("CREATE INDEX IF NOT EXISTS idx_indicators_daily_ticker_date ON indicators_daily(ticker, trade_date DESC)");
        sqls.add("CREATE INDEX IF NOT EXISTS idx_signals_run_ticker ON signals(run_id, ticker)");
        sqls.add("CREATE INDEX IF NOT EXISTS idx_signals_asof ON signals(as_of DESC)");
        sqls.add("CREATE INDEX IF NOT EXISTS idx_run_logs_run_id ON run_logs(run_id)");
        sqls.add("CREATE INDEX IF NOT EXISTS idx_run_logs_started ON run_logs(started_at DESC)");
        sqls.add("CREATE INDEX IF NOT EXISTS docs_ticker_published_idx ON docs(ticker, published_at DESC)");
        sqls.add("CREATE INDEX IF NOT EXISTS docs_embedding_ivfflat ON docs USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)");
        sqls.add("CREATE INDEX IF NOT EXISTS idx_runs_started ON runs(started_at DESC)");
        sqls.add("CREATE INDEX IF NOT EXISTS idx_candidates_run_rank ON candidates(run_id, rank_no)");
        sqls.add("CREATE INDEX IF NOT EXISTS idx_candidates_ticker ON candidates(ticker)");
        sqls.add("CREATE INDEX IF NOT EXISTS idx_scan_results_run_ticker ON scan_results(run_id, ticker)");
        sqls.add("CREATE INDEX IF NOT EXISTS idx_scan_results_run_failure ON scan_results(run_id, failure_reason)");
        sqls.add("CREATE INDEX IF NOT EXISTS idx_scan_results_run_insufficient ON scan_results(run_id, data_insufficient_reason)");
        return sqls;
    }

    private int readSchemaVersion(Connection conn) {
        try (PreparedStatement ps = conn.prepareStatement("SELECT meta_value FROM metadata WHERE meta_key='schema_version'")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String value = rs.getString(1);
                    if (value != null && !value.trim().isEmpty()) {
                        return Integer.parseInt(value.trim());
                    }
                }
            }
        } catch (Exception ignored) {
            return 0;
        }
        return 0;
    }

    private void writeSchemaVersion(Connection conn, int version) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO metadata(meta_key, meta_value, updated_at) VALUES('schema_version', ?, now()) " +
                        "ON CONFLICT(meta_key) DO UPDATE SET meta_value=excluded.meta_value, updated_at=excluded.updated_at"
        )) {
            ps.setString(1, Integer.toString(version));
            ps.executeUpdate();
        }
    }

    private String summarizeSql(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return "-";
        }
        String oneLine = sql.replace('\n', ' ').replace('\r', ' ').replaceAll("\\s+", " ").trim();
        if (oneLine.length() <= 180) {
            return oneLine;
        }
        return oneLine.substring(0, 177) + "...";
    }

    private String safe(String value) {
        return value == null ? "" : value;
    }
}