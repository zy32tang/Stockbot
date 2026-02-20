package com.stockbot.jp.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * 模块说明：MigrationRunner（class）。
 * 主要职责：承载 db 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class MigrationRunner {

/**
 * 方法说明：run，负责执行核心流程并返回执行结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public void run(Database database) throws SQLException {
        try (Connection conn = database.connect(); Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE TABLE IF NOT EXISTS metadata (" +
                    "meta_key TEXT PRIMARY KEY," +
                    "meta_value TEXT NOT NULL," +
                    "updated_at TEXT NOT NULL" +
                    ")");

            st.executeUpdate("CREATE TABLE IF NOT EXISTS universe (" +
                    "ticker TEXT PRIMARY KEY," +
                    "code TEXT NOT NULL," +
                    "name TEXT NOT NULL," +
                    "market TEXT," +
                    "active INTEGER NOT NULL DEFAULT 1," +
                    "source TEXT NOT NULL," +
                    "updated_at TEXT NOT NULL" +
                    ")");
            st.executeUpdate("CREATE UNIQUE INDEX IF NOT EXISTS idx_universe_code ON universe(code)");
            st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_universe_active ON universe(active)");

            st.executeUpdate("CREATE TABLE IF NOT EXISTS bars_daily (" +
                    "ticker TEXT NOT NULL," +
                    "trade_date TEXT NOT NULL," +
                    "open REAL NOT NULL," +
                    "high REAL NOT NULL," +
                    "low REAL NOT NULL," +
                    "close REAL NOT NULL," +
                    "volume REAL," +
                    "source TEXT NOT NULL," +
                    "updated_at TEXT NOT NULL," +
                    "PRIMARY KEY (ticker, trade_date)," +
                    "FOREIGN KEY (ticker) REFERENCES universe(ticker)" +
                    ")");
            st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_bars_ticker_date ON bars_daily(ticker, trade_date)");

            st.executeUpdate("CREATE TABLE IF NOT EXISTS runs (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                    "mode TEXT NOT NULL," +
                    "started_at TEXT NOT NULL," +
                    "finished_at TEXT," +
                    "status TEXT NOT NULL," +
                    "universe_size INTEGER NOT NULL DEFAULT 0," +
                    "scanned_size INTEGER NOT NULL DEFAULT 0," +
                    "candidate_size INTEGER NOT NULL DEFAULT 0," +
                    "top_n INTEGER NOT NULL DEFAULT 0," +
                    "report_path TEXT," +
                    "notes TEXT" +
                    ")");

            st.executeUpdate("CREATE TABLE IF NOT EXISTS candidates (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                    "run_id INTEGER NOT NULL," +
                    "rank_no INTEGER NOT NULL," +
                    "ticker TEXT NOT NULL," +
                    "code TEXT NOT NULL," +
                    "name TEXT," +
                    "market TEXT," +
                    "score REAL NOT NULL," +
                    "close REAL," +
                    "reasons_json TEXT NOT NULL," +
                    "indicators_json TEXT NOT NULL," +
                    "created_at TEXT NOT NULL," +
                    "FOREIGN KEY (run_id) REFERENCES runs(id)" +
                    ")");
            st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_candidates_run_rank ON candidates(run_id, rank_no)");
            st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_candidates_ticker ON candidates(ticker)");

            st.executeUpdate("CREATE TABLE IF NOT EXISTS scan_results (" +
                    "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                    "run_id INTEGER NOT NULL," +
                    "ticker TEXT NOT NULL," +
                    "code TEXT," +
                    "market TEXT," +
                    "data_source TEXT," +
                    "price_timestamp TEXT," +
                    "bars_count INTEGER NOT NULL DEFAULT 0," +
                    "last_close REAL," +
                    "cache_hit INTEGER NOT NULL DEFAULT 0," +
                    "fetch_latency_ms INTEGER NOT NULL DEFAULT 0," +
                    "fetch_success INTEGER NOT NULL DEFAULT 0," +
                    "indicator_ready INTEGER NOT NULL DEFAULT 0," +
                    "candidate_ready INTEGER NOT NULL DEFAULT 0," +
                    "data_insufficient_reason TEXT NOT NULL DEFAULT 'NONE'," +
                    "failure_reason TEXT NOT NULL DEFAULT 'none'," +
                    "request_failure_category TEXT," +
                    "error TEXT," +
                    "created_at TEXT NOT NULL," +
                    "FOREIGN KEY (run_id) REFERENCES runs(id)" +
                    ")");
            st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_scan_results_run_ticker ON scan_results(run_id, ticker)");
            st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_scan_results_run_failure ON scan_results(run_id, failure_reason)");
            st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_scan_results_run_insufficient ON scan_results(run_id, data_insufficient_reason)");

            migrateLegacy(conn);

            // Create after legacy migration in case started_at was added to an old runs table.
            st.executeUpdate("CREATE INDEX IF NOT EXISTS idx_runs_started ON runs(started_at DESC)");
        }
    }

/**
 * 方法说明：migrateLegacy，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void migrateLegacy(Connection conn) throws SQLException {
        ensureColumn(conn, "runs", "mode", "TEXT DEFAULT 'DAILY'");
        ensureColumn(conn, "runs", "started_at", "TEXT");
        ensureColumn(conn, "runs", "finished_at", "TEXT");
        ensureColumn(conn, "runs", "status", "TEXT DEFAULT 'SUCCESS'");
        ensureColumn(conn, "runs", "universe_size", "INTEGER DEFAULT 0");
        ensureColumn(conn, "runs", "scanned_size", "INTEGER DEFAULT 0");
        ensureColumn(conn, "runs", "candidate_size", "INTEGER DEFAULT 0");
        ensureColumn(conn, "runs", "top_n", "INTEGER DEFAULT 0");
        ensureColumn(conn, "runs", "report_path", "TEXT");

        ensureColumn(conn, "scan_results", "code", "TEXT");
        ensureColumn(conn, "scan_results", "market", "TEXT");
        ensureColumn(conn, "scan_results", "data_source", "TEXT");
        ensureColumn(conn, "scan_results", "price_timestamp", "TEXT");
        ensureColumn(conn, "scan_results", "bars_count", "INTEGER DEFAULT 0");
        ensureColumn(conn, "scan_results", "last_close", "REAL");
        ensureColumn(conn, "scan_results", "cache_hit", "INTEGER DEFAULT 0");
        ensureColumn(conn, "scan_results", "fetch_latency_ms", "INTEGER DEFAULT 0");
        ensureColumn(conn, "scan_results", "fetch_success", "INTEGER DEFAULT 0");
        ensureColumn(conn, "scan_results", "indicator_ready", "INTEGER DEFAULT 0");
        ensureColumn(conn, "scan_results", "candidate_ready", "INTEGER DEFAULT 0");
        ensureColumn(conn, "scan_results", "data_insufficient_reason", "TEXT DEFAULT 'NONE'");
        ensureColumn(conn, "scan_results", "failure_reason", "TEXT DEFAULT 'none'");
        ensureColumn(conn, "scan_results", "request_failure_category", "TEXT");
        ensureColumn(conn, "scan_results", "error", "TEXT");
        ensureColumn(conn, "scan_results", "created_at", "TEXT");

        Set<String> runCols = getColumns(conn, "runs");
        if (runCols.contains("run_mode")) {
            exec(conn, "UPDATE runs SET mode=COALESCE(mode, run_mode) WHERE mode IS NULL OR mode=''");
        }
        if (runCols.contains("run_at")) {
            exec(conn, "UPDATE runs SET started_at=COALESCE(started_at, run_at)");
            exec(conn, "UPDATE runs SET finished_at=COALESCE(finished_at, run_at)");
        }
        exec(conn, "UPDATE runs SET mode='DAILY' WHERE mode IS NULL OR mode=''");
        exec(conn, "UPDATE runs SET started_at=COALESCE(started_at, datetime('now')) WHERE started_at IS NULL OR started_at=''");
        exec(conn, "UPDATE runs SET status='SUCCESS' WHERE status IS NULL OR status=''");
    }

/**
 * 方法说明：ensureColumn，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void ensureColumn(Connection conn, String table, String column, String definition) throws SQLException {
        Set<String> columns = getColumns(conn, table);
        if (columns.contains(column.toLowerCase(Locale.ROOT))) {
            return;
        }
        exec(conn, "ALTER TABLE " + table + " ADD COLUMN " + column + " " + definition);
    }

/**
 * 方法说明：getColumns，负责获取数据并返回结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private Set<String> getColumns(Connection conn, String table) throws SQLException {
        Set<String> out = new HashSet<>();
        try (PreparedStatement ps = conn.prepareStatement("PRAGMA table_info(" + table + ")");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String name = rs.getString("name");
                if (name != null) {
                    out.add(name.toLowerCase(Locale.ROOT));
                }
            }
        }
        return out;
    }

/**
 * 方法说明：exec，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void exec(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.executeUpdate(sql);
        }
    }
}
