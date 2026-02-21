package com.stockbot.jp.db;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/**
 * One-off migration tool from SQLite to PostgreSQL.
 */
public final class SqliteToPostgresMigrator {
    private static final int BATCH_SIZE = 500;

    private final Database postgres;

    public SqliteToPostgresMigrator(Database postgres) {
        this.postgres = postgres;
    }

    public MigrationStats migrate(Path sqlitePath) throws SQLException {
        if (sqlitePath == null) {
            throw new IllegalArgumentException("--sqlite-path is required");
        }
        Path resolved = sqlitePath.toAbsolutePath().normalize();
        if (!Files.exists(resolved)) {
            throw new IllegalArgumentException("SQLite file does not exist: " + resolved);
        }

        try (Connection sqlite = DriverManager.getConnection("jdbc:sqlite:" + resolved);
             Connection pg = postgres.connect()) {
            pg.setAutoCommit(false);
            try {
                int watchlist = migrateWatchlist(sqlite, pg);
                int priceDaily = migratePriceDaily(sqlite, pg);
                int signals = migrateSignals(sqlite, pg);
                int runLogs = migrateRunLogs(sqlite, pg);
                pg.commit();
                return new MigrationStats(watchlist, priceDaily, signals, runLogs);
            } catch (Exception e) {
                pg.rollback();
                if (e instanceof SQLException) {
                    throw (SQLException) e;
                }
                throw new SQLException("sqlite migration failed: " + e.getMessage(), e);
            }
        }
    }

    private int migrateWatchlist(Connection sqlite, Connection pg) throws SQLException {
        if (!tableExists(sqlite, "watchlist") && !tableExists(sqlite, "universe")) {
            return 0;
        }

        String selectSql;
        if (tableExists(sqlite, "watchlist")) {
            selectSql = "SELECT ticker, name, market FROM watchlist";
        } else {
            String activeExpr = columnExists(sqlite, "universe", "active") ? "active" : "1";
            selectSql = "SELECT ticker, name, market FROM universe WHERE " + activeExpr + " = 1";
        }

        String insertSql = "INSERT INTO watchlist(ticker, name, market, updated_at) VALUES(?, ?, ?, ?) " +
                "ON CONFLICT(ticker) DO UPDATE SET name=excluded.name, market=excluded.market, updated_at=excluded.updated_at";

        int count = 0;
        int batch = 0;
        try (PreparedStatement select = sqlite.prepareStatement(selectSql);
             ResultSet rs = select.executeQuery();
             PreparedStatement insert = pg.prepareStatement(insertSql)) {
            while (rs.next()) {
                String ticker = trim(rs.getString("ticker"));
                if (ticker == null) {
                    continue;
                }
                insert.setString(1, ticker);
                insert.setString(2, trim(rs.getString("name")));
                insert.setString(3, trim(rs.getString("market")));
                insert.setObject(4, OffsetDateTime.now(ZoneOffset.UTC));
                insert.addBatch();
                count++;
                batch++;
                if (batch >= BATCH_SIZE) {
                    insert.executeBatch();
                    batch = 0;
                }
            }
            if (batch > 0) {
                insert.executeBatch();
            }
        }
        return count;
    }

    private int migratePriceDaily(Connection sqlite, Connection pg) throws SQLException {
        if (!tableExists(sqlite, "bars_daily")) {
            return 0;
        }
        String selectSql = "SELECT ticker, trade_date, open, high, low, close, volume, source FROM bars_daily";
        String insertSql = "INSERT INTO price_daily(ticker, trade_date, open, high, low, close, volume, source) VALUES(?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT(ticker, trade_date) DO UPDATE SET " +
                "open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close, volume=excluded.volume, source=excluded.source";

        int count = 0;
        int batch = 0;
        try (PreparedStatement select = sqlite.prepareStatement(selectSql);
             ResultSet rs = select.executeQuery();
             PreparedStatement insert = pg.prepareStatement(insertSql)) {
            while (rs.next()) {
                String ticker = trim(rs.getString("ticker"));
                LocalDate tradeDate = parseLocalDate(rs.getObject("trade_date"));
                if (ticker == null || tradeDate == null) {
                    continue;
                }
                insert.setString(1, ticker);
                insert.setObject(2, tradeDate);
                insert.setObject(3, rs.getObject("open"));
                insert.setObject(4, rs.getObject("high"));
                insert.setObject(5, rs.getObject("low"));
                insert.setObject(6, rs.getObject("close"));
                insert.setObject(7, rs.getObject("volume"));
                insert.setString(8, trim(rs.getString("source")));
                insert.addBatch();
                count++;
                batch++;
                if (batch >= BATCH_SIZE) {
                    insert.executeBatch();
                    batch = 0;
                }
            }
            if (batch > 0) {
                insert.executeBatch();
            }
        }
        return count;
    }

    private int migrateSignals(Connection sqlite, Connection pg) throws SQLException {
        if (tableExists(sqlite, "signals")) {
            return migrateSignalsFromSignalsTable(sqlite, pg);
        }
        if (tableExists(sqlite, "candidates") && tableExists(sqlite, "runs")) {
            return migrateSignalsFromCandidates(sqlite, pg);
        }
        return 0;
    }

    private int migrateSignalsFromSignalsTable(Connection sqlite, Connection pg) throws SQLException {
        String selectSql = "SELECT run_id, ticker, as_of, score, risk_level, signal_state, position_pct, reason FROM signals";
        String insertSql = "INSERT INTO signals(run_id, ticker, as_of, score, risk_level, signal_state, position_pct, reason) " +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?)";

        int count = 0;
        int batch = 0;
        try (PreparedStatement select = sqlite.prepareStatement(selectSql);
             ResultSet rs = select.executeQuery();
             PreparedStatement insert = pg.prepareStatement(insertSql)) {
            while (rs.next()) {
                String ticker = trim(rs.getString("ticker"));
                if (ticker == null) {
                    continue;
                }
                insert.setString(1, trimToDefault(rs.getString("run_id"), "legacy"));
                insert.setString(2, ticker);
                insert.setObject(3, parseOffsetDateTime(rs.getObject("as_of"), OffsetDateTime.now(ZoneOffset.UTC)));
                insert.setObject(4, rs.getObject("score"));
                insert.setString(5, trim(rs.getString("risk_level")));
                insert.setString(6, trim(rs.getString("signal_state")));
                insert.setObject(7, rs.getObject("position_pct"));
                insert.setString(8, trim(rs.getString("reason")));
                insert.addBatch();
                count++;
                batch++;
                if (batch >= BATCH_SIZE) {
                    insert.executeBatch();
                    batch = 0;
                }
            }
            if (batch > 0) {
                insert.executeBatch();
            }
        }
        return count;
    }

    private int migrateSignalsFromCandidates(Connection sqlite, Connection pg) throws SQLException {
        String startedExpr = columnExists(sqlite, "runs", "started_at") ? "r.started_at" : "NULL";
        String runAtExpr = columnExists(sqlite, "runs", "run_at") ? "r.run_at" : "NULL";
        String selectSql = "SELECT c.run_id, c.ticker, " + startedExpr + " AS started_at, " + runAtExpr + " AS run_at, " +
                "c.score, c.reasons_json FROM candidates c LEFT JOIN runs r ON r.id = c.run_id";
        String insertSql = "INSERT INTO signals(run_id, ticker, as_of, score, risk_level, signal_state, position_pct, reason) " +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?)";

        int count = 0;
        int batch = 0;
        try (PreparedStatement select = sqlite.prepareStatement(selectSql);
             ResultSet rs = select.executeQuery();
             PreparedStatement insert = pg.prepareStatement(insertSql)) {
            while (rs.next()) {
                String ticker = trim(rs.getString("ticker"));
                if (ticker == null) {
                    continue;
                }
                Object asOfRaw = rs.getObject("started_at");
                if (asOfRaw == null) {
                    asOfRaw = rs.getObject("run_at");
                }
                insert.setString(1, trimToDefault(rs.getString("run_id"), "legacy"));
                insert.setString(2, ticker);
                insert.setObject(3, parseOffsetDateTime(asOfRaw, OffsetDateTime.now(ZoneOffset.UTC)));
                insert.setObject(4, rs.getObject("score"));
                insert.setString(5, null);
                insert.setString(6, "CANDIDATE");
                insert.setObject(7, null);
                insert.setString(8, trim(rs.getString("reasons_json")));
                insert.addBatch();
                count++;
                batch++;
                if (batch >= BATCH_SIZE) {
                    insert.executeBatch();
                    batch = 0;
                }
            }
            if (batch > 0) {
                insert.executeBatch();
            }
        }
        return count;
    }

    private int migrateRunLogs(Connection sqlite, Connection pg) throws SQLException {
        if (!tableExists(sqlite, "runs")) {
            return 0;
        }

        String modeExpr = columnExists(sqlite, "runs", "mode") ? "mode" :
                (columnExists(sqlite, "runs", "run_mode") ? "run_mode" : "'UNKNOWN'");
        String startedExpr = columnExists(sqlite, "runs", "started_at") ? "started_at" :
                (columnExists(sqlite, "runs", "run_at") ? "run_at" : "NULL");
        String finishedExpr = columnExists(sqlite, "runs", "finished_at") ? "finished_at" : "NULL";
        String statusExpr = columnExists(sqlite, "runs", "status") ? "status" : "'UNKNOWN'";
        String notesExpr = columnExists(sqlite, "runs", "notes") ? "notes" : "NULL";

        String selectSql = "SELECT id, " + modeExpr + " AS mode, " + startedExpr + " AS started_at, " +
                finishedExpr + " AS finished_at, " + statusExpr + " AS status, " + notesExpr + " AS notes FROM runs";
        String insertSql = "INSERT INTO run_logs(run_id, mode, started_at, ended_at, step, elapsed_ms, status, message) " +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?)";

        int count = 0;
        int batch = 0;
        try (PreparedStatement select = sqlite.prepareStatement(selectSql);
             ResultSet rs = select.executeQuery();
             PreparedStatement insert = pg.prepareStatement(insertSql)) {
            while (rs.next()) {
                OffsetDateTime started = parseOffsetDateTime(rs.getObject("started_at"), OffsetDateTime.now(ZoneOffset.UTC));
                OffsetDateTime ended = parseOffsetDateTime(rs.getObject("finished_at"), null);
                insert.setString(1, trimToDefault(rs.getString("id"), "legacy"));
                insert.setString(2, trimToDefault(rs.getString("mode"), "UNKNOWN"));
                insert.setObject(3, started);
                insert.setObject(4, ended);
                insert.setString(5, "SQLITE_MIGRATION");
                insert.setObject(6, null);
                insert.setString(7, trimToDefault(rs.getString("status"), "UNKNOWN"));
                insert.setString(8, trim(rs.getString("notes")));
                insert.addBatch();
                count++;
                batch++;
                if (batch >= BATCH_SIZE) {
                    insert.executeBatch();
                    batch = 0;
                }
            }
            if (batch > 0) {
                insert.executeBatch();
            }
        }
        return count;
    }

    private boolean tableExists(Connection conn, String table) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, table, null)) {
            return rs.next();
        }
    }

    private boolean columnExists(Connection conn, String table, String column) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getColumns(null, null, table, column)) {
            return rs.next();
        }
    }

    private LocalDate parseLocalDate(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof java.sql.Date) {
            return ((java.sql.Date) raw).toLocalDate();
        }
        String text = raw.toString().trim();
        if (text.isEmpty()) {
            return null;
        }
        if (text.length() >= 10) {
            text = text.substring(0, 10);
        }
        try {
            return LocalDate.parse(text);
        } catch (Exception ignored) {
            return null;
        }
    }

    private OffsetDateTime parseOffsetDateTime(Object raw, OffsetDateTime fallback) {
        if (raw == null) {
            return fallback;
        }
        if (raw instanceof OffsetDateTime) {
            return (OffsetDateTime) raw;
        }
        if (raw instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) raw).toInstant().atOffset(ZoneOffset.UTC);
        }
        if (raw instanceof java.sql.Date) {
            return ((java.sql.Date) raw).toLocalDate().atStartOfDay().atOffset(ZoneOffset.UTC);
        }
        if (raw instanceof Number) {
            long epoch = ((Number) raw).longValue();
            if (String.valueOf(Math.abs(epoch)).length() > 11) {
                return Instant.ofEpochMilli(epoch).atOffset(ZoneOffset.UTC);
            }
            return Instant.ofEpochSecond(epoch).atOffset(ZoneOffset.UTC);
        }

        String text = raw.toString().trim();
        if (text.isEmpty()) {
            return fallback;
        }
        try {
            return OffsetDateTime.parse(text);
        } catch (Exception ignored) {
            // continue
        }
        try {
            return Instant.parse(text).atOffset(ZoneOffset.UTC);
        } catch (Exception ignored) {
            // continue
        }
        try {
            return LocalDateTime.parse(text.replace(' ', 'T')).atOffset(ZoneOffset.UTC);
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private String trim(String value) {
        if (value == null) {
            return null;
        }
        String out = value.trim();
        return out.isEmpty() ? null : out;
    }

    private String trimToDefault(String value, String fallback) {
        String trimmed = trim(value);
        return trimmed == null ? fallback : trimmed;
    }

    public static final class MigrationStats {
        public final int watchlistCount;
        public final int priceDailyCount;
        public final int signalsCount;
        public final int runLogsCount;

        public MigrationStats(int watchlistCount, int priceDailyCount, int signalsCount, int runLogsCount) {
            this.watchlistCount = watchlistCount;
            this.priceDailyCount = priceDailyCount;
            this.signalsCount = signalsCount;
            this.runLogsCount = runLogsCount;
        }
    }
}
