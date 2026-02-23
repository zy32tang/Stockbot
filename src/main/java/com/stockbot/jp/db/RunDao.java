package com.stockbot.jp.db;

import com.stockbot.jp.model.CandidateRow;
import com.stockbot.jp.model.RunRow;
import com.stockbot.jp.model.ScoredCandidate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * DAO for run/candidate persistence.
 */
public final class RunDao {
    private final Database database;

    public RunDao(Database database) {
        this.database = database;
    }

    public Database database() {
        return database;
    }

    public void recoverDanglingRuns() {
        String sql = "UPDATE runs SET status='ABORTED', finished_at=?, notes=COALESCE(notes, '') || ';recovered_on_startup' " +
                "WHERE status='RUNNING'";
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setObject(1, OffsetDateTime.now(ZoneOffset.UTC));
            ps.executeUpdate();
        } catch (SQLException ignored) {
            // Keep startup resilient if schema is partially initialized.
        }
    }

    public long startRun(String mode, String notes) throws SQLException {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        String insertRunSql = "INSERT INTO runs(mode, started_at, status, notes) VALUES(?, ?, ?, ?)";

        try (Connection conn = database.connect()) {
            conn.setAutoCommit(false);
            long runId;
            try (PreparedStatement ps = conn.prepareStatement(insertRunSql, Statement.RETURN_GENERATED_KEYS)) {
                ps.setString(1, mode);
                ps.setObject(2, now);
                ps.setString(3, "RUNNING");
                ps.setString(4, notes);
                ps.executeUpdate();
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    if (!rs.next()) {
                        throw new SQLException("failed to create run");
                    }
                    runId = rs.getLong(1);
                }
            }

            insertRunLog(conn, Long.toString(runId), mode, now, null, "START", null, "RUNNING", notes);
            conn.commit();
            return runId;
        }
    }

    public void finishRun(
            long runId,
            String status,
            int universeSize,
            int scannedSize,
            int candidateSize,
            int topN,
            String reportPath,
            String notes
    ) throws SQLException {
        String sql = "UPDATE runs SET finished_at=?, status=?, universe_size=?, scanned_size=?, " +
                "candidate_size=?, top_n=?, report_path=?, notes=? WHERE id=?";
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            ps.setObject(1, now);
            ps.setString(2, status);
            ps.setInt(3, universeSize);
            ps.setInt(4, scannedSize);
            ps.setInt(5, candidateSize);
            ps.setInt(6, topN);
            ps.setString(7, reportPath);
            ps.setString(8, notes);
            ps.setLong(9, runId);
            ps.executeUpdate();

            String mode = findRunMode(conn, runId).orElse("UNKNOWN");
            insertRunLog(conn, Long.toString(runId), mode, now, now, "FINISH", null, status, notes);
            conn.commit();
        }
    }

    public void insertCandidates(long runId, List<ScoredCandidate> candidates) throws SQLException {
        if (candidates == null || candidates.isEmpty()) {
            return;
        }
        String candidateSql = "INSERT INTO candidates(run_id, rank_no, ticker, code, name, market, score, close, reasons_json, indicators_json, created_at) " +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String signalSql = "INSERT INTO signals(run_id, ticker, as_of, score, risk_level, signal_state, position_pct, reason) " +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?)";

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        try (Connection conn = database.connect();
             PreparedStatement candidatePs = conn.prepareStatement(candidateSql);
             PreparedStatement signalPs = conn.prepareStatement(signalSql)) {
            conn.setAutoCommit(false);
            int rank = 1;
            for (ScoredCandidate c : candidates) {
                if (c == null) {
                    continue;
                }
                candidatePs.setLong(1, runId);
                candidatePs.setInt(2, rank++);
                candidatePs.setString(3, c.ticker);
                candidatePs.setString(4, c.code);
                candidatePs.setString(5, c.name);
                candidatePs.setString(6, c.market);
                candidatePs.setDouble(7, c.score);
                candidatePs.setObject(8, Double.isFinite(c.close) ? c.close : null);
                candidatePs.setString(9, c.reasonsJson);
                candidatePs.setString(10, c.indicatorsJson);
                candidatePs.setObject(11, now);
                candidatePs.addBatch();

                signalPs.setString(1, Long.toString(runId));
                signalPs.setString(2, c.ticker);
                signalPs.setObject(3, now);
                signalPs.setDouble(4, c.score);
                signalPs.setString(5, null);
                signalPs.setString(6, "CANDIDATE");
                signalPs.setObject(7, null);
                signalPs.setString(8, c.reasonsJson);
                signalPs.addBatch();
            }
            candidatePs.executeBatch();
            signalPs.executeBatch();
            conn.commit();
        }
    }

    public List<RunRow> listRecentRuns(int limit) throws SQLException {
        String sql = "SELECT id, mode, started_at, finished_at, status, universe_size, scanned_size, candidate_size, top_n, report_path, notes " +
                "FROM runs ORDER BY id DESC LIMIT ?";
        List<RunRow> out = new ArrayList<>();
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(mapRun(rs));
                }
            }
        }
        return out;
    }

    public List<RunRow> listSuccessfulDailyRuns(int limit) throws SQLException {
        String sql = "SELECT id, mode, started_at, finished_at, status, universe_size, scanned_size, candidate_size, top_n, report_path, notes " +
                "FROM runs WHERE mode='DAILY' AND status='SUCCESS' ORDER BY id DESC LIMIT ?";
        List<RunRow> out = new ArrayList<>();
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(mapRun(rs));
                }
            }
        }
        return out;
    }

    public Optional<RunRow> findLatestDailyRunWithReport() throws SQLException {
        String sql = "SELECT id, mode, started_at, finished_at, status, universe_size, scanned_size, candidate_size, top_n, report_path, notes " +
                "FROM runs " +
                "WHERE mode IN ('DAILY','DAILY_REPORT') " +
                "AND status IN ('SUCCESS','PARTIAL') " +
                "AND report_path IS NOT NULL " +
                "AND report_path <> '' " +
                "ORDER BY id DESC LIMIT 1";
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return Optional.of(mapRun(rs));
            }
        }
        return Optional.empty();
    }

    public Optional<RunRow> findLatestMarketScanRun() throws SQLException {
        String sql = "SELECT id, mode, started_at, finished_at, status, universe_size, scanned_size, candidate_size, top_n, report_path, notes " +
                "FROM runs r " +
                "WHERE r.mode IN ('DAILY_MARKET_SCAN','DAILY') " +
                "AND r.status IN ('SUCCESS','PARTIAL') " +
                "AND EXISTS (SELECT 1 FROM candidates c WHERE c.run_id = r.id) " +
                "ORDER BY r.id DESC LIMIT 1";
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return Optional.of(mapRun(rs));
            }
        }
        return Optional.empty();
    }

    public Optional<RunRow> findById(long runId) throws SQLException {
        String sql = "SELECT id, mode, started_at, finished_at, status, universe_size, scanned_size, candidate_size, top_n, report_path, notes " +
                "FROM runs WHERE id=? LIMIT 1";
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRun(rs));
                }
            }
        }
        return Optional.empty();
    }

    public Optional<RunRow> findLatestRunWithCandidatesBefore(long beforeRunId) throws SQLException {
        String sql = "SELECT id, mode, started_at, finished_at, status, universe_size, scanned_size, candidate_size, top_n, report_path, notes " +
                "FROM runs r " +
                "WHERE r.id < ? " +
                "AND r.status IN ('SUCCESS','PARTIAL') " +
                "AND EXISTS (SELECT 1 FROM candidates c WHERE c.run_id = r.id) " +
                "ORDER BY r.id DESC LIMIT 1";
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, beforeRunId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapRun(rs));
                }
            }
        }
        return Optional.empty();
    }

    public List<CandidateRow> listCandidates(long runId, int topK) throws SQLException {
        String sql = "SELECT run_id, rank_no, ticker, code, name, score, close " +
                "FROM candidates WHERE run_id=? ORDER BY rank_no ASC LIMIT ?";
        List<CandidateRow> out = new ArrayList<>();
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, runId);
            ps.setInt(2, Math.max(1, topK));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    Double close = rs.getObject("close") == null ? null : rs.getDouble("close");
                    out.add(new CandidateRow(
                            rs.getLong("run_id"),
                            rs.getInt("rank_no"),
                            rs.getString("ticker"),
                            rs.getString("code"),
                            rs.getString("name"),
                            rs.getDouble("score"),
                            close
                    ));
                }
            }
        }
        return out;
    }

    public List<ScoredCandidate> listScoredCandidates(long runId, int topK) throws SQLException {
        String sql = "SELECT ticker, code, name, market, score, close, reasons_json, indicators_json " +
                "FROM candidates WHERE run_id=? ORDER BY rank_no ASC LIMIT ?";
        List<ScoredCandidate> out = new ArrayList<>();
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, runId);
            ps.setInt(2, Math.max(1, topK));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new ScoredCandidate(
                            rs.getString("ticker"),
                            rs.getString("code"),
                            rs.getString("name"),
                            rs.getString("market"),
                            rs.getDouble("score"),
                            rs.getDouble("close"),
                            rs.getString("reasons_json"),
                            rs.getString("indicators_json")
                    ));
                }
            }
        }
        return out;
    }

    public List<String> listCandidateTickers(long runId, int limit) throws SQLException {
        String sql = "SELECT ticker FROM candidates WHERE run_id=? ORDER BY rank_no ASC LIMIT ?";
        List<String> out = new ArrayList<>();
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, runId);
            ps.setInt(2, Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String ticker = rs.getString("ticker");
                    if (ticker != null && !ticker.trim().isEmpty()) {
                        out.add(ticker.trim());
                    }
                }
            }
        }
        return out;
    }

    public String summarizeRecentRuns(int limit) throws SQLException {
        List<RunRow> runs = listRecentRuns(limit);
        if (runs.isEmpty()) {
            return "No runs in DB.";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("Recent runs:\n");
        for (RunRow run : runs) {
            sb.append(String.format(
                    Locale.US,
                    "#%d mode=%s status=%s scanned=%d candidates=%d topN=%d started=%s\n",
                    run.id,
                    run.mode,
                    run.status,
                    run.scannedSize,
                    run.candidateSize,
                    run.topN,
                    run.startedAt
            ));
        }
        return sb.toString().trim();
    }

    private RunRow mapRun(ResultSet rs) throws SQLException {
        Instant started = toInstant(rs, "started_at");
        Instant finished = toInstant(rs, "finished_at");
        return new RunRow(
                rs.getLong("id"),
                rs.getString("mode"),
                started,
                finished,
                rs.getString("status"),
                rs.getInt("universe_size"),
                rs.getInt("scanned_size"),
                rs.getInt("candidate_size"),
                rs.getInt("top_n"),
                rs.getString("report_path"),
                rs.getString("notes")
        );
    }

    private Instant toInstant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime odt = rs.getObject(column, OffsetDateTime.class);
        if (odt != null) {
            return odt.toInstant();
        }
        String text = rs.getString(column);
        if (text == null || text.trim().isEmpty()) {
            return null;
        }
        try {
            return Instant.parse(text.trim());
        } catch (Exception ignored) {
            return null;
        }
    }

    private Optional<String> findRunMode(Connection conn, long runId) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement("SELECT mode FROM runs WHERE id=?")) {
            ps.setLong(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.ofNullable(rs.getString("mode"));
                }
            }
        }
        return Optional.empty();
    }

    private void insertRunLog(
            Connection conn,
            String runId,
            String mode,
            OffsetDateTime startedAt,
            OffsetDateTime endedAt,
            String step,
            Long elapsedMs,
            String status,
            String message
    ) throws SQLException {
        String sql = "INSERT INTO run_logs(run_id, mode, started_at, ended_at, step, elapsed_ms, status, message) VALUES(?, ?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, runId);
            ps.setString(2, mode == null ? "UNKNOWN" : mode);
            ps.setObject(3, startedAt == null ? OffsetDateTime.now(ZoneOffset.UTC) : startedAt);
            ps.setObject(4, endedAt);
            ps.setString(5, step);
            ps.setObject(6, elapsedMs);
            ps.setString(7, status);
            ps.setString(8, message);
            ps.executeUpdate();
        }
    }
}
