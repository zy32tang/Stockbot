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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

public final class RunDao {
    private final Database database;

    public RunDao(Database database) {
        this.database = database;
    }

    public void recoverDanglingRuns() {
        try (Connection conn = database.connect()) {
            Set<String> columns = getColumns(conn, "runs");
            if (!columns.contains("status")) {
                return;
            }
            List<String> setParts = new ArrayList<>();
            setParts.add("status='ABORTED'");
            boolean hasFinishedAt = columns.contains("finished_at");
            if (hasFinishedAt) {
                setParts.add("finished_at=?");
            }
            if (columns.contains("notes")) {
                setParts.add("notes=COALESCE(notes, '') || ';recovered_on_startup'");
            }
            String where = "status='RUNNING'";
            String sql = "UPDATE runs SET " + String.join(", ", setParts) + " WHERE " + where;
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                if (hasFinishedAt) {
                    ps.setString(1, Instant.now().toString());
                }
                ps.executeUpdate();
            }
        } catch (SQLException ignored) {
            // Old/partial schemas should not block startup.
        }
    }

    public long startRun(String mode, String notes) throws SQLException {
        Instant now = Instant.now();
        try (Connection conn = database.connect()) {
            Set<String> columns = getColumns(conn, "runs");
            List<String> names = new ArrayList<>();
            List<Object> values = new ArrayList<>();

            if (columns.contains("mode")) {
                names.add("mode");
                values.add(mode);
            }
            if (columns.contains("run_mode")) {
                names.add("run_mode");
                values.add(mode);
            }
            if (columns.contains("started_at")) {
                names.add("started_at");
                values.add(now.toString());
            }
            if (columns.contains("run_at")) {
                names.add("run_at");
                values.add(now.toString());
            }
            if (columns.contains("status")) {
                names.add("status");
                values.add("RUNNING");
            }
            if (columns.contains("notes")) {
                names.add("notes");
                values.add(notes);
            }
            if (columns.contains("label")) {
                names.add("label");
                values.add(mode);
            }
            if (columns.contains("ai_targets")) {
                names.add("ai_targets");
                values.add(0);
            }

            if (names.isEmpty()) {
                throw new SQLException("runs table has no writable columns");
            }

            String sql = "INSERT INTO runs(" + String.join(", ", names) + ") VALUES(" + placeholders(values.size()) + ")";
            try (PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                for (int i = 0; i < values.size(); i++) {
                    ps.setObject(i + 1, values.get(i));
                }
                ps.executeUpdate();
                try (ResultSet rs = ps.getGeneratedKeys()) {
                    if (rs.next()) {
                        return rs.getLong(1);
                    }
                }
            }
        }
        throw new SQLException("failed to create run");
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
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, Instant.now().toString());
            ps.setString(2, status);
            ps.setInt(3, universeSize);
            ps.setInt(4, scannedSize);
            ps.setInt(5, candidateSize);
            ps.setInt(6, topN);
            ps.setString(7, reportPath);
            ps.setString(8, notes);
            ps.setLong(9, runId);
            ps.executeUpdate();
        }
    }

    public void insertCandidates(long runId, List<ScoredCandidate> candidates) throws SQLException {
        if (candidates == null || candidates.isEmpty()) {
            return;
        }
        String sql = "INSERT INTO candidates(run_id, rank_no, ticker, code, name, market, score, close, reasons_json, indicators_json, created_at) " +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        Instant now = Instant.now();
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            int rank = 1;
            for (ScoredCandidate c : candidates) {
                ps.setLong(1, runId);
                ps.setInt(2, rank++);
                ps.setString(3, c.ticker);
                ps.setString(4, c.code);
                ps.setString(5, c.name);
                ps.setString(6, c.market);
                ps.setDouble(7, c.score);
                ps.setDouble(8, c.close);
                ps.setString(9, c.reasonsJson);
                ps.setString(10, c.indicatorsJson);
                ps.setString(11, now.toString());
                ps.addBatch();
            }
            ps.executeBatch();
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
        Instant started = parseInstant(rs.getString("started_at"));
        Instant finished = parseInstant(rs.getString("finished_at"));
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

    private Instant parseInstant(String text) {
        if (text == null || text.isEmpty()) {
            return null;
        }
        try {
            return Instant.parse(text);
        } catch (Exception ignored) {
            return null;
        }
    }

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

    private String placeholders(int n) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < n; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("?");
        }
        return sb.toString();
    }
}
