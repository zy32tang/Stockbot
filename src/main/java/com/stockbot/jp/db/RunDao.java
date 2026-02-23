package com.stockbot.jp.db;

import com.stockbot.jp.db.mybatis.CandidateInsertParam;
import com.stockbot.jp.db.mybatis.CandidateRowRecord;
import com.stockbot.jp.db.mybatis.MyBatisSupport;
import com.stockbot.jp.db.mybatis.RunFinishParam;
import com.stockbot.jp.db.mybatis.RunInsertParam;
import com.stockbot.jp.db.mybatis.RunLogInsertParam;
import com.stockbot.jp.db.mybatis.RunMapper;
import com.stockbot.jp.db.mybatis.RunRowRecord;
import com.stockbot.jp.db.mybatis.ScoredCandidateRowRecord;
import com.stockbot.jp.db.mybatis.SignalInsertParam;
import com.stockbot.jp.model.CandidateRow;
import com.stockbot.jp.model.RunRow;
import com.stockbot.jp.model.ScoredCandidate;
import org.apache.ibatis.session.SqlSession;

import java.sql.Connection;
import java.sql.SQLException;
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
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            RunMapper mapper = session.getMapper(RunMapper.class);
            mapper.recoverDanglingRuns(OffsetDateTime.now(ZoneOffset.UTC));
        } catch (SQLException ignored) {
            // Keep startup resilient if schema is partially initialized.
        }
    }

    public long startRun(String mode, String notes) throws SQLException {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            conn.setAutoCommit(false);
            RunMapper mapper = session.getMapper(RunMapper.class);

            RunInsertParam run = RunInsertParam.builder()
                    .mode(mode)
                    .startedAt(now)
                    .status("RUNNING")
                    .notes(notes)
                    .build();
            mapper.insertRun(run);
            Long runId = run.getId();
            if (runId == null || runId <= 0L) {
                throw new SQLException("failed to create run");
            }

            mapper.insertRunLog(RunLogInsertParam.builder()
                    .runId(Long.toString(runId))
                    .mode(mode)
                    .startedAt(now)
                    .endedAt(null)
                    .step("START")
                    .elapsedMs(null)
                    .status("RUNNING")
                    .message(notes)
                    .build());
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
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            conn.setAutoCommit(false);
            RunMapper mapper = session.getMapper(RunMapper.class);

            mapper.updateRunFinish(RunFinishParam.builder()
                    .finishedAt(now)
                    .status(status)
                    .universeSize(universeSize)
                    .scannedSize(scannedSize)
                    .candidateSize(candidateSize)
                    .topN(topN)
                    .reportPath(reportPath)
                    .notes(notes)
                    .runId(runId)
                    .build());

            String mode = mapper.findRunMode(runId);
            mapper.insertRunLog(RunLogInsertParam.builder()
                    .runId(Long.toString(runId))
                    .mode(mode == null ? "UNKNOWN" : mode)
                    .startedAt(now)
                    .endedAt(now)
                    .step("FINISH")
                    .elapsedMs(null)
                    .status(status)
                    .message(notes)
                    .build());
            conn.commit();
        }
    }

    public void insertCandidates(long runId, List<ScoredCandidate> candidates) throws SQLException {
        if (candidates == null || candidates.isEmpty()) {
            return;
        }

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            conn.setAutoCommit(false);
            RunMapper mapper = session.getMapper(RunMapper.class);

            int rank = 1;
            for (ScoredCandidate c : candidates) {
                if (c == null) {
                    continue;
                }
                mapper.insertCandidate(CandidateInsertParam.builder()
                        .runId(runId)
                        .rankNo(rank++)
                        .ticker(c.ticker)
                        .code(c.code)
                        .name(c.name)
                        .market(c.market)
                        .score(c.score)
                        .close(Double.isFinite(c.close) ? c.close : null)
                        .reasonsJson(c.reasonsJson)
                        .indicatorsJson(c.indicatorsJson)
                        .createdAt(now)
                        .build());

                mapper.insertSignal(SignalInsertParam.builder()
                        .runId(Long.toString(runId))
                        .ticker(c.ticker)
                        .asOf(now)
                        .score(c.score)
                        .riskLevel(null)
                        .signalState("CANDIDATE")
                        .positionPct(null)
                        .reason(c.reasonsJson)
                        .build());
            }
            conn.commit();
        }
    }

    public List<RunRow> listRecentRuns(int limit) throws SQLException {
        List<RunRow> out = new ArrayList<>();
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            RunMapper mapper = session.getMapper(RunMapper.class);
            List<RunRowRecord> rows = mapper.listRecentRuns(Math.max(1, limit));
            for (RunRowRecord row : rows) {
                if (row == null) {
                    continue;
                }
                out.add(toRunRow(row));
            }
        }
        return out;
    }

    public List<RunRow> listSuccessfulDailyRuns(int limit) throws SQLException {
        List<RunRow> out = new ArrayList<>();
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            RunMapper mapper = session.getMapper(RunMapper.class);
            List<RunRowRecord> rows = mapper.listSuccessfulDailyRuns(Math.max(1, limit));
            for (RunRowRecord row : rows) {
                if (row == null) {
                    continue;
                }
                out.add(toRunRow(row));
            }
        }
        return out;
    }

    public Optional<RunRow> findLatestDailyRunWithReport() throws SQLException {
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            RunMapper mapper = session.getMapper(RunMapper.class);
            return toOptionalRun(mapper.findLatestDailyRunWithReport());
        }
    }

    public Optional<RunRow> findLatestMarketScanRun() throws SQLException {
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            RunMapper mapper = session.getMapper(RunMapper.class);
            return toOptionalRun(mapper.findLatestMarketScanRun());
        }
    }

    public Optional<RunRow> findById(long runId) throws SQLException {
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            RunMapper mapper = session.getMapper(RunMapper.class);
            return toOptionalRun(mapper.findById(runId));
        }
    }

    public Optional<RunRow> findLatestRunWithCandidatesBefore(long beforeRunId) throws SQLException {
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            RunMapper mapper = session.getMapper(RunMapper.class);
            return toOptionalRun(mapper.findLatestRunWithCandidatesBefore(beforeRunId));
        }
    }

    public List<CandidateRow> listCandidates(long runId, int topK) throws SQLException {
        List<CandidateRow> out = new ArrayList<>();
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            RunMapper mapper = session.getMapper(RunMapper.class);
            List<CandidateRowRecord> rows = mapper.listCandidates(runId, Math.max(1, topK));
            for (CandidateRowRecord row : rows) {
                if (row == null) {
                    continue;
                }
                out.add(new CandidateRow(
                        row.getRunId(),
                        row.getRankNo(),
                        row.getTicker(),
                        row.getCode(),
                        row.getName(),
                        row.getScore(),
                        row.getClose()
                ));
            }
        }
        return out;
    }

    public List<ScoredCandidate> listScoredCandidates(long runId, int topK) throws SQLException {
        List<ScoredCandidate> out = new ArrayList<>();
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            RunMapper mapper = session.getMapper(RunMapper.class);
            List<ScoredCandidateRowRecord> rows = mapper.listScoredCandidates(runId, Math.max(1, topK));
            for (ScoredCandidateRowRecord row : rows) {
                if (row == null) {
                    continue;
                }
                out.add(new ScoredCandidate(
                        row.getTicker(),
                        row.getCode(),
                        row.getName(),
                        row.getMarket(),
                        n(row.getScore()),
                        n(row.getClose()),
                        row.getReasonsJson(),
                        row.getIndicatorsJson()
                ));
            }
        }
        return out;
    }

    public List<String> listCandidateTickers(long runId, int limit) throws SQLException {
        List<String> out = new ArrayList<>();
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            RunMapper mapper = session.getMapper(RunMapper.class);
            List<String> rows = mapper.listCandidateTickers(runId, Math.max(1, limit));
            for (String ticker : rows) {
                if (ticker != null && !ticker.trim().isEmpty()) {
                    out.add(ticker.trim());
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

    private Optional<RunRow> toOptionalRun(RunRowRecord row) {
        if (row == null) {
            return Optional.empty();
        }
        return Optional.of(toRunRow(row));
    }

    private RunRow toRunRow(RunRowRecord row) {
        return new RunRow(
                row.getId(),
                row.getMode(),
                toInstant(row.getStartedAt()),
                toInstant(row.getFinishedAt()),
                row.getStatus(),
                row.getUniverseSize(),
                row.getScannedSize(),
                row.getCandidateSize(),
                row.getTopN(),
                row.getReportPath(),
                row.getNotes()
        );
    }

    private Instant toInstant(OffsetDateTime value) {
        return value == null ? null : value.toInstant();
    }

    private double n(Double value) {
        return value == null ? 0.0 : value;
    }
}
