package com.stockbot.jp.db.mybatis;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Options;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.OffsetDateTime;
import java.util.List;

public interface RunMapper {
    @Update("UPDATE runs SET status='ABORTED', finished_at=#{finishedAt}, notes=COALESCE(notes, '') || ';recovered_on_startup' " +
            "WHERE status='RUNNING'")
    int recoverDanglingRuns(@Param("finishedAt") OffsetDateTime finishedAt);

    @Insert("INSERT INTO runs(mode, started_at, status, notes) VALUES(#{mode}, #{startedAt}, #{status}, #{notes})")
    @Options(useGeneratedKeys = true, keyProperty = "id", keyColumn = "id")
    int insertRun(RunInsertParam run);

    @Insert("INSERT INTO run_logs(run_id, mode, started_at, ended_at, step, elapsed_ms, status, message) " +
            "VALUES(#{runId}, #{mode}, #{startedAt}, #{endedAt}, #{step}, #{elapsedMs}, #{status}, #{message})")
    int insertRunLog(RunLogInsertParam row);

    @Update("UPDATE runs SET finished_at=#{finishedAt}, status=#{status}, universe_size=#{universeSize}, scanned_size=#{scannedSize}, " +
            "candidate_size=#{candidateSize}, top_n=#{topN}, report_path=#{reportPath}, notes=#{notes} WHERE id=#{runId}")
    int updateRunFinish(RunFinishParam row);

    @Select("SELECT mode FROM runs WHERE id=#{runId}")
    String findRunMode(@Param("runId") long runId);

    @Insert("INSERT INTO candidates(run_id, rank_no, ticker, code, name, market, score, close, reasons_json, indicators_json, created_at) " +
            "VALUES(#{runId}, #{rankNo}, #{ticker}, #{code}, #{name}, #{market}, #{score}, #{close}, #{reasonsJson}, #{indicatorsJson}, #{createdAt})")
    int insertCandidate(CandidateInsertParam row);

    @Insert("INSERT INTO signals(run_id, ticker, as_of, score, risk_level, signal_state, position_pct, reason) " +
            "VALUES(#{runId}, #{ticker}, #{asOf}, #{score}, #{riskLevel}, #{signalState}, #{positionPct}, #{reason})")
    int insertSignal(SignalInsertParam row);

    @Select("SELECT id, mode, started_at, finished_at, status, universe_size, scanned_size, candidate_size, top_n, report_path, notes " +
            "FROM runs ORDER BY id DESC LIMIT #{limit}")
    List<RunRowRecord> listRecentRuns(@Param("limit") int limit);

    @Select("SELECT id, mode, started_at, finished_at, status, universe_size, scanned_size, candidate_size, top_n, report_path, notes " +
            "FROM runs WHERE mode='DAILY' AND status='SUCCESS' ORDER BY id DESC LIMIT #{limit}")
    List<RunRowRecord> listSuccessfulDailyRuns(@Param("limit") int limit);

    @Select("SELECT id, mode, started_at, finished_at, status, universe_size, scanned_size, candidate_size, top_n, report_path, notes " +
            "FROM runs " +
            "WHERE mode IN ('DAILY','DAILY_REPORT') " +
            "AND status IN ('SUCCESS','PARTIAL') " +
            "AND report_path IS NOT NULL " +
            "AND report_path <> '' " +
            "ORDER BY id DESC LIMIT 1")
    RunRowRecord findLatestDailyRunWithReport();

    @Select("SELECT id, mode, started_at, finished_at, status, universe_size, scanned_size, candidate_size, top_n, report_path, notes " +
            "FROM runs r " +
            "WHERE r.mode IN ('DAILY_MARKET_SCAN','DAILY') " +
            "AND r.status IN ('SUCCESS','PARTIAL') " +
            "AND EXISTS (SELECT 1 FROM candidates c WHERE c.run_id = r.id) " +
            "ORDER BY r.id DESC LIMIT 1")
    RunRowRecord findLatestMarketScanRun();

    @Select("SELECT id, mode, started_at, finished_at, status, universe_size, scanned_size, candidate_size, top_n, report_path, notes " +
            "FROM runs WHERE id=#{runId} LIMIT 1")
    RunRowRecord findById(@Param("runId") long runId);

    @Select("SELECT id, mode, started_at, finished_at, status, universe_size, scanned_size, candidate_size, top_n, report_path, notes " +
            "FROM runs r " +
            "WHERE r.id < #{beforeRunId} " +
            "AND r.status IN ('SUCCESS','PARTIAL') " +
            "AND EXISTS (SELECT 1 FROM candidates c WHERE c.run_id = r.id) " +
            "ORDER BY r.id DESC LIMIT 1")
    RunRowRecord findLatestRunWithCandidatesBefore(@Param("beforeRunId") long beforeRunId);

    @Select("SELECT run_id, rank_no, ticker, code, name, score, close " +
            "FROM candidates WHERE run_id=#{runId} ORDER BY rank_no ASC LIMIT #{topK}")
    List<CandidateRowRecord> listCandidates(@Param("runId") long runId, @Param("topK") int topK);

    @Select("SELECT ticker, code, name, market, score, COALESCE(close, 0) AS close, reasons_json, indicators_json " +
            "FROM candidates WHERE run_id=#{runId} ORDER BY rank_no ASC LIMIT #{topK}")
    List<ScoredCandidateRowRecord> listScoredCandidates(@Param("runId") long runId, @Param("topK") int topK);

    @Select("SELECT ticker FROM candidates WHERE run_id=#{runId} ORDER BY rank_no ASC LIMIT #{limit}")
    List<String> listCandidateTickers(@Param("runId") long runId, @Param("limit") int limit);
}
