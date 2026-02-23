package com.stockbot.jp.db.mybatis;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface ScanResultMapper {
    @Insert("INSERT INTO scan_results(run_id, ticker, code, market, data_source, price_timestamp, bars_count, last_close, cache_hit, " +
            "fetch_latency_ms, fetch_success, indicator_ready, candidate_ready, data_insufficient_reason, failure_reason, request_failure_category, error, created_at) " +
            "VALUES(#{runId}, #{ticker}, #{code}, #{market}, #{dataSource}, #{priceTimestamp}, #{barsCount}, #{lastClose}, #{cacheHit}, " +
            "#{fetchLatencyMs}, #{fetchSuccess}, #{indicatorReady}, #{candidateReady}, #{dataInsufficientReason}, #{failureReason}, #{requestFailureCategory}, #{error}, #{createdAt})")
    int insertScanResult(ScanResultInsertParam row);

    @Select("SELECT COUNT(*) AS total, " +
            "SUM(CASE WHEN fetch_success THEN 1 ELSE 0 END) AS fetch_coverage, " +
            "SUM(CASE WHEN indicator_ready THEN 1 ELSE 0 END) AS indicator_coverage, " +
            "SUM(CASE WHEN LOWER(COALESCE(failure_reason, ''))='filtered_non_tradable' THEN 1 ELSE 0 END) AS ex_filtered_non_tradable, " +
            "SUM(CASE WHEN LOWER(COALESCE(failure_reason, ''))='history_short' THEN 1 ELSE 0 END) AS ex_history_short, " +
            "SUM(CASE WHEN LOWER(COALESCE(failure_reason, ''))='stale' THEN 1 ELSE 0 END) AS ex_stale, " +
            "SUM(CASE WHEN LOWER(COALESCE(failure_reason, ''))='http_404/no_data' OR LOWER(COALESCE(request_failure_category, ''))='no_data' THEN 1 ELSE 0 END) AS ex_no_data, " +
            "SUM(CASE WHEN NOT (" +
            "LOWER(COALESCE(failure_reason, ''))='filtered_non_tradable' OR " +
            "LOWER(COALESCE(failure_reason, ''))='history_short' OR " +
            "LOWER(COALESCE(failure_reason, ''))='stale' OR " +
            "LOWER(COALESCE(failure_reason, ''))='http_404/no_data' OR " +
            "LOWER(COALESCE(request_failure_category, ''))='no_data'" +
            ") THEN 1 ELSE 0 END) AS tradable_denominator, " +
            "SUM(CASE WHEN indicator_ready AND NOT (" +
            "LOWER(COALESCE(failure_reason, ''))='filtered_non_tradable' OR " +
            "LOWER(COALESCE(failure_reason, ''))='history_short' OR " +
            "LOWER(COALESCE(failure_reason, ''))='stale' OR " +
            "LOWER(COALESCE(failure_reason, ''))='http_404/no_data' OR " +
            "LOWER(COALESCE(request_failure_category, ''))='no_data'" +
            ") THEN 1 ELSE 0 END) AS tradable_indicator_coverage " +
            "FROM scan_results WHERE run_id=#{runId}")
    ScanCoverageRow selectCoverage(@Param("runId") long runId);

    @Select("SELECT failure_reason AS value, COUNT(*) AS n " +
            "FROM scan_results WHERE run_id=#{runId} AND failure_reason IS NOT NULL AND failure_reason<>'' " +
            "GROUP BY failure_reason")
    List<ScanReasonCountRow> selectFailureReasonCounts(@Param("runId") long runId);

    @Select("SELECT request_failure_category AS value, COUNT(*) AS n " +
            "FROM scan_results WHERE run_id=#{runId} AND request_failure_category IS NOT NULL AND request_failure_category<>'' " +
            "GROUP BY request_failure_category")
    List<ScanReasonCountRow> selectRequestFailureCategoryCounts(@Param("runId") long runId);

    @Select("SELECT data_insufficient_reason AS value, COUNT(*) AS n " +
            "FROM scan_results WHERE run_id=#{runId} AND data_insufficient_reason IS NOT NULL AND data_insufficient_reason<>'' " +
            "GROUP BY data_insufficient_reason")
    List<ScanReasonCountRow> selectDataInsufficientReasonCounts(@Param("runId") long runId);

    @Select("SELECT data_source, COUNT(*) AS n FROM scan_results WHERE run_id=#{runId} GROUP BY data_source")
    List<DataSourceCountRow> selectDataSourceCounts(@Param("runId") long runId);
}
