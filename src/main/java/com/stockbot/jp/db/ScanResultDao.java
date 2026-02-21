package com.stockbot.jp.db;

import com.stockbot.jp.model.DataInsufficientReason;
import com.stockbot.jp.model.ScanFailureReason;
import com.stockbot.jp.model.ScanResultSummary;
import com.stockbot.jp.model.TickerScanResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DAO for ticker scan diagnostics.
 */
public final class ScanResultDao {
    private final Database database;

    public ScanResultDao(Database database) {
        this.database = database;
    }

    public void insertBatch(long runId, List<TickerScanResult> results) throws SQLException {
        if (runId <= 0 || results == null || results.isEmpty()) {
            return;
        }
        String sql = "INSERT INTO scan_results(run_id, ticker, code, market, data_source, price_timestamp, bars_count, last_close, cache_hit, " +
                "fetch_latency_ms, fetch_success, indicator_ready, candidate_ready, data_insufficient_reason, failure_reason, request_failure_category, error, created_at) " +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            for (TickerScanResult result : results) {
                if (result == null || result.universe == null) {
                    continue;
                }
                ps.setLong(1, runId);
                ps.setString(2, result.universe.ticker);
                ps.setString(3, result.universe.code);
                ps.setString(4, result.universe.market);
                ps.setString(5, result.dataSource);
                ps.setObject(6, result.lastTradeDate);
                ps.setInt(7, Math.max(0, result.barsCount));
                ps.setObject(8, Double.isFinite(result.lastClose) && result.lastClose > 0.0 ? result.lastClose : null);
                ps.setBoolean(9, result.cacheHit);
                ps.setLong(10, Math.max(0L, result.fetchLatencyMs));
                ps.setBoolean(11, result.fetchSuccess);
                ps.setBoolean(12, result.indicatorReady);
                ps.setBoolean(13, result.candidate != null);
                ps.setString(14, result.dataInsufficientReason == null
                        ? DataInsufficientReason.NONE.name()
                        : result.dataInsufficientReason.name());
                ps.setString(15, result.failureReason == null
                        ? ScanFailureReason.NONE.label()
                        : result.failureReason.label());
                ps.setString(16, result.requestFailureCategory);
                ps.setString(17, result.error);
                ps.setObject(18, now);
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
        }
    }

    public ScanResultSummary summarizeByRun(long runId) throws SQLException {
        Map<ScanFailureReason, Integer> failureCounts = new EnumMap<>(ScanFailureReason.class);
        for (ScanFailureReason reason : ScanFailureReason.values()) {
            failureCounts.put(reason, 0);
        }

        Map<DataInsufficientReason, Integer> insufficientCounts = new EnumMap<>(DataInsufficientReason.class);
        for (DataInsufficientReason reason : DataInsufficientReason.values()) {
            insufficientCounts.put(reason, 0);
        }
        Map<ScanFailureReason, Integer> requestFailureCounts = new EnumMap<>(ScanFailureReason.class);
        for (ScanFailureReason reason : ScanFailureReason.values()) {
            requestFailureCounts.put(reason, 0);
        }

        int total = 0;
        int fetchCoverage = 0;
        int indicatorCoverage = 0;

        try (Connection conn = database.connect()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT COUNT(*) AS total, " +
                            "SUM(CASE WHEN fetch_success THEN 1 ELSE 0 END) AS fetch_coverage, " +
                            "SUM(CASE WHEN indicator_ready THEN 1 ELSE 0 END) AS indicator_coverage " +
                            "FROM scan_results WHERE run_id=?"
            )) {
                ps.setLong(1, runId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        total = rs.getInt("total");
                        fetchCoverage = rs.getInt("fetch_coverage");
                        indicatorCoverage = rs.getInt("indicator_coverage");
                    }
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT failure_reason, COUNT(*) AS n " +
                            "FROM scan_results WHERE run_id=? AND failure_reason IS NOT NULL AND failure_reason<>'' " +
                            "GROUP BY failure_reason"
            )) {
                ps.setLong(1, runId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ScanFailureReason reason = ScanFailureReason.fromLabel(rs.getString("failure_reason"));
                        failureCounts.put(reason, rs.getInt("n"));
                    }
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT request_failure_category, COUNT(*) AS n " +
                            "FROM scan_results WHERE run_id=? AND request_failure_category IS NOT NULL AND request_failure_category<>'' " +
                            "GROUP BY request_failure_category"
            )) {
                ps.setLong(1, runId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        ScanFailureReason reason = mapRequestCategory(rs.getString("request_failure_category"));
                        requestFailureCounts.put(reason, rs.getInt("n"));
                    }
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT data_insufficient_reason, COUNT(*) AS n " +
                            "FROM scan_results WHERE run_id=? AND data_insufficient_reason IS NOT NULL AND data_insufficient_reason<>'' " +
                            "GROUP BY data_insufficient_reason"
            )) {
                ps.setLong(1, runId);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        DataInsufficientReason reason = DataInsufficientReason.fromText(rs.getString("data_insufficient_reason"));
                        insufficientCounts.put(reason, rs.getInt("n"));
                    }
                }
            }
        }

        return new ScanResultSummary(total, fetchCoverage, indicatorCoverage, failureCounts, requestFailureCounts, insufficientCounts);
    }

    public Map<String, Integer> dataSourceCountsByRun(long runId) throws SQLException {
        Map<String, Integer> out = new LinkedHashMap<>();
        out.put("yahoo", 0);
        out.put("cache", 0);
        out.put("other", 0);

        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT data_source, COUNT(*) AS n FROM scan_results WHERE run_id=? GROUP BY data_source"
             )) {
            ps.setLong(1, runId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String raw = rs.getString("data_source");
                    String key = raw == null ? "" : raw.trim().toLowerCase();
                    if (!out.containsKey(key)) {
                        key = "other";
                    }
                    out.put(key, out.getOrDefault(key, 0) + rs.getInt("n"));
                }
            }
        }
        return Map.copyOf(out);
    }

    private ScanFailureReason mapRequestCategory(String rawCategory) {
        String category = rawCategory == null ? "" : rawCategory.trim().toLowerCase();
        if ("timeout".equals(category)) {
            return ScanFailureReason.TIMEOUT;
        }
        if ("no_data".equals(category)) {
            return ScanFailureReason.HTTP_404_NO_DATA;
        }
        if ("parse_error".equals(category)) {
            return ScanFailureReason.PARSE_ERROR;
        }
        if ("rate_limit".equals(category)) {
            return ScanFailureReason.RATE_LIMIT;
        }
        return ScanFailureReason.OTHER;
    }
}
