package com.stockbot.jp.db;

import com.stockbot.jp.db.mybatis.DataSourceCountRow;
import com.stockbot.jp.db.mybatis.MyBatisSupport;
import com.stockbot.jp.db.mybatis.ScanCoverageRow;
import com.stockbot.jp.db.mybatis.ScanReasonCountRow;
import com.stockbot.jp.db.mybatis.ScanResultInsertParam;
import com.stockbot.jp.db.mybatis.ScanResultMapper;
import com.stockbot.jp.model.DataInsufficientReason;
import com.stockbot.jp.model.ScanFailureReason;
import com.stockbot.jp.model.ScanResultSummary;
import com.stockbot.jp.model.TickerScanResult;
import org.apache.ibatis.session.SqlSession;

import java.sql.Connection;
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
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            conn.setAutoCommit(false);
            ScanResultMapper mapper = session.getMapper(ScanResultMapper.class);
            for (TickerScanResult result : results) {
                if (result == null || result.universe == null) {
                    continue;
                }
                ScanResultInsertParam row = ScanResultInsertParam.builder()
                        .runId(runId)
                        .ticker(result.universe.ticker)
                        .code(result.universe.code)
                        .market(result.universe.market)
                        .dataSource(result.dataSource)
                        .priceTimestamp(result.lastTradeDate)
                        .barsCount(Math.max(0, result.barsCount))
                        .lastClose(Double.isFinite(result.lastClose) && result.lastClose > 0.0 ? result.lastClose : null)
                        .cacheHit(result.cacheHit)
                        .fetchLatencyMs(Math.max(0L, result.fetchLatencyMs))
                        .fetchSuccess(result.fetchSuccess)
                        .indicatorReady(result.indicatorReady)
                        .candidateReady(result.candidate != null)
                        .dataInsufficientReason(result.dataInsufficientReason == null
                                ? DataInsufficientReason.NONE.name()
                                : result.dataInsufficientReason.name())
                        .failureReason(result.failureReason == null
                                ? ScanFailureReason.NONE.label()
                                : result.failureReason.label())
                        .requestFailureCategory(result.requestFailureCategory)
                        .error(result.error)
                        .createdAt(now)
                        .build();
                mapper.insertScanResult(row);
            }
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
        int tradableDenominator = 0;
        int tradableIndicatorCoverage = 0;
        Map<String, Integer> breakdownDenominatorExcluded = new LinkedHashMap<>();
        breakdownDenominatorExcluded.put("filtered_non_tradable", 0);
        breakdownDenominatorExcluded.put("history_short", 0);
        breakdownDenominatorExcluded.put("stale", 0);
        breakdownDenominatorExcluded.put("http_404/no_data", 0);

        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            ScanResultMapper mapper = session.getMapper(ScanResultMapper.class);

            ScanCoverageRow coverage = mapper.selectCoverage(runId);
            if (coverage != null) {
                total = coverage.getTotal();
                fetchCoverage = coverage.getFetchCoverage();
                indicatorCoverage = coverage.getIndicatorCoverage();
                breakdownDenominatorExcluded.put("filtered_non_tradable", Math.max(0, coverage.getExFilteredNonTradable()));
                breakdownDenominatorExcluded.put("history_short", Math.max(0, coverage.getExHistoryShort()));
                breakdownDenominatorExcluded.put("stale", Math.max(0, coverage.getExStale()));
                breakdownDenominatorExcluded.put("http_404/no_data", Math.max(0, coverage.getExNoData()));
                tradableDenominator = coverage.getTradableDenominator();
                tradableIndicatorCoverage = coverage.getTradableIndicatorCoverage();
            }

            for (ScanReasonCountRow row : mapper.selectFailureReasonCounts(runId)) {
                if (row == null) {
                    continue;
                }
                ScanFailureReason reason = ScanFailureReason.fromLabel(row.getValue());
                failureCounts.put(reason, row.getN());
            }

            for (ScanReasonCountRow row : mapper.selectRequestFailureCategoryCounts(runId)) {
                if (row == null) {
                    continue;
                }
                ScanFailureReason reason = mapRequestCategory(row.getValue());
                requestFailureCounts.put(reason, row.getN());
            }

            for (ScanReasonCountRow row : mapper.selectDataInsufficientReasonCounts(runId)) {
                if (row == null) {
                    continue;
                }
                DataInsufficientReason reason = DataInsufficientReason.fromText(row.getValue());
                insufficientCounts.put(reason, row.getN());
            }
        }

        return new ScanResultSummary(
                total,
                fetchCoverage,
                indicatorCoverage,
                Math.max(0, tradableDenominator),
                Math.max(0, tradableIndicatorCoverage),
                breakdownDenominatorExcluded,
                failureCounts,
                requestFailureCounts,
                insufficientCounts
        );
    }

    public Map<String, Integer> dataSourceCountsByRun(long runId) throws SQLException {
        Map<String, Integer> out = new LinkedHashMap<>();
        out.put("yahoo", 0);
        out.put("cache", 0);
        out.put("other", 0);

        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            ScanResultMapper mapper = session.getMapper(ScanResultMapper.class);
            for (DataSourceCountRow row : mapper.selectDataSourceCounts(runId)) {
                if (row == null) {
                    continue;
                }
                String raw = row.getDataSource();
                String key = raw == null ? "" : raw.trim().toLowerCase();
                if (!out.containsKey(key)) {
                    key = "other";
                }
                out.put(key, out.getOrDefault(key, 0) + row.getN());
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
