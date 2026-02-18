package com.stockbot.jp.model;

import java.time.LocalDate;
import java.util.List;

public final class TickerScanResult {
    public final UniverseRecord universe;
    public final List<BarDaily> bars;
    public final ScoredCandidate candidate;
    public final String error;
    public final long downloadNanos;
    public final long parseNanos;
    public final String dataSource;
    public final boolean requestFailed;
    public final String requestFailureCategory;
    public final long fetchLatencyMs;
    public final boolean cacheHit;
    public final int barsCount;
    public final LocalDate lastTradeDate;
    public final double lastClose;
    public final boolean fetchSuccess;
    public final boolean indicatorReady;
    public final DataInsufficientReason dataInsufficientReason;
    public final ScanFailureReason failureReason;

    private TickerScanResult(
            UniverseRecord universe,
            List<BarDaily> bars,
            ScoredCandidate candidate,
            String error,
            long downloadNanos,
            long parseNanos,
            String dataSource,
            boolean requestFailed,
            String requestFailureCategory,
            long fetchLatencyMs,
            boolean cacheHit,
            int barsCount,
            LocalDate lastTradeDate,
            double lastClose,
            boolean fetchSuccess,
            boolean indicatorReady,
            DataInsufficientReason dataInsufficientReason,
            ScanFailureReason failureReason
    ) {
        this.universe = universe;
        this.bars = bars == null ? List.of() : bars;
        this.candidate = candidate;
        this.error = error;
        this.downloadNanos = Math.max(0L, downloadNanos);
        this.parseNanos = Math.max(0L, parseNanos);
        this.dataSource = dataSource == null ? "" : dataSource;
        this.requestFailed = requestFailed;
        this.requestFailureCategory = requestFailureCategory == null ? "" : requestFailureCategory;
        this.fetchLatencyMs = Math.max(0L, fetchLatencyMs);
        this.cacheHit = cacheHit;
        this.barsCount = Math.max(0, barsCount);
        this.lastTradeDate = lastTradeDate;
        this.lastClose = lastClose;
        this.fetchSuccess = fetchSuccess;
        this.indicatorReady = indicatorReady;
        this.dataInsufficientReason = dataInsufficientReason == null ? DataInsufficientReason.NONE : dataInsufficientReason;
        this.failureReason = failureReason == null ? ScanFailureReason.NONE : failureReason;
    }

    public static TickerScanResult ok(
            UniverseRecord universe,
            List<BarDaily> bars,
            ScoredCandidate candidate,
            long downloadNanos,
            long parseNanos,
            String dataSource,
            boolean requestFailed,
            String requestFailureCategory,
            long fetchLatencyMs,
            boolean cacheHit,
            int barsCount,
            LocalDate lastTradeDate,
            double lastClose,
            boolean fetchSuccess,
            boolean indicatorReady,
            DataInsufficientReason dataInsufficientReason,
            ScanFailureReason failureReason
    ) {
        return new TickerScanResult(
                universe,
                bars,
                candidate,
                null,
                downloadNanos,
                parseNanos,
                dataSource,
                requestFailed,
                requestFailureCategory,
                fetchLatencyMs,
                cacheHit,
                barsCount,
                lastTradeDate,
                lastClose,
                fetchSuccess,
                indicatorReady,
                dataInsufficientReason,
                failureReason
        );
    }

    public static TickerScanResult failed(
            UniverseRecord universe,
            String error,
            long downloadNanos,
            long parseNanos,
            String dataSource,
            boolean requestFailed,
            String requestFailureCategory,
            long fetchLatencyMs,
            boolean cacheHit,
            int barsCount,
            LocalDate lastTradeDate,
            double lastClose,
            boolean fetchSuccess,
            DataInsufficientReason dataInsufficientReason,
            ScanFailureReason failureReason
    ) {
        return new TickerScanResult(
                universe,
                List.of(),
                null,
                error,
                downloadNanos,
                parseNanos,
                dataSource,
                requestFailed,
                requestFailureCategory,
                fetchLatencyMs,
                cacheHit,
                barsCount,
                lastTradeDate,
                lastClose,
                fetchSuccess,
                false,
                dataInsufficientReason,
                failureReason
        );
    }

    public static TickerScanResult failed(UniverseRecord universe, String error) {
        return failed(
                universe,
                error,
                0L,
                0L,
                "",
                false,
                "",
                0L,
                false,
                0,
                null,
                Double.NaN,
                false,
                DataInsufficientReason.NONE,
                ScanFailureReason.OTHER
        );
    }
}
