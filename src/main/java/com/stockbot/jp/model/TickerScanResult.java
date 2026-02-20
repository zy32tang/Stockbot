package com.stockbot.jp.model;

import java.time.LocalDate;
import java.util.List;

/**
 * 模块说明：TickerScanResult（class）。
 * 主要职责：承载 model 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
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

/**
 * 方法说明：TickerScanResult，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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

/**
 * 方法说明：ok，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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

/**
 * 方法说明：failed，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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

/**
 * 方法说明：failed，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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
