package com.stockbot.jp.model;

import com.stockbot.model.DailyPrice;

import java.util.List;

/**
 * 模块说明：WatchlistAnalysis（class）。
 * 主要职责：承载 model 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class WatchlistAnalysis {
    public final String watchItem;
    public final String code;
    public final String ticker;
    public final String displayName;
    public final String companyNameLocal;
    public final String industryZh;
    public final String industryEn;
    public final String resolvedMarket;
    public final String resolveStatus;
    public final String normalizedTicker;

    public final double lastClose;
    public final double prevClose;
    public final double pctChange;
    public final String dataSource;
    public final String priceTimestamp;
    public final int barsCount;
    public final boolean cacheHit;
    public final long fetchLatencyMs;
    public final boolean fetchSuccess;
    public final boolean indicatorReady;
    public final boolean priceSuspect;

    public final double totalScore;
    public final String rating;
    public final String risk;
    public final boolean aiTriggered;
    public final String gateReason;
    public final int newsCount;
    public final String newsSource;
    public final String aiSummary;
    public final List<String> newsDigests;

    public final double technicalScore;
    public final String technicalStatus;
    public final String technicalReasonsJson;
    public final String technicalIndicatorsJson;
    public final String diagnosticsJson;
    public final String error;
    public final List<DailyPrice> priceHistory;

/**
 * 方法说明：WatchlistAnalysis，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public WatchlistAnalysis(
            String watchItem,
            String code,
            String ticker,
            String displayName,
            String companyNameLocal,
            String industryZh,
            String industryEn,
            String resolvedMarket,
            String resolveStatus,
            String normalizedTicker,
            double lastClose,
            double prevClose,
            double pctChange,
            String dataSource,
            String priceTimestamp,
            int barsCount,
            boolean cacheHit,
            long fetchLatencyMs,
            boolean fetchSuccess,
            boolean indicatorReady,
            boolean priceSuspect,
            double totalScore,
            String rating,
            String risk,
            boolean aiTriggered,
            String gateReason,
            int newsCount,
            String newsSource,
            String aiSummary,
            List<String> newsDigests,
            double technicalScore,
            String technicalStatus,
            String technicalReasonsJson,
            String technicalIndicatorsJson,
            String diagnosticsJson,
            String error,
            List<DailyPrice> priceHistory
    ) {
        this.watchItem = watchItem;
        this.code = code;
        this.ticker = ticker;
        this.displayName = displayName;
        this.companyNameLocal = companyNameLocal;
        this.industryZh = industryZh;
        this.industryEn = industryEn;
        this.resolvedMarket = resolvedMarket == null ? "" : resolvedMarket;
        this.resolveStatus = resolveStatus == null ? "" : resolveStatus;
        this.normalizedTicker = normalizedTicker == null ? "" : normalizedTicker;
        this.lastClose = lastClose;
        this.prevClose = prevClose;
        this.pctChange = pctChange;
        this.dataSource = dataSource == null ? "" : dataSource;
        this.priceTimestamp = priceTimestamp == null ? "" : priceTimestamp;
        this.barsCount = Math.max(0, barsCount);
        this.cacheHit = cacheHit;
        this.fetchLatencyMs = Math.max(0L, fetchLatencyMs);
        this.fetchSuccess = fetchSuccess;
        this.indicatorReady = indicatorReady;
        this.priceSuspect = priceSuspect;
        this.totalScore = totalScore;
        this.rating = rating;
        this.risk = risk;
        this.aiTriggered = aiTriggered;
        this.gateReason = gateReason;
        this.newsCount = newsCount;
        this.newsSource = newsSource;
        this.aiSummary = aiSummary;
        this.newsDigests = newsDigests == null ? List.of() : List.copyOf(newsDigests);
        this.technicalScore = technicalScore;
        this.technicalStatus = technicalStatus;
        this.technicalReasonsJson = technicalReasonsJson;
        this.technicalIndicatorsJson = technicalIndicatorsJson;
        this.diagnosticsJson = diagnosticsJson == null ? "" : diagnosticsJson;
        this.error = error;
        this.priceHistory = priceHistory == null ? List.of() : List.copyOf(priceHistory);
    }
}
