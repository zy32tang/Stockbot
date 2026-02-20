package com.stockbot.jp.polymarket;

import java.util.List;

/**
 * 模块说明：PolymarketTopicSignal（class）。
 * 主要职责：承载 polymarket 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class PolymarketTopicSignal {
    public final String topic;
    public final double impliedProbabilityPct;
    public final double change24hPct;
    public final String oiDirection;
    public final List<String> likelyIndustries;
    public final List<PolymarketWatchImpact> watchImpacts;
    public final String sourceMarketTitle;

/**
 * 方法说明：PolymarketTopicSignal，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public PolymarketTopicSignal(
            String topic,
            double impliedProbabilityPct,
            double change24hPct,
            String oiDirection,
            List<String> likelyIndustries,
            List<PolymarketWatchImpact> watchImpacts,
            String sourceMarketTitle
    ) {
        this.topic = topic == null ? "" : topic;
        this.impliedProbabilityPct = impliedProbabilityPct;
        this.change24hPct = change24hPct;
        this.oiDirection = oiDirection == null ? "-" : oiDirection;
        this.likelyIndustries = likelyIndustries == null ? List.of() : List.copyOf(likelyIndustries);
        this.watchImpacts = watchImpacts == null ? List.of() : List.copyOf(watchImpacts);
        this.sourceMarketTitle = sourceMarketTitle == null ? "" : sourceMarketTitle;
    }
}
