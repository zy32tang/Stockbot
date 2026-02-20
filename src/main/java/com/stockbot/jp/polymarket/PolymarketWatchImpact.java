package com.stockbot.jp.polymarket;

/**
 * 模块说明：PolymarketWatchImpact（class）。
 * 主要职责：承载 polymarket 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class PolymarketWatchImpact {
    public final String code;
    public final String impact;
    public final double confidence;
    public final String rationale;

/**
 * 方法说明：PolymarketWatchImpact，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public PolymarketWatchImpact(String code, String impact, double confidence, String rationale) {
        this.code = code == null ? "" : code;
        this.impact = impact == null ? "neutral" : impact;
        this.confidence = Math.max(0.0, Math.min(1.0, confidence));
        this.rationale = rationale == null ? "" : rationale;
    }
}
