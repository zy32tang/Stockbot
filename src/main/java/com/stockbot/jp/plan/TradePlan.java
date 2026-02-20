package com.stockbot.jp.plan;

/**
 * 模块说明：TradePlan（class）。
 * 主要职责：承载 plan 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class TradePlan {
    public final boolean valid;
    public final double entryLow;
    public final double entryHigh;
    public final double stopLoss;
    public final double takeProfit;
    public final double rrRatio;

/**
 * 方法说明：TradePlan，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public TradePlan(boolean valid, double entryLow, double entryHigh, double stopLoss, double takeProfit, double rrRatio) {
        this.valid = valid;
        this.entryLow = entryLow;
        this.entryHigh = entryHigh;
        this.stopLoss = stopLoss;
        this.takeProfit = takeProfit;
        this.rrRatio = rrRatio;
    }

/**
 * 方法说明：invalid，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static TradePlan invalid() {
        return new TradePlan(false, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
    }
}
