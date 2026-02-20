package com.stockbot.jp.watch;

/**
 * 模块说明：TickerSpec（class）。
 * 主要职责：承载 watch 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class TickerSpec {
    public enum Market {
        JP,
        US,
        UNKNOWN
    }

    public enum ResolveStatus {
        OK,
        NEED_MARKET_HINT,
        INVALID
    }

    public final String raw;
    public final Market market;
    public final String normalized;
    public final ResolveStatus resolveStatus;

/**
 * 方法说明：TickerSpec，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public TickerSpec(String raw, Market market, String normalized, ResolveStatus resolveStatus) {
        this.raw = raw == null ? "" : raw;
        this.market = market == null ? Market.UNKNOWN : market;
        this.normalized = normalized == null ? "" : normalized;
        this.resolveStatus = resolveStatus == null ? ResolveStatus.INVALID : resolveStatus;
    }

/**
 * 方法说明：isOk，负责判断条件是否满足。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public boolean isOk() {
        return resolveStatus == ResolveStatus.OK;
    }
}
