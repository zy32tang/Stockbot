package com.stockbot.jp.polymarket;

import java.util.List;

/**
 * 模块说明：PolymarketSignalReport（class）。
 * 主要职责：承载 polymarket 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class PolymarketSignalReport {
    public final boolean enabled;
    public final String statusMessage;
    public final List<PolymarketTopicSignal> signals;

/**
 * 方法说明：PolymarketSignalReport，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public PolymarketSignalReport(boolean enabled, String statusMessage, List<PolymarketTopicSignal> signals) {
        this.enabled = enabled;
        this.statusMessage = statusMessage == null ? "" : statusMessage;
        this.signals = signals == null ? List.of() : List.copyOf(signals);
    }

/**
 * 方法说明：disabled，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static PolymarketSignalReport disabled(String message) {
        return new PolymarketSignalReport(false, message, List.of());
    }

/**
 * 方法说明：empty，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static PolymarketSignalReport empty(String message) {
        return new PolymarketSignalReport(true, message, List.of());
    }
}
