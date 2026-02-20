package com.stockbot.jp.model;

/**
 * 模块说明：DataInsufficientReason（enum）。
 * 主要职责：承载 model 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public enum DataInsufficientReason {
    NONE,
    NO_DATA,
    STALE,
    HISTORY_SHORT;

/**
 * 方法说明：fromText，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static DataInsufficientReason fromText(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return NONE;
        }
        try {
            return DataInsufficientReason.valueOf(raw.trim().toUpperCase());
        } catch (Exception ignored) {
            return NONE;
        }
    }
}
