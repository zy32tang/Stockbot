package com.stockbot.jp.model;

/**
 * 模块说明：ScanFailureReason（enum）。
 * 主要职责：承载 model 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public enum ScanFailureReason {
    NONE("none"),
    TIMEOUT("timeout"),
    HTTP_404_NO_DATA("http_404/no_data"),
    PARSE_ERROR("parse_error"),
    STALE("stale"),
    HISTORY_SHORT("history_short"),
    FILTERED_NON_TRADABLE("filtered_non_tradable"),
    RATE_LIMIT("rate_limit"),
    OTHER("other");

    private final String label;

    ScanFailureReason(String label) {
        this.label = label;
    }

/**
 * 方法说明：label，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public String label() {
        return label;
    }

/**
 * 方法说明：fromLabel，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static ScanFailureReason fromLabel(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return NONE;
        }
        String target = raw.trim().toLowerCase();
        for (ScanFailureReason reason : values()) {
            if (reason.label.equals(target)) {
                return reason;
            }
        }
        return OTHER;
    }
}
