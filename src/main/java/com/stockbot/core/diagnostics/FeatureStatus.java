package com.stockbot.core.diagnostics;

/**
 * 模块说明：FeatureStatus（enum）。
 * 主要职责：承载 diagnostics 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public enum FeatureStatus {
    ENABLED,
    DISABLED_BY_CONFIG,
    DISABLED_NOT_IMPLEMENTED,
    DISABLED_RUNTIME_ERROR
}
