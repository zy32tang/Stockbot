package com.stockbot.core.diagnostics;

/**
 * 模块说明：CauseCode（enum）。
 * 主要职责：承载 diagnostics 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public enum CauseCode {
    NONE,
    NO_BARS,
    HISTORY_SHORT,
    STALE,
    FETCH_FAILED,
    INDICATOR_ERROR,
    PLAN_INVALID,
    FEATURE_DISABLED_BY_CONFIG,
    FEATURE_NOT_IMPLEMENTED,
    FEATURE_RUNTIME_ERROR,
    TICKER_RESOLVE_FAILED,
    FILTER_REJECTED,
    RISK_REJECTED,
    SCORE_BELOW_THRESHOLD,
    GATE_SKIP_ON_PARTIAL,
    GATE_MIN_FETCH_COVERAGE,
    GATE_MIN_INDICATOR_COVERAGE,
    MISSING_INDICATORS,
    DATA_GAP,
    RUNTIME_ERROR
}
