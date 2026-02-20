package com.stockbot.jp.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 模块说明：FilterDecision（class）。
 * 主要职责：承载 model 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class FilterDecision {
    public final boolean passed;
    public final List<String> reasons;
    public final Map<String, Object> metrics;

/**
 * 方法说明：FilterDecision，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public FilterDecision(boolean passed, List<String> reasons, Map<String, Object> metrics) {
        this.passed = passed;
        this.reasons = reasons == null ? List.of() : Collections.unmodifiableList(reasons);
        this.metrics = metrics == null ? Map.of() : Collections.unmodifiableMap(metrics);
    }
}
