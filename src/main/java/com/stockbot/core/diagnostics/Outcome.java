package com.stockbot.core.diagnostics;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 模块说明：Outcome（class）。
 * 主要职责：承载 diagnostics 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class Outcome<T> {
    public final boolean success;
    public final T value;
    public final CauseCode causeCode;
    public final String owner;
    public final Map<String, Object> details;

/**
 * 方法说明：Outcome，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private Outcome(boolean success, T value, CauseCode causeCode, String owner, Map<String, Object> details) {
        this.success = success;
        this.value = value;
        this.causeCode = causeCode == null ? CauseCode.NONE : causeCode;
        this.owner = owner == null ? "" : owner;
        this.details = details == null ? Map.of() : Map.copyOf(details);
    }

/**
 * 方法说明：success，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static <T> Outcome<T> success(T value, String owner) {
        return new Outcome<>(true, value, CauseCode.NONE, owner, Map.of());
    }

/**
 * 方法说明：success，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static <T> Outcome<T> success(T value, String owner, Map<String, Object> details) {
        return new Outcome<>(true, value, CauseCode.NONE, owner, copy(details));
    }

/**
 * 方法说明：failure，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static <T> Outcome<T> failure(CauseCode causeCode, String owner) {
        return new Outcome<>(false, null, causeCode, owner, Map.of());
    }

/**
 * 方法说明：failure，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static <T> Outcome<T> failure(CauseCode causeCode, String owner, Map<String, Object> details) {
        return new Outcome<>(false, null, causeCode, owner, copy(details));
    }

/**
 * 方法说明：copy，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static Map<String, Object> copy(Map<String, Object> in) {
        if (in == null || in.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.putAll(in);
        return out;
    }
}
