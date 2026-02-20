package com.stockbot.jp.model;

import java.util.EnumMap;
import java.util.Map;

/**
 * 模块说明：ScanResultSummary（class）。
 * 主要职责：承载 model 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class ScanResultSummary {
    public final int total;
    public final int fetchCoverage;
    public final int indicatorCoverage;
    public final Map<ScanFailureReason, Integer> failureCounts;
    public final Map<ScanFailureReason, Integer> requestFailureCounts;
    public final Map<DataInsufficientReason, Integer> insufficientCounts;

/**
 * 方法说明：ScanResultSummary，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public ScanResultSummary(
            int total,
            int fetchCoverage,
            int indicatorCoverage,
            Map<ScanFailureReason, Integer> failureCounts,
            Map<ScanFailureReason, Integer> requestFailureCounts,
            Map<DataInsufficientReason, Integer> insufficientCounts
    ) {
        this.total = Math.max(0, total);
        this.fetchCoverage = Math.max(0, fetchCoverage);
        this.indicatorCoverage = Math.max(0, indicatorCoverage);
        this.failureCounts = copyFailureCounts(failureCounts);
        this.requestFailureCounts = copyFailureCounts(requestFailureCounts);
        this.insufficientCounts = copyInsufficientCounts(insufficientCounts);
    }

/**
 * 方法说明：failureCount，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public int failureCount(ScanFailureReason reason) {
        if (reason == null) {
            return 0;
        }
        return failureCounts.getOrDefault(reason, 0);
    }

/**
 * 方法说明：requestFailureCount，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public int requestFailureCount(ScanFailureReason reason) {
        if (reason == null) {
            return 0;
        }
        return requestFailureCounts.getOrDefault(reason, 0);
    }

/**
 * 方法说明：insufficientCount，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public int insufficientCount(DataInsufficientReason reason) {
        if (reason == null) {
            return 0;
        }
        return insufficientCounts.getOrDefault(reason, 0);
    }

/**
 * 方法说明：copyFailureCounts，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private Map<ScanFailureReason, Integer> copyFailureCounts(Map<ScanFailureReason, Integer> in) {
        Map<ScanFailureReason, Integer> out = new EnumMap<>(ScanFailureReason.class);
        for (ScanFailureReason reason : ScanFailureReason.values()) {
            out.put(reason, 0);
        }
        if (in != null) {
            for (Map.Entry<ScanFailureReason, Integer> e : in.entrySet()) {
                ScanFailureReason key = e.getKey();
                if (key == null) {
                    continue;
                }
                int value = e.getValue() == null ? 0 : Math.max(0, e.getValue());
                out.put(key, value);
            }
        }
        return Map.copyOf(out);
    }

/**
 * 方法说明：copyInsufficientCounts，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private Map<DataInsufficientReason, Integer> copyInsufficientCounts(Map<DataInsufficientReason, Integer> in) {
        Map<DataInsufficientReason, Integer> out = new EnumMap<>(DataInsufficientReason.class);
        for (DataInsufficientReason reason : DataInsufficientReason.values()) {
            out.put(reason, 0);
        }
        if (in != null) {
            for (Map.Entry<DataInsufficientReason, Integer> e : in.entrySet()) {
                DataInsufficientReason key = e.getKey();
                if (key == null) {
                    continue;
                }
                int value = e.getValue() == null ? 0 : Math.max(0, e.getValue());
                out.put(key, value);
            }
        }
        return Map.copyOf(out);
    }
}
