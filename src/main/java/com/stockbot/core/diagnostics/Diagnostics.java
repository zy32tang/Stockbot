package com.stockbot.core.diagnostics;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 模块说明：Diagnostics（class）。
 * 主要职责：承载 diagnostics 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class Diagnostics {
    public final long runId;
    public final String runMode;
    public final Map<String, ConfigItem> configSnapshot = new LinkedHashMap<>();
    public final Map<String, Integer> dataSourceStats = new LinkedHashMap<>();
    public final Map<String, CoverageMetric> coverages = new LinkedHashMap<>();
    public final Map<String, FeatureResolution> featureStatuses = new LinkedHashMap<>();
    public final List<GateTrace> top5Gates = new ArrayList<>();
    public final List<String> notes = new ArrayList<>();

    public String coverageScope = "MARKET";
    public String selectedFetchCoverageKey = "";
    public String selectedIndicatorCoverageKey = "";
    public String coverageSource = "";
    public String coverageOwner = "";

/**
 * 方法说明：Diagnostics，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public Diagnostics(long runId, String runMode) {
        this.runId = runId;
        this.runMode = runMode == null ? "" : runMode;
    }

/**
 * 方法说明：addConfig，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public void addConfig(String key, String value, String source) {
        if (key == null || key.trim().isEmpty()) {
            return;
        }
        configSnapshot.put(key, new ConfigItem(key, value, source));
    }

/**
 * 方法说明：addDataSourceStat，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public void addDataSourceStat(String key, int count) {
        if (key == null || key.trim().isEmpty()) {
            return;
        }
        dataSourceStats.put(key, Math.max(0, count));
    }

/**
 * 方法说明：addCoverage，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public void addCoverage(String key, int numerator, int denominator, String source, String owner) {
        if (key == null || key.trim().isEmpty()) {
            return;
        }
        coverages.put(key, new CoverageMetric(key, numerator, denominator, source, owner));
    }

/**
 * 方法说明：selectCoverage，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public void selectCoverage(
            String scope,
            String fetchCoverageKey,
            String indicatorCoverageKey,
            String source,
            String owner
    ) {
        this.coverageScope = scope == null || scope.trim().isEmpty() ? "MARKET" : scope.trim().toUpperCase();
        this.selectedFetchCoverageKey = fetchCoverageKey == null ? "" : fetchCoverageKey;
        this.selectedIndicatorCoverageKey = indicatorCoverageKey == null ? "" : indicatorCoverageKey;
        this.coverageSource = source == null ? "" : source;
        this.coverageOwner = owner == null ? "" : owner;
    }

/**
 * 方法说明：addFeatureStatus，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public void addFeatureStatus(String key, FeatureResolution status) {
        if (key == null || key.trim().isEmpty() || status == null) {
            return;
        }
        featureStatuses.put(key, status);
    }

/**
 * 方法说明：addTop5Gate，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public void addTop5Gate(
            String gate,
            boolean passed,
            int failCount,
            String threshold,
            CauseCode causeCode,
            String owner,
            String details
    ) {
        top5Gates.add(new GateTrace(gate, passed, failCount, threshold, causeCode, owner, details));
    }

/**
 * 方法说明：addNote，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public void addNote(String note) {
        if (note == null || note.trim().isEmpty()) {
            return;
        }
        notes.add(note.trim());
    }

/**
 * 方法说明：feature，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public FeatureResolution feature(String key) {
        if (key == null) {
            return null;
        }
        return featureStatuses.get(key);
    }

/**
 * 方法说明：selectedFetchCoverage，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public CoverageMetric selectedFetchCoverage() {
        return coverages.get(selectedFetchCoverageKey);
    }

/**
 * 方法说明：selectedIndicatorCoverage，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public CoverageMetric selectedIndicatorCoverage() {
        return coverages.get(selectedIndicatorCoverageKey);
    }

    public static final class ConfigItem {
        public final String key;
        public final String value;
        public final String source;

        private ConfigItem(String key, String value, String source) {
            this.key = key;
            this.value = value == null ? "" : value;
            this.source = source == null ? "default" : source;
        }
    }

    public static final class CoverageMetric {
        public final String key;
        public final int numerator;
        public final int denominator;
        public final double pct;
        public final String source;
        public final String owner;

        private CoverageMetric(String key, int numerator, int denominator, String source, String owner) {
            this.key = key;
            this.numerator = Math.max(0, numerator);
            this.denominator = Math.max(0, denominator);
            this.pct = this.denominator <= 0 ? 0.0 : this.numerator * 100.0 / this.denominator;
            this.source = source == null ? "" : source;
            this.owner = owner == null ? "" : owner;
        }
    }

    public static final class GateTrace {
        public final String gate;
        public final boolean passed;
        public final int failCount;
        public final String threshold;
        public final CauseCode causeCode;
        public final String owner;
        public final String details;

        private GateTrace(
                String gate,
                boolean passed,
                int failCount,
                String threshold,
                CauseCode causeCode,
                String owner,
                String details
        ) {
            this.gate = gate == null ? "" : gate;
            this.passed = passed;
            this.failCount = Math.max(0, failCount);
            this.threshold = threshold == null ? "" : threshold;
            this.causeCode = causeCode == null ? CauseCode.NONE : causeCode;
            this.owner = owner == null ? "" : owner;
            this.details = details == null ? "" : details;
        }
    }
}
