package com.stockbot.core.diagnostics;

/**
 * 模块说明：FeatureResolution（class）。
 * 主要职责：承载 diagnostics 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class FeatureResolution {
    public final String featureKey;
    public final boolean configValue;
    public final boolean implementationPresent;
    public final FeatureStatus status;
    public final CauseCode causeCode;
    public final String owner;
    public final String message;
    public final String runtimeExceptionClass;

/**
 * 方法说明：FeatureResolution，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public FeatureResolution(
            String featureKey,
            boolean configValue,
            boolean implementationPresent,
            FeatureStatus status,
            CauseCode causeCode,
            String owner,
            String message,
            String runtimeExceptionClass
    ) {
        this.featureKey = featureKey == null ? "" : featureKey;
        this.configValue = configValue;
        this.implementationPresent = implementationPresent;
        this.status = status == null ? FeatureStatus.DISABLED_NOT_IMPLEMENTED : status;
        this.causeCode = causeCode == null ? CauseCode.NONE : causeCode;
        this.owner = owner == null ? "" : owner;
        this.message = message == null ? "" : message;
        this.runtimeExceptionClass = runtimeExceptionClass == null ? "" : runtimeExceptionClass;
    }
}
