package com.stockbot.core.diagnostics;

/**
 * 模块说明：FeatureStatusResolver（class）。
 * 主要职责：承载 diagnostics 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class FeatureStatusResolver {
/**
 * 方法说明：FeatureStatusResolver，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private FeatureStatusResolver() {
    }

/**
 * 方法说明：resolveFeatureStatus，负责解析规则并确定最终结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static FeatureResolution resolveFeatureStatus(
            String featureKey,
            boolean configValue,
            boolean implementationPresent,
            Throwable runtimeError,
            String owner
    ) {
        if (!configValue) {
            return new FeatureResolution(
                    featureKey,
                    false,
                    implementationPresent,
                    FeatureStatus.DISABLED_BY_CONFIG,
                    CauseCode.FEATURE_DISABLED_BY_CONFIG,
                    owner,
                    "disabled by config",
                    ""
            );
        }
        if (!implementationPresent) {
            return new FeatureResolution(
                    featureKey,
                    true,
                    false,
                    FeatureStatus.DISABLED_NOT_IMPLEMENTED,
                    CauseCode.FEATURE_NOT_IMPLEMENTED,
                    owner,
                    "feature not implemented",
                    ""
            );
        }
        if (runtimeError != null) {
            return new FeatureResolution(
                    featureKey,
                    true,
                    true,
                    FeatureStatus.DISABLED_RUNTIME_ERROR,
                    CauseCode.FEATURE_RUNTIME_ERROR,
                    owner,
                    runtimeError.getMessage() == null ? "runtime error" : runtimeError.getMessage(),
                    runtimeError.getClass().getSimpleName()
            );
        }
        return new FeatureResolution(
                featureKey,
                true,
                true,
                FeatureStatus.ENABLED,
                CauseCode.NONE,
                owner,
                "enabled",
                ""
        );
    }
}
