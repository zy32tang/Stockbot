package com.stockbot.core.diagnostics;

public final class FeatureStatusResolver {
    private FeatureStatusResolver() {
    }

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
