package com.stockbot.core.diagnostics;

public final class FeatureResolution {
    public final String featureKey;
    public final boolean configValue;
    public final boolean implementationPresent;
    public final FeatureStatus status;
    public final CauseCode causeCode;
    public final String owner;
    public final String message;
    public final String runtimeExceptionClass;

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
