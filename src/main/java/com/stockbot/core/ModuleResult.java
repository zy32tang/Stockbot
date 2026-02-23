package com.stockbot.core;

import java.util.LinkedHashMap;
import java.util.Map;

public record ModuleResult(
        ModuleStatus status,
        String reason,
        Map<String, Object> evidence
) {
    public ModuleResult {
        status = status == null ? ModuleStatus.ERROR : status;
        reason = reason == null ? "" : reason;
        Map<String, Object> copy = evidence == null ? Map.of() : new LinkedHashMap<>(evidence);
        evidence = Map.copyOf(copy);
    }

    public static ModuleResult ok(String reason) {
        return new ModuleResult(ModuleStatus.OK, reason, Map.of());
    }

    public static ModuleResult disabled(String reason, Map<String, Object> evidence) {
        return new ModuleResult(ModuleStatus.DISABLED, reason, evidence);
    }

    public static ModuleResult insufficient(String reason, Map<String, Object> evidence) {
        return new ModuleResult(ModuleStatus.INSUFFICIENT_DATA, reason, evidence);
    }

    public static ModuleResult error(String reason, Map<String, Object> evidence) {
        return new ModuleResult(ModuleStatus.ERROR, reason, evidence);
    }
}
