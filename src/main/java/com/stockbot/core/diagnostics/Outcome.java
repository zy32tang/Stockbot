package com.stockbot.core.diagnostics;

import java.util.LinkedHashMap;
import java.util.Map;

public final class Outcome<T> {
    public final boolean success;
    public final T value;
    public final CauseCode causeCode;
    public final String owner;
    public final Map<String, Object> details;

    private Outcome(boolean success, T value, CauseCode causeCode, String owner, Map<String, Object> details) {
        this.success = success;
        this.value = value;
        this.causeCode = causeCode == null ? CauseCode.NONE : causeCode;
        this.owner = owner == null ? "" : owner;
        this.details = details == null ? Map.of() : Map.copyOf(details);
    }

    public static <T> Outcome<T> success(T value, String owner) {
        return new Outcome<>(true, value, CauseCode.NONE, owner, Map.of());
    }

    public static <T> Outcome<T> success(T value, String owner, Map<String, Object> details) {
        return new Outcome<>(true, value, CauseCode.NONE, owner, copy(details));
    }

    public static <T> Outcome<T> failure(CauseCode causeCode, String owner) {
        return new Outcome<>(false, null, causeCode, owner, Map.of());
    }

    public static <T> Outcome<T> failure(CauseCode causeCode, String owner, Map<String, Object> details) {
        return new Outcome<>(false, null, causeCode, owner, copy(details));
    }

    private static Map<String, Object> copy(Map<String, Object> in) {
        if (in == null || in.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.putAll(in);
        return out;
    }
}
