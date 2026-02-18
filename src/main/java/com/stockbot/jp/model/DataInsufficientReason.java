package com.stockbot.jp.model;

public enum DataInsufficientReason {
    NONE,
    NO_DATA,
    STALE,
    HISTORY_SHORT;

    public static DataInsufficientReason fromText(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return NONE;
        }
        try {
            return DataInsufficientReason.valueOf(raw.trim().toUpperCase());
        } catch (Exception ignored) {
            return NONE;
        }
    }
}
