package com.stockbot.jp.model;

public enum ScanFailureReason {
    NONE("none"),
    TIMEOUT("timeout"),
    HTTP_404_NO_DATA("http_404/no_data"),
    PARSE_ERROR("parse_error"),
    STALE("stale"),
    HISTORY_SHORT("history_short"),
    FILTERED_NON_TRADABLE("filtered_non_tradable"),
    RATE_LIMIT("rate_limit"),
    OTHER("other");

    private final String label;

    ScanFailureReason(String label) {
        this.label = label;
    }

    public String label() {
        return label;
    }

    public static ScanFailureReason fromLabel(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return NONE;
        }
        String target = raw.trim().toLowerCase();
        for (ScanFailureReason reason : values()) {
            if (reason.label.equals(target)) {
                return reason;
            }
        }
        return OTHER;
    }
}
