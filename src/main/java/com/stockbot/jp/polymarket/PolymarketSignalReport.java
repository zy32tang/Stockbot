package com.stockbot.jp.polymarket;

import java.util.List;

public final class PolymarketSignalReport {
    public final boolean enabled;
    public final String statusMessage;
    public final List<PolymarketTopicSignal> signals;

    public PolymarketSignalReport(boolean enabled, String statusMessage, List<PolymarketTopicSignal> signals) {
        this.enabled = enabled;
        this.statusMessage = statusMessage == null ? "" : statusMessage;
        this.signals = signals == null ? List.of() : List.copyOf(signals);
    }

    public static PolymarketSignalReport disabled(String message) {
        return new PolymarketSignalReport(false, message, List.of());
    }

    public static PolymarketSignalReport empty(String message) {
        return new PolymarketSignalReport(true, message, List.of());
    }
}
