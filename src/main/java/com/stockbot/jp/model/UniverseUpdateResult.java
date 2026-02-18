package com.stockbot.jp.model;

public final class UniverseUpdateResult {
    public final boolean updated;
    public final int totalSymbols;
    public final String message;

    public UniverseUpdateResult(boolean updated, int totalSymbols, String message) {
        this.updated = updated;
        this.totalSymbols = totalSymbols;
        this.message = message;
    }
}
