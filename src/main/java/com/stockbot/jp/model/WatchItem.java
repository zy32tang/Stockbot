package com.stockbot.jp.model;

/**
 * Unified watch item for cross-module signal evaluation and display.
 */
public final class WatchItem {
    public final String ticker;
    public final String market;
    public final String displayCode;
    public final String displayNameLocal;
    public final String displayNameEn;

    public WatchItem(
            String ticker,
            String market,
            String displayCode,
            String displayNameLocal,
            String displayNameEn
    ) {
        this.ticker = ticker == null ? "" : ticker;
        this.market = market == null ? "" : market;
        this.displayCode = displayCode == null ? "" : displayCode;
        this.displayNameLocal = displayNameLocal == null ? "" : displayNameLocal;
        this.displayNameEn = displayNameEn == null ? "" : displayNameEn;
    }
}
