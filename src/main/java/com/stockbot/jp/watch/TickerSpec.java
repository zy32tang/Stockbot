package com.stockbot.jp.watch;

public final class TickerSpec {
    public enum Market {
        JP,
        US,
        UNKNOWN
    }

    public enum ResolveStatus {
        OK,
        NEED_MARKET_HINT,
        INVALID
    }

    public final String raw;
    public final Market market;
    public final String normalized;
    public final ResolveStatus resolveStatus;

    public TickerSpec(String raw, Market market, String normalized, ResolveStatus resolveStatus) {
        this.raw = raw == null ? "" : raw;
        this.market = market == null ? Market.UNKNOWN : market;
        this.normalized = normalized == null ? "" : normalized;
        this.resolveStatus = resolveStatus == null ? ResolveStatus.INVALID : resolveStatus;
    }

    public boolean isOk() {
        return resolveStatus == ResolveStatus.OK;
    }
}
