package com.stockbot.jp.polymarket;

public final class PolymarketWatchImpact {
    public final String code;
    public final String impact;
    public final double confidence;
    public final String rationale;

    public PolymarketWatchImpact(String code, String impact, double confidence, String rationale) {
        this.code = code == null ? "" : code;
        this.impact = impact == null ? "neutral" : impact;
        this.confidence = Math.max(0.0, Math.min(1.0, confidence));
        this.rationale = rationale == null ? "" : rationale;
    }
}
