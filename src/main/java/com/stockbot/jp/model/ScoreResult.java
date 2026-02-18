package com.stockbot.jp.model;

import java.util.Collections;
import java.util.Map;

public final class ScoreResult {
    public final double score;
    public final Map<String, Double> breakdown;

    public ScoreResult(double score, Map<String, Double> breakdown) {
        this.score = score;
        this.breakdown = breakdown == null ? Map.of() : Collections.unmodifiableMap(breakdown);
    }
}
