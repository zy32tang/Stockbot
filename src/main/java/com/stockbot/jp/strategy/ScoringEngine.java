package com.stockbot.jp.strategy;

import com.stockbot.jp.config.Config;
import com.stockbot.jp.model.IndicatorSnapshot;
import com.stockbot.jp.model.RiskDecision;
import com.stockbot.jp.model.ScoreResult;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ScoringEngine {
    private static final List<String> FACTOR_NAMES = List.of(
            "pullback",
            "rsi",
            "sma_gap",
            "bollinger",
            "rebound",
            "volume",
            "risk_penalty"
    );

    private final Config config;

    public ScoringEngine(Config config) {
        this.config = config;
    }

    public ScoreResult score(IndicatorSnapshot ind, RiskDecision risk) {
        double wPullback = config.getDouble("score.weight_pullback", 0.22);
        double wRsi = config.getDouble("score.weight_rsi", 0.23);
        double wSmaGap = config.getDouble("score.weight_sma_gap", 0.16);
        double wBand = config.getDouble("score.weight_band", 0.14);
        double wRebound = config.getDouble("score.weight_rebound", 0.12);
        double wVolume = config.getDouble("score.weight_volume", 0.13);
        double wSum = wPullback + wRsi + wSmaGap + wBand + wRebound + wVolume;
        if (wSum <= 0.0001) {
            wSum = 1.0;
        }

        double pullback = scorePullback(ind.drawdown120Pct);
        double rsi = scoreRsi(ind.rsi14);
        double smaGap = scoreSmaGap(ind.pctFromSma20);
        double band = scoreBand(ind.lastClose, ind.bollingerLower, ind.bollingerUpper);
        double rebound = scoreRebound(ind.return3dPct, ind.return5dPct, ind.return10dPct);
        double volume = scoreVolume(ind.volumeRatio20);

        double weighted = (pullback * wPullback
                + rsi * wRsi
                + smaGap * wSmaGap
                + band * wBand
                + rebound * wRebound
                + volume * wVolume) / wSum;
        double finalScore = clamp(weighted - risk.penalty, 0.0, 100.0);

        Map<String, Double> breakdown = new LinkedHashMap<>();
        breakdown.put("pullback", pullback);
        breakdown.put("rsi", rsi);
        breakdown.put("sma_gap", smaGap);
        breakdown.put("bollinger", band);
        breakdown.put("rebound", rebound);
        breakdown.put("volume", volume);
        breakdown.put("risk_penalty", risk.penalty);
        breakdown.put("final", finalScore);
        return new ScoreResult(round2(finalScore), breakdown);
    }

    private double scorePullback(double drawdownPct) {
        return clamp(((-drawdownPct) - 5.0) / 30.0 * 100.0, 0.0, 100.0);
    }

    private double scoreRsi(double rsi) {
        return clamp(100.0 - Math.abs(rsi - 40.0) * 3.0, 0.0, 100.0);
    }

    private double scoreSmaGap(double pctFromSma20) {
        return clamp((-pctFromSma20) / 10.0 * 100.0, 0.0, 100.0);
    }

    private double scoreBand(double close, double lower, double upper) {
        double width = upper - lower;
        if (width <= 0.0) {
            return 50.0;
        }
        double pos = (close - lower) / width;
        return clamp((1.0 - pos) * 100.0, 0.0, 100.0);
    }

    private double scoreRebound(double r3, double r5, double r10) {
        double raw = 50.0 + r3 * 7.0 + r5 * 3.5 - Math.max(0.0, -r10) * 1.0;
        return clamp(raw, 0.0, 100.0);
    }

    private double scoreVolume(double volumeRatio20) {
        return clamp(volumeRatio20 * 50.0, 0.0, 100.0);
    }

    private double clamp(double value, double min, double max) {
        if (value < min) {
            return min;
        }
        if (value > max) {
            return max;
        }
        return value;
    }

    private double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    public static List<String> factorNames() {
        return FACTOR_NAMES;
    }
}
