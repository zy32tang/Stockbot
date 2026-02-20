package com.stockbot.jp.strategy;

import com.stockbot.jp.config.Config;
import com.stockbot.jp.model.IndicatorSnapshot;
import com.stockbot.jp.model.RiskDecision;
import com.stockbot.jp.model.ScoreResult;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 模块说明：ScoringEngine（class）。
 * 主要职责：承载 strategy 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
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

/**
 * 方法说明：ScoringEngine，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public ScoringEngine(Config config) {
        this.config = config;
    }

/**
 * 方法说明：score，负责计算评分并输出分值。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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

/**
 * 方法说明：scorePullback，负责计算评分并输出分值。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double scorePullback(double drawdownPct) {
        return clamp(((-drawdownPct) - 5.0) / 30.0 * 100.0, 0.0, 100.0);
    }

/**
 * 方法说明：scoreRsi，负责计算评分并输出分值。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double scoreRsi(double rsi) {
        return clamp(100.0 - Math.abs(rsi - 40.0) * 3.0, 0.0, 100.0);
    }

/**
 * 方法说明：scoreSmaGap，负责计算评分并输出分值。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double scoreSmaGap(double pctFromSma20) {
        return clamp((-pctFromSma20) / 10.0 * 100.0, 0.0, 100.0);
    }

/**
 * 方法说明：scoreBand，负责计算评分并输出分值。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double scoreBand(double close, double lower, double upper) {
        double width = upper - lower;
        if (width <= 0.0) {
            return 50.0;
        }
        double pos = (close - lower) / width;
        return clamp((1.0 - pos) * 100.0, 0.0, 100.0);
    }

/**
 * 方法说明：scoreRebound，负责计算评分并输出分值。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double scoreRebound(double r3, double r5, double r10) {
        double raw = 50.0 + r3 * 7.0 + r5 * 3.5 - Math.max(0.0, -r10) * 1.0;
        return clamp(raw, 0.0, 100.0);
    }

/**
 * 方法说明：scoreVolume，负责计算评分并输出分值。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double scoreVolume(double volumeRatio20) {
        return clamp(volumeRatio20 * 50.0, 0.0, 100.0);
    }

/**
 * 方法说明：clamp，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double clamp(double value, double min, double max) {
        if (value < min) {
            return min;
        }
        if (value > max) {
            return max;
        }
        return value;
    }

/**
 * 方法说明：round2，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

/**
 * 方法说明：factorNames，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static List<String> factorNames() {
        return FACTOR_NAMES;
    }
}
