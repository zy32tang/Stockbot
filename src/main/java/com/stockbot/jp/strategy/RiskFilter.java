package com.stockbot.jp.strategy;

import com.stockbot.jp.config.Config;
import com.stockbot.jp.model.IndicatorSnapshot;
import com.stockbot.jp.model.RiskDecision;

import java.util.ArrayList;
import java.util.List;

public final class RiskFilter {
    private static final List<String> RISK_FLAG_NAMES = List.of(
            "atr_too_high",
            "volatility_too_high",
            "drawdown_too_deep",
            "liquidity_weak"
    );

    private final Config config;

    public RiskFilter(Config config) {
        this.config = config;
    }

    public RiskDecision evaluate(IndicatorSnapshot ind) {
        double maxAtrPct = config.getDouble("risk.max_atr_pct", 9.0);
        double maxVolatilityPct = config.getDouble("risk.max_volatility_pct", 80.0);
        double maxDrawdownAbs = config.getDouble("risk.max_drawdown_pct", 60.0);
        double minVolumeRatio = config.getDouble("risk.min_volume_ratio", 0.3);
        double atrFailMultiplier = Math.max(1.0, config.getDouble("risk.fail_atr_multiplier", 1.5));
        double volatilityFailMultiplier = Math.max(1.0, config.getDouble("risk.fail_volatility_multiplier", 1.4));
        double atrPenaltyScale = Math.max(0.0, config.getDouble("risk.penalty.atr_scale", 1.7));
        double atrPenaltyCap = Math.max(0.0, config.getDouble("risk.penalty.atr_cap", 18.0));
        double volatilityPenaltyScale = Math.max(0.0, config.getDouble("risk.penalty.volatility_scale", 0.45));
        double volatilityPenaltyCap = Math.max(0.0, config.getDouble("risk.penalty.volatility_cap", 18.0));
        double drawdownPenaltyScale = Math.max(0.0, config.getDouble("risk.penalty.drawdown_scale", 1.1));
        double drawdownPenaltyCap = Math.max(0.0, config.getDouble("risk.penalty.drawdown_cap", 22.0));
        double liquidityPenalty = Math.max(0.0, config.getDouble("risk.penalty.liquidity", 12.0));

        List<String> flags = new ArrayList<>();
        double penalty = 0.0;
        boolean pass = true;

        if (ind.atrPct > maxAtrPct) {
            flags.add("atr_too_high");
            penalty += Math.min(atrPenaltyCap, (ind.atrPct - maxAtrPct) * atrPenaltyScale);
            if (ind.atrPct > maxAtrPct * atrFailMultiplier) {
                pass = false;
            }
        }
        if (ind.volatility20Pct > maxVolatilityPct) {
            flags.add("volatility_too_high");
            penalty += Math.min(volatilityPenaltyCap, (ind.volatility20Pct - maxVolatilityPct) * volatilityPenaltyScale);
            if (ind.volatility20Pct > maxVolatilityPct * volatilityFailMultiplier) {
                pass = false;
            }
        }
        if (Math.abs(ind.drawdown120Pct) > maxDrawdownAbs) {
            flags.add("drawdown_too_deep");
            penalty += Math.min(drawdownPenaltyCap, (Math.abs(ind.drawdown120Pct) - maxDrawdownAbs) * drawdownPenaltyScale);
            pass = false;
        }
        if (ind.volumeRatio20 < minVolumeRatio) {
            flags.add("liquidity_weak");
            penalty += liquidityPenalty;
            pass = false;
        }

        return new RiskDecision(pass, penalty, flags);
    }

    public static List<String> riskFlagNames() {
        return RISK_FLAG_NAMES;
    }
}
