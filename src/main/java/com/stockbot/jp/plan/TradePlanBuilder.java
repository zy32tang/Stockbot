package com.stockbot.jp.plan;

import com.stockbot.core.diagnostics.CauseCode;
import com.stockbot.core.diagnostics.Outcome;
import com.stockbot.jp.config.Config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class TradePlanBuilder {
    public static final String OWNER = "com.stockbot.jp.plan.TradePlanBuilder#build(...)";

    private final Config config;

    public TradePlanBuilder(Config config) {
        this.config = config;
    }

    public Outcome<TradePlan> build(Input in) {
        if (in == null) {
            return Outcome.failure(CauseCode.PLAN_INVALID, OWNER, Map.of("reason", "input_null"));
        }

        List<String> missing = new ArrayList<>();
        if (!isFinitePositive(in.lastClose)) missing.add("last_close");
        if (!isFinitePositive(in.sma20)) missing.add("sma20");
        if (!isFinitePositive(in.lowLookback)) missing.add("low_lookback");
        if (!isFinitePositive(in.highLookback)) missing.add("high_lookback");
        if (!missing.isEmpty()) {
            return Outcome.failure(
                    CauseCode.PLAN_INVALID,
                    OWNER,
                    Map.of("missing_inputs", missing)
            );
        }

        double rrFloor = Math.max(1.0, config.getDouble("plan.rr.min_floor", 1.1));
        double rrMin = Math.max(rrFloor, config.getDouble("rr.min", 1.5));
        double entryBufferPct = Math.max(0.0, config.getDouble("plan.entry.buffer_pct", 0.5));
        double entryBand = entryBufferPct / 100.0;
        double stopBuffer = clamp(config.getDouble("stop.loss.bufferPct", 0.02), 0.0, 0.2);
        double atrMult = Math.max(0.1, config.getDouble("plan.stop.atr_mult", 1.5));
        double highLookbackMult = clamp(config.getDouble("plan.target.high_lookback_mult", 0.98), 0.5, 1.5);

        double entryMid = in.lastClose;
        double entryLow = round2(entryMid * (1.0 - entryBand));
        double entryHigh = round2(entryMid * (1.0 + entryBand));

        double stopByLow = in.lowLookback * (1.0 - stopBuffer);
        double stopByAtr = Double.isFinite(in.atr14) ? (entryMid - atrMult * in.atr14) : Double.NaN;
        double stopLoss = round2(firstFinitePositive(stopByLow, stopByAtr));
        double riskDist = entryMid - stopLoss;
        if (!Double.isFinite(stopLoss) || !Double.isFinite(riskDist) || riskDist <= 0.0) {
            return Outcome.failure(
                    CauseCode.PLAN_INVALID,
                    OWNER,
                    Map.of(
                            "reason", "invalid_stop_or_risk_distance",
                            "entry_mid", round2(entryMid),
                            "stop_loss", stopLoss,
                            "rr_min", rrMin
                    )
            );
        }

        double baseTarget = Math.max(in.sma20, in.highLookback * highLookbackMult);
        double takeProfit = round2(Math.max(baseTarget, entryMid + rrMin * riskDist));
        if (!(stopLoss < entryLow && entryLow <= entryHigh && entryHigh < takeProfit)) {
            return Outcome.failure(
                    CauseCode.PLAN_INVALID,
                    OWNER,
                    Map.of(
                            "reason", "price_structure_invalid",
                            "entry_low", entryLow,
                            "entry_high", entryHigh,
                            "stop_loss", stopLoss,
                            "take_profit", takeProfit
                    )
            );
        }

        double rr = (takeProfit - entryMid) / (entryMid - stopLoss);
        if (!Double.isFinite(rr) || rr < rrMin) {
            return Outcome.failure(
                    CauseCode.PLAN_INVALID,
                    OWNER,
                    Map.of(
                            "reason", "rr_below_threshold",
                            "rr", round2(rr),
                            "rr_min", rrMin
                    )
            );
        }

        TradePlan plan = new TradePlan(
                true,
                round2(entryLow),
                round2(entryHigh),
                round2(stopLoss),
                round2(takeProfit),
                round2(rr)
        );
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("rr_min", rrMin);
        details.put("entry_buffer_pct", entryBufferPct);
        details.put("stop_buffer_pct", stopBuffer * 100.0);
        details.put("stop_atr_mult", atrMult);
        details.put("target_high_lookback_mult", highLookbackMult);
        return Outcome.success(plan, OWNER, details);
    }

    private boolean isFinitePositive(double value) {
        return Double.isFinite(value) && value > 0.0;
    }

    private double firstFinitePositive(double a, double b) {
        if (isFinitePositive(a)) return a;
        if (isFinitePositive(b)) return b;
        return Double.NaN;
    }

    private double clamp(double value, double min, double max) {
        if (!Double.isFinite(value)) return min;
        if (value < min) return min;
        if (value > max) return max;
        return value;
    }

    private double round2(double value) {
        if (!Double.isFinite(value)) {
            return value;
        }
        return Math.round(value * 100.0) / 100.0;
    }

    public static final class Input {
        public final double lastClose;
        public final double sma20;
        public final double lowLookback;
        public final double highLookback;
        public final double atr14;

        public Input(double lastClose, double sma20, double lowLookback, double highLookback, double atr14) {
            this.lastClose = lastClose;
            this.sma20 = sma20;
            this.lowLookback = lowLookback;
            this.highLookback = highLookback;
            this.atr14 = atr14;
        }
    }
}
