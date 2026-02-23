package com.stockbot.jp.plan;

import com.stockbot.core.diagnostics.CauseCode;
import com.stockbot.core.diagnostics.Outcome;
import com.stockbot.jp.config.Config;
import com.stockbot.jp.model.WatchlistAnalysis;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 模块说明：TradePlanBuilder（class）。
 * 主要职责：承载 plan 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class TradePlanBuilder {
    public static final String OWNER = "com.stockbot.jp.plan.TradePlanBuilder#build(...)";
    public static final String OWNER_WATCH = "com.stockbot.jp.plan.TradePlanBuilder#buildForWatchlist(...)";

    private final Config config;

/**
 * 方法说明：TradePlanBuilder，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public TradePlanBuilder(Config config) {
        this.config = config;
    }

/**
 * 方法说明：build，负责构建目标对象或输出内容。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public Outcome<TradePlan> build(Input in) {
        if (in == null) {
            return planFailure(
                    OWNER,
                    "input_null",
                    "计划无效：历史数据不足",
                    Map.of("missing_inputs", List.of("input"))
            );
        }

        List<String> missing = new ArrayList<>();
        if (!isFinitePositive(in.lastClose)) missing.add("last_close");
        if (!isFinitePositive(in.sma20)) missing.add("sma20");
        if (!isFinitePositive(in.lowLookback)) missing.add("low_lookback");
        if (!isFinitePositive(in.highLookback)) missing.add("high_lookback");
        if (!missing.isEmpty()) {
            return planFailure(
                    OWNER,
                    "missing_core_inputs",
                    "计划无效：历史数据不足",
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
        double maxEntryDeviationPct = Math.max(1.0, config.getDouble("plan.max_entry_deviation_pct", 8.0));
        double maxVolatilityPct = config.getDouble("risk.max_volatility_pct", 80.0);
        double minVolumeRatio = config.getDouble("risk.min_volume_ratio", 0.3);

        if (!isFinitePositive(in.atr14)) {
            return planFailure(
                    OWNER,
                    "atr_unavailable",
                    "计划无效：近期波动/成交量异常",
                    Map.of("atr14", in.atr14)
            );
        }
        if ((Double.isFinite(in.volatility20Pct) && in.volatility20Pct > maxVolatilityPct)
                || (Double.isFinite(in.volumeRatio20) && in.volumeRatio20 < minVolumeRatio)) {
            return planFailure(
                    OWNER,
                    "abnormal_volatility_or_liquidity",
                    "计划无效：近期波动/成交量异常",
                    Map.of(
                            "volatility20_pct", in.volatility20Pct,
                            "max_volatility_pct", maxVolatilityPct,
                            "volume_ratio20", in.volumeRatio20,
                            "min_volume_ratio", minVolumeRatio
                    )
            );
        }

        double entryMid = in.lastClose;
        double entryLow = round2(entryMid * (1.0 - entryBand));
        double entryHigh = round2(entryMid * (1.0 + entryBand));
        double entryDeviationPct = Math.abs(entryMid - in.sma20) / in.sma20 * 100.0;
        if (Double.isFinite(entryDeviationPct) && entryDeviationPct > maxEntryDeviationPct) {
            return planFailure(
                    OWNER,
                    "entry_deviation_too_large",
                    "计划无效：当前价已严重偏离安全入场区间",
                    Map.of(
                            "last_close", round2(entryMid),
                            "sma20", round2(in.sma20),
                            "entry_deviation_pct", round2(entryDeviationPct),
                            "max_entry_deviation_pct", round2(maxEntryDeviationPct)
                    )
            );
        }

        double stopByLow = in.lowLookback * (1.0 - stopBuffer);
        double stopByAtr = entryMid - atrMult * in.atr14;
        double stopLoss = round2(firstFinitePositive(stopByLow, stopByAtr));
        double riskDist = entryMid - stopLoss;
        if (!Double.isFinite(stopLoss) || !Double.isFinite(riskDist) || riskDist <= 0.0) {
            return planFailure(
                    OWNER,
                    "invalid_stop_or_risk_distance",
                    "计划无效：近期波动/成交量异常",
                    Map.of(
                            "entry_mid", round2(entryMid),
                            "stop_loss", stopLoss,
                            "rr_min", rrMin
                    )
            );
        }

        double baseTarget = Math.max(in.sma20, in.highLookback * highLookbackMult);
        double takeProfit = round2(Math.max(baseTarget, entryMid + rrMin * riskDist));
        if (!(stopLoss < entryLow && entryLow <= entryHigh && entryHigh < takeProfit)) {
            return planFailure(
                    OWNER,
                    "price_structure_invalid",
                    "计划无效：当前价已严重偏离安全入场区间",
                    Map.of(
                            "entry_low", entryLow,
                            "entry_high", entryHigh,
                            "stop_loss", stopLoss,
                            "take_profit", takeProfit
                    )
            );
        }

        double rr = (takeProfit - entryMid) / (entryMid - stopLoss);
        if (!Double.isFinite(rr) || rr < rrMin) {
            return planFailure(
                    OWNER,
                    "rr_below_threshold",
                    "计划无效：当前价已严重偏离安全入场区间",
                    Map.of(
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
        details.put("entry_deviation_pct", round2(entryDeviationPct));
        details.put("max_entry_deviation_pct", round2(maxEntryDeviationPct));
        return Outcome.success(plan, OWNER, details);
    }

    public Outcome<TradePlan> buildForWatchlist(WatchlistAnalysis w) {
        if (w == null) {
            return planFailure(
                    OWNER_WATCH,
                    "watchlist_row_missing",
                    "计划无效：历史数据不足",
                    Map.of("missing_inputs", List.of("watchlist_row"))
            );
        }

        JSONObject root = safeJson(w.technicalIndicatorsJson);
        double lastClose = firstFinitePositive(
                root.optDouble("last_close", Double.NaN),
                w.lastClose
        );
        double sma20 = root.optDouble("sma20", Double.NaN);
        double lowLookback = firstFinitePositive(
                root.optDouble("low_lookback", Double.NaN),
                root.optDouble("bollinger_lower", Double.NaN)
        );
        double highLookback = firstFinitePositive(
                root.optDouble("high_lookback", Double.NaN),
                root.optDouble("bollinger_upper", Double.NaN)
        );
        double atr14 = root.optDouble("atr14", Double.NaN);
        double volatility20Pct = root.optDouble("volatility20_pct", Double.NaN);
        double volumeRatio20 = root.optDouble("volume_ratio20", Double.NaN);

        List<String> missing = new ArrayList<>();
        if (!isFinitePositive(lastClose)) missing.add("last_close");
        if (!isFinitePositive(sma20)) missing.add("sma20");
        if (!isFinitePositive(lowLookback)) missing.add("low_lookback");
        if (!isFinitePositive(highLookback)) missing.add("high_lookback");

        if (!missing.isEmpty()) {
            return planFailure(
                    OWNER_WATCH,
                    "missing_watchlist_inputs",
                    "计划无效：历史数据不足",
                    Map.of(
                            "missing_inputs", missing,
                            "watch_item", safeText(w.watchItem),
                            "ticker", safeText(w.ticker).toUpperCase(Locale.ROOT)
                    )
            );
        }

        return build(new Input(lastClose, sma20, lowLookback, highLookback, atr14, volatility20Pct, volumeRatio20));
    }

/**
 * 方法说明：isFinitePositive，负责判断条件是否满足。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private Outcome<TradePlan> planFailure(String owner, String reasonKey, String userMessage, Map<String, Object> extras) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("reason", safeText(reasonKey));
        details.put("user_message", safeText(userMessage));
        if (extras != null && !extras.isEmpty()) {
            details.putAll(extras);
        }
        return Outcome.failure(CauseCode.PLAN_INVALID, owner, details);
    }

    private boolean isFinitePositive(double value) {
        return Double.isFinite(value) && value > 0.0;
    }

/**
 * 方法说明：firstFinitePositive，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double firstFinitePositive(double a, double b) {
        if (isFinitePositive(a)) return a;
        if (isFinitePositive(b)) return b;
        return Double.NaN;
    }

/**
 * 方法说明：clamp，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double clamp(double value, double min, double max) {
        if (!Double.isFinite(value)) return min;
        if (value < min) return min;
        if (value > max) return max;
        return value;
    }

/**
 * 方法说明：round2，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double round2(double value) {
        if (!Double.isFinite(value)) {
            return value;
        }
        return Math.round(value * 100.0) / 100.0;
    }

    private JSONObject safeJson(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return new JSONObject();
        }
        try {
            return new JSONObject(raw);
        } catch (Exception ignored) {
            return new JSONObject();
        }
    }

    private String safeText(String text) {
        return text == null ? "" : text.trim();
    }

    public static final class Input {
        public final double lastClose;
        public final double sma20;
        public final double lowLookback;
        public final double highLookback;
        public final double atr14;
        public final double volatility20Pct;
        public final double volumeRatio20;

        public Input(double lastClose, double sma20, double lowLookback, double highLookback, double atr14) {
            this(lastClose, sma20, lowLookback, highLookback, atr14, Double.NaN, Double.NaN);
        }

        public Input(
                double lastClose,
                double sma20,
                double lowLookback,
                double highLookback,
                double atr14,
                double volatility20Pct,
                double volumeRatio20
        ) {
            this.lastClose = lastClose;
            this.sma20 = sma20;
            this.lowLookback = lowLookback;
            this.highLookback = highLookback;
            this.atr14 = atr14;
            this.volatility20Pct = volatility20Pct;
            this.volumeRatio20 = volumeRatio20;
        }
    }
}
