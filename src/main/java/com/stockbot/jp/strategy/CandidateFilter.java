package com.stockbot.jp.strategy;

import com.stockbot.jp.config.Config;
import com.stockbot.jp.model.BarDaily;
import com.stockbot.jp.model.FilterDecision;
import com.stockbot.jp.model.IndicatorSnapshot;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 模块说明：CandidateFilter（class）。
 * 主要职责：承载 strategy 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class CandidateFilter {
    private static final List<String> HARD_RULE_NAMES = List.of(
            "history_too_short",
            "price_out_of_range",
            "avg_volume_too_low",
            "drawdown_too_deep",
            "too_far_above_sma60",
            "short_term_drop_too_fast"
    );
    private static final List<String> SIGNAL_RULE_NAMES = List.of(
            "pullback_detected",
            "rsi_rebound_zone",
            "price_near_or_below_sma20",
            "near_lower_bollinger",
            "short_term_rebound",
            "volume_support"
    );

    private final Config config;

/**
 * 方法说明：CandidateFilter，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public CandidateFilter(Config config) {
        this.config = config;
    }

/**
 * 方法说明：evaluate，负责评估条件并输出判定结论。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public FilterDecision evaluate(List<BarDaily> bars, IndicatorSnapshot ind) {
        List<String> reasons = new ArrayList<>();
        Map<String, Object> metrics = new LinkedHashMap<>();
        int minHistory = Math.max(120, config.getInt("scan.min_history_bars", 180));
        double minPrice = config.getDouble("scan.min_price", 100.0);
        double maxPrice = config.getDouble("scan.max_price", 100000.0);
        double minAvgVolume = config.getDouble("scan.min_avg_volume", 100000.0);
        double pullbackThreshold = config.getDouble("filter.pullback_threshold_pct", -8.0);
        double maxDrawdown = config.getDouble("filter.max_drawdown_pct", -45.0);
        double rsiFloor = config.getDouble("filter.rsi_floor", 20.0);
        double rsiCeiling = config.getDouble("filter.rsi_ceiling", 55.0);
        double maxPctFromSma20 = config.getDouble("filter.max_pct_from_sma20", 2.0);
        double maxPctFromSma60 = config.getDouble("filter.max_pct_from_sma60", 6.0);
        double bandProximity = config.getDouble("filter.band_proximity_pct", 3.0);
        int minSignals = Math.max(1, config.getInt("filter.min_signals", 3));
        double hardMaxDrop3d = config.getDouble("filter.hard.max_drop_3d_pct", -8.0);

        boolean hardPass = true;
        if (bars.size() < minHistory) {
            hardPass = false;
            reasons.add("history_too_short");
        }
        if (ind.lastClose < minPrice || ind.lastClose > maxPrice) {
            hardPass = false;
            reasons.add("price_out_of_range");
        }
        if (ind.avgVolume20 < minAvgVolume) {
            hardPass = false;
            reasons.add("avg_volume_too_low");
        }
        if (ind.drawdown120Pct < maxDrawdown) {
            hardPass = false;
            reasons.add("drawdown_too_deep");
        }
        if (ind.pctFromSma60 > maxPctFromSma60) {
            hardPass = false;
            reasons.add("too_far_above_sma60");
        }
        if (ind.return3dPct < hardMaxDrop3d) {
            hardPass = false;
            reasons.add("short_term_drop_too_fast");
        }

        int signals = 0;
        if (ind.drawdown120Pct <= pullbackThreshold) {
            signals++;
            reasons.add("pullback_detected");
        }
        if (ind.rsi14 >= rsiFloor && ind.rsi14 <= rsiCeiling) {
            signals++;
            reasons.add("rsi_rebound_zone");
        }
        if (ind.pctFromSma20 <= maxPctFromSma20) {
            signals++;
            reasons.add("price_near_or_below_sma20");
        }
        if (ind.lastClose <= ind.bollingerLower * (1.0 + bandProximity / 100.0)) {
            signals++;
            reasons.add("near_lower_bollinger");
        }
        if (ind.return3dPct > 0 || ind.return5dPct > 0) {
            signals++;
            reasons.add("short_term_rebound");
        }
        if (ind.volumeRatio20 >= 1.0) {
            signals++;
            reasons.add("volume_support");
        }

        metrics.put("signal_count", signals);
        metrics.put("min_signal_required", minSignals);
        metrics.put("drawdown120_pct", ind.drawdown120Pct);
        metrics.put("rsi14", ind.rsi14);
        metrics.put("pct_from_sma20", ind.pctFromSma20);
        metrics.put("pct_from_sma60", ind.pctFromSma60);
        metrics.put("return3d_pct", ind.return3dPct);
        metrics.put("return5d_pct", ind.return5dPct);
        metrics.put("volume_ratio20", ind.volumeRatio20);

        boolean passed = hardPass && signals >= minSignals;
        return new FilterDecision(passed, reasons, metrics);
    }

/**
 * 方法说明：hardRuleNames，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static List<String> hardRuleNames() {
        return HARD_RULE_NAMES;
    }

/**
 * 方法说明：signalRuleNames，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static List<String> signalRuleNames() {
        return SIGNAL_RULE_NAMES;
    }
}
