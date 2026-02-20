package com.stockbot.jp.indicator;

import com.stockbot.jp.model.BarDaily;
import com.stockbot.jp.model.IndicatorSnapshot;

import java.util.List;

/**
 * 模块说明：IndicatorEngine（class）。
 * 主要职责：承载 indicator 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class IndicatorEngine {
    private static final List<String> COMPUTED_INDICATORS = List.of(
            "sma20", "sma60", "sma60_prev5", "sma120",
            "rsi14", "atr14", "atr_pct",
            "bollinger_upper", "bollinger_middle", "bollinger_lower",
            "drawdown120_pct", "volatility20_pct",
            "avg_volume20", "volume_ratio20",
            "pct_from_sma20", "pct_from_sma60",
            "return3d_pct", "return5d_pct", "return10d_pct",
            "low_lookback", "high_lookback"
    );

    private final int stopLossLookbackDays;

/**
 * 方法说明：IndicatorEngine，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public IndicatorEngine() {
        this(20);
    }

/**
 * 方法说明：IndicatorEngine，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public IndicatorEngine(int stopLossLookbackDays) {
        this.stopLossLookbackDays = Math.max(5, stopLossLookbackDays);
    }

/**
 * 方法说明：compute，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public IndicatorSnapshot compute(List<BarDaily> bars) {
        if (bars == null || bars.isEmpty()) {
            return null;
        }

        int size = bars.size();
        double[] closes = new double[size];
        double[] highs = new double[size];
        double[] lows = new double[size];
        double[] volumes = new double[size];
        for (int i = 0; i < size; i++) {
            BarDaily bar = bars.get(i);
            closes[i] = bar.close;
            highs[i] = bar.high;
            lows[i] = bar.low;
            volumes[i] = bar.volume;
        }

        double lastClose = closes[size - 1];
        double sma20 = sma(closes, 20);
        double sma60 = sma(closes, 60);
        double sma60Prev5 = smaAtOffset(closes, 60, 5);
        double sma60Slope = sma60 - sma60Prev5;
        double sma120 = sma(closes, 120);
        double rsi14 = rsi(closes, 14);
        double atr14 = atr(highs, lows, closes, 14);
        double atrPct = safePct(atr14, lastClose);

        Bollinger band = bollinger(closes, 20, 2.0);
        double drawdown120Pct = drawdownPct(closes, 120);
        double volatility20Pct = volatilityPct(closes, 20);
        double avgVolume20 = sma(volumes, 20);
        double volumeRatio20 = avgVolume20 <= 0 ? 0.0 : volumes[size - 1] / avgVolume20;

        double pctFromSma20 = sma20 <= 0 ? 0.0 : (lastClose - sma20) / sma20 * 100.0;
        double pctFromSma60 = sma60 <= 0 ? 0.0 : (lastClose - sma60) / sma60 * 100.0;

        double return3dPct = returnPct(closes, 3);
        double return5dPct = returnPct(closes, 5);
        double return10dPct = returnPct(closes, 10);
        double lowLookback = minRecent(lows, stopLossLookbackDays);
        double highLookback = maxRecent(highs, stopLossLookbackDays);

        return new IndicatorSnapshot(
                lastClose,
                sma20,
                sma60,
                sma60Prev5,
                sma60Slope,
                sma120,
                rsi14,
                atr14,
                atrPct,
                band.upper,
                band.middle,
                band.lower,
                drawdown120Pct,
                volatility20Pct,
                avgVolume20,
                volumeRatio20,
                pctFromSma20,
                pctFromSma60,
                return3dPct,
                return5dPct,
                return10dPct,
                lowLookback,
                highLookback
        );
    }

/**
 * 方法说明：sma，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double sma(double[] values, int period) {
        if (values.length < period || period <= 0) {
            return 0.0;
        }
        double sum = 0.0;
        for (int i = values.length - period; i < values.length; i++) {
            sum += values[i];
        }
        return sum / period;
    }

/**
 * 方法说明：smaAtOffset，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double smaAtOffset(double[] values, int period, int offset) {
        if (period <= 0 || offset < 0 || values.length < period + offset) {
            return 0.0;
        }
        int endExclusive = values.length - offset;
        int startInclusive = endExclusive - period;
        if (startInclusive < 0) {
            return 0.0;
        }
        double sum = 0.0;
        for (int i = startInclusive; i < endExclusive; i++) {
            sum += values[i];
        }
        return sum / period;
    }

/**
 * 方法说明：rsi，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double rsi(double[] closes, int period) {
        if (closes.length <= period) {
            return 50.0;
        }
        double gain = 0.0;
        double loss = 0.0;
        for (int i = 1; i <= period; i++) {
            double diff = closes[i] - closes[i - 1];
            if (diff >= 0) {
                gain += diff;
            } else {
                loss -= diff;
            }
        }
        double avgGain = gain / period;
        double avgLoss = loss / period;

        for (int i = period + 1; i < closes.length; i++) {
            double diff = closes[i] - closes[i - 1];
            double currentGain = diff > 0 ? diff : 0.0;
            double currentLoss = diff < 0 ? -diff : 0.0;
            avgGain = (avgGain * (period - 1) + currentGain) / period;
            avgLoss = (avgLoss * (period - 1) + currentLoss) / period;
        }
        if (avgLoss == 0.0) {
            return 100.0;
        }
        double rs = avgGain / avgLoss;
        return 100.0 - (100.0 / (1.0 + rs));
    }

/**
 * 方法说明：atr，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double atr(double[] highs, double[] lows, double[] closes, int period) {
        if (closes.length <= period) {
            return 0.0;
        }
        double sum = 0.0;
        int start = closes.length - period;
        for (int i = start; i < closes.length; i++) {
            double prevClose = i == 0 ? closes[i] : closes[i - 1];
            double tr1 = highs[i] - lows[i];
            double tr2 = Math.abs(highs[i] - prevClose);
            double tr3 = Math.abs(lows[i] - prevClose);
            double tr = Math.max(tr1, Math.max(tr2, tr3));
            sum += tr;
        }
        return sum / period;
    }

/**
 * 方法说明：bollinger，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private Bollinger bollinger(double[] closes, int period, double k) {
        if (closes.length < period) {
            double last = closes[closes.length - 1];
            return new Bollinger(last, last, last);
        }
        double mean = sma(closes, period);
        double sumSq = 0.0;
        for (int i = closes.length - period; i < closes.length; i++) {
            double d = closes[i] - mean;
            sumSq += d * d;
        }
        double stdev = Math.sqrt(sumSq / period);
        return new Bollinger(mean + k * stdev, mean, mean - k * stdev);
    }

/**
 * 方法说明：drawdownPct，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double drawdownPct(double[] closes, int lookback) {
        if (closes.length == 0) {
            return 0.0;
        }
        int start = Math.max(0, closes.length - lookback);
        double peak = closes[start];
        for (int i = start; i < closes.length; i++) {
            peak = Math.max(peak, closes[i]);
        }
        if (peak <= 0) {
            return 0.0;
        }
        return (closes[closes.length - 1] - peak) / peak * 100.0;
    }

/**
 * 方法说明：volatilityPct，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double volatilityPct(double[] closes, int period) {
        if (closes.length <= period) {
            return 0.0;
        }
        int returnsCount = period;
        double[] returns = new double[returnsCount];
        int start = closes.length - period - 1;
        for (int i = 0; i < returnsCount; i++) {
            double prev = closes[start + i];
            double next = closes[start + i + 1];
            if (prev <= 0 || next <= 0) {
                returns[i] = 0.0;
            } else {
                returns[i] = Math.log(next / prev);
            }
        }
        double mean = 0.0;
        for (double r : returns) {
            mean += r;
        }
        mean /= returnsCount;

        double var = 0.0;
        for (double r : returns) {
            double d = r - mean;
            var += d * d;
        }
        var /= returnsCount;
        double dailyVol = Math.sqrt(var);
        return dailyVol * Math.sqrt(252.0) * 100.0;
    }

/**
 * 方法说明：returnPct，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double returnPct(double[] closes, int days) {
        if (closes.length <= days) {
            return 0.0;
        }
        double prev = closes[closes.length - 1 - days];
        if (prev == 0.0) {
            return 0.0;
        }
        return (closes[closes.length - 1] - prev) / prev * 100.0;
    }

/**
 * 方法说明：safePct，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double safePct(double value, double base) {
        if (base == 0.0) {
            return 0.0;
        }
        return value / base * 100.0;
    }

/**
 * 方法说明：minRecent，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double minRecent(double[] values, int period) {
        if (values == null || values.length == 0 || period <= 0) {
            return 0.0;
        }
        int start = Math.max(0, values.length - period);
        double min = Double.POSITIVE_INFINITY;
        for (int i = start; i < values.length; i++) {
            min = Math.min(min, values[i]);
        }
        if (!Double.isFinite(min)) {
            return 0.0;
        }
        return min;
    }

/**
 * 方法说明：maxRecent，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double maxRecent(double[] values, int period) {
        if (values == null || values.length == 0 || period <= 0) {
            return 0.0;
        }
        int start = Math.max(0, values.length - period);
        double max = Double.NEGATIVE_INFINITY;
        for (int i = start; i < values.length; i++) {
            max = Math.max(max, values[i]);
        }
        if (!Double.isFinite(max)) {
            return 0.0;
        }
        return max;
    }

    private static final class Bollinger {
        final double upper;
        final double middle;
        final double lower;

        private Bollinger(double upper, double middle, double lower) {
            this.upper = upper;
            this.middle = middle;
            this.lower = lower;
        }
    }

/**
 * 方法说明：computedIndicators，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static List<String> computedIndicators() {
        return COMPUTED_INDICATORS;
    }
}
