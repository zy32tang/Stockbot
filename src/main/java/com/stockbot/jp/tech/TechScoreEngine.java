package com.stockbot.jp.tech;

import com.stockbot.jp.config.Config;
import com.stockbot.jp.model.BarDaily;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class TechScoreEngine {
    private final int maShort;
    private final int maMid;
    private final int maLong;
    private final double biasSafe;
    private final double biasRisk;
    private final int volumeAvgWindow;
    private final double stopMaxPctIn;
    private final double stopMaxPctRisk;

    public TechScoreEngine() {
        this(null);
    }

    public TechScoreEngine(Config config) {
        this.maShort = positiveInt(config, "tech.ma.short", 5);
        this.maMid = positiveInt(config, "tech.ma.mid", 10);
        this.maLong = positiveInt(config, "tech.ma.long", 20);
        this.biasSafe = positiveDouble(config, "tech.bias.safe", 0.05);
        this.biasRisk = positiveDouble(config, "tech.bias.risk", 0.08);
        this.volumeAvgWindow = positiveInt(config, "tech.volume.avg_window", 20);
        this.stopMaxPctIn = positiveDouble(config, "tech.stop.max_pct_in", 0.04);
        this.stopMaxPctRisk = positiveDouble(config, "tech.stop.max_pct_risk", 0.06);
    }

    public TechScoreResult evaluate(String ticker, String companyName, List<BarDaily> bars) {
        List<BarDaily> safeBars = bars == null ? List.of() : bars;
        double price = lastClose(safeBars);
        double lastVolume = lastVolume(safeBars);
        int size = safeBars.size();

        if (size < 6 || !isFinitePositive(price) || !isFinitePositive(lastVolume)) {
            return missingResult(ticker, companyName, price, size, lastVolume);
        }

        int effectiveShort = clampWindow(maShort, size, 3);
        int effectiveMid = clampWindow(maMid, size, 6);
        int effectiveLong = resolveLongWindow(size);
        int effectiveVolumeWindow = resolveVolumeWindow(size);

        boolean degraded = effectiveShort != maShort
                || effectiveMid != maMid
                || effectiveLong != maLong
                || effectiveVolumeWindow != volumeAvgWindow;
        DataStatus dataStatus = degraded ? DataStatus.DEGRADED : DataStatus.OK;

        double ma5 = smaClose(safeBars, effectiveShort);
        double ma10 = smaClose(safeBars, effectiveMid);
        double ma20 = smaClose(safeBars, effectiveLong);
        double avgVol = smaVolume(safeBars, effectiveVolumeWindow);
        if (!isFinitePositive(avgVol)) {
            return missingResult(ticker, companyName, price, size, lastVolume);
        }
        double volRatio = lastVolume / avgVol;
        double bias = ma5 > 0.0 ? (price - ma5) / ma5 : 0.0;

        int trendStructure = scoreTrendStructure(price, ma5, ma10, ma20);
        int biasRiskScore = scoreBiasRisk(bias);
        int volumeConfirm = scoreVolumeConfirm(volRatio);

        double swingLow = recentLow(safeBars, Math.min(size, Math.max(10, maMid)));
        boolean bullishStructure = ma5 > ma10 && ma10 > ma20;
        double stopLine = resolveStopLine(price, ma10, ma20, swingLow, bullishStructure);
        double stopPct = safeStopPct(price, stopLine);
        int executionQuality = scoreExecutionQuality(stopPct);

        int trendStrength = clampInt(
                trendStructure + biasRiskScore + volumeConfirm + executionQuality,
                0,
                100
        );

        boolean bearConfirm = price < ma20 && ma5 < ma10 && ma10 < ma20;
        RiskLevel riskLevel = resolveRiskLevel(bias, stopPct, bearishOrDefensive(bearConfirm), bullishStructure);

        double ma20Prev = smaCloseAtOffset(safeBars, effectiveLong, 1);
        boolean ma20Down = isFinitePositive(ma20Prev) && ma20 < ma20Prev;
        SignalStatus signalStatus = resolveSignalStatus(
                dataStatus,
                bias,
                bullishStructure,
                volumeConfirm,
                ma5,
                ma10,
                ma20,
                price,
                ma20Down
        );

        List<TechChecklistItem> checklist = buildChecklist(
                dataStatus,
                degraded,
                size,
                price,
                ma5,
                ma10,
                ma20,
                effectiveShort,
                effectiveMid,
                effectiveLong,
                bias,
                volRatio,
                effectiveVolumeWindow,
                stopLine,
                stopPct,
                bullishStructure
        );

        return TechScoreResult.builder()
                .ticker(safeText(ticker))
                .companyName(safeText(companyName))
                .price(price)
                .trendStrength(trendStrength)
                .subscores(new TechSubscores(trendStructure, biasRiskScore, volumeConfirm, executionQuality))
                .signalStatus(signalStatus)
                .riskLevel(riskLevel)
                .ma5(ma5)
                .ma10(ma10)
                .ma20(ma20)
                .bias(bias)
                .volRatio(volRatio)
                .stopLine(stopLine)
                .stopPct(stopPct)
                .dataStatus(dataStatus)
                .checklist(checklist)
                .build();
    }

    private List<TechChecklistItem> buildChecklist(
            DataStatus dataStatus,
            boolean degraded,
            int size,
            double price,
            double ma5,
            double ma10,
            double ma20,
            int effectiveShort,
            int effectiveMid,
            int effectiveLong,
            double bias,
            double volRatio,
            int effectiveVolumeWindow,
            double stopLine,
            double stopPct,
            boolean bullishStructure
    ) {
        List<TechChecklistItem> items = new ArrayList<>();
        ChecklistStatus trendStatus;
        if (bullishStructure && price >= ma10) {
            trendStatus = ChecklistStatus.PASS;
        } else if (ma5 > ma10) {
            trendStatus = ChecklistStatus.WATCH;
        } else {
            trendStatus = ChecklistStatus.FAIL;
        }
        items.add(item(
                trendStatus,
                "Trend structure",
                String.format(Locale.US, "P=%.2f MA5/10/20=%.2f/%.2f/%.2f", price, ma5, ma10, ma20),
                "MA5>MA10>MA20 and price>=MA10"
        ));

        ChecklistStatus biasStatus;
        if (bias > biasRisk) {
            biasStatus = ChecklistStatus.FAIL;
        } else if (Math.abs(bias) <= biasSafe) {
            biasStatus = ChecklistStatus.PASS;
        } else {
            biasStatus = ChecklistStatus.WATCH;
        }
        items.add(item(
                biasStatus,
                "Bias risk",
                String.format(Locale.US, "bias=%.2f%%", bias * 100.0),
                String.format(Locale.US, "|bias|<=%.2f%% safe, >%.2f%% risk", biasSafe * 100.0, biasRisk * 100.0)
        ));

        ChecklistStatus volumeStatus = volRatio >= 1.5
                ? ChecklistStatus.PASS
                : (volRatio >= 1.1 ? ChecklistStatus.WATCH : ChecklistStatus.FAIL);
        items.add(item(
                volumeStatus,
                "Volume confirm",
                String.format(Locale.US, "vol_ratio=%.2f", volRatio),
                ">=1.5 PASS, 1.1~1.5 WATCH, <1.1 FAIL"
        ));

        ChecklistStatus stopStatus = stopPct <= stopMaxPctIn
                ? ChecklistStatus.PASS
                : (stopPct <= stopMaxPctRisk ? ChecklistStatus.WATCH : ChecklistStatus.FAIL);
        items.add(item(
                stopStatus,
                "Execution stop",
                String.format(Locale.US, "stop_line=%.2f stop_pct=%.2f%%", stopLine, stopPct * 100.0),
                String.format(Locale.US, "<=%.2f%% PASS, <=%.2f%% WATCH", stopMaxPctIn * 100.0, stopMaxPctRisk * 100.0)
        ));

        items.add(item(
                ChecklistStatus.PASS,
                "Stop line basis",
                bullishStructure ? "min(MA10, recent swing low)" : "MA20 or recent swing low",
                "Stable and explainable stop anchor"
        ));

        if (dataStatus == DataStatus.DEGRADED || degraded) {
            items.add(item(
                    ChecklistStatus.WATCH,
                    "Data window downgraded",
                    String.format(
                            Locale.US,
                            "bars=%d MA(5/10/20)->(%d/%d/%d), vol_window=%d",
                            size,
                            effectiveShort,
                            effectiveMid,
                            effectiveLong,
                            effectiveVolumeWindow
                    ),
                    "Insufficient history for full window, downgraded to keep stable output"
            ));
        }
        return items;
    }

    private RiskLevel resolveRiskLevel(boolean biasTooHigh, boolean bearConfirm, double stopPct, boolean bullishStructure, double absBias) {
        if (biasTooHigh || bearConfirm || stopPct > stopMaxPctRisk) {
            return RiskLevel.RISK;
        }
        if (bullishStructure && absBias <= biasSafe && stopPct <= stopMaxPctIn) {
            return RiskLevel.IN;
        }
        return RiskLevel.NEAR;
    }

    private RiskLevel resolveRiskLevel(double bias, double stopPct, boolean bearConfirm, boolean bullishStructure) {
        return resolveRiskLevel(bias > biasRisk, bearConfirm, stopPct, bullishStructure, Math.abs(bias));
    }

    private boolean bearishOrDefensive(boolean bearConfirm) {
        return bearConfirm;
    }

    private SignalStatus resolveSignalStatus(
            DataStatus dataStatus,
            double bias,
            boolean bullishStructure,
            int volumeConfirm,
            double ma5,
            double ma10,
            double ma20,
            double price,
            boolean ma20Down
    ) {
        if (dataStatus == DataStatus.MISSING || bias > biasRisk) {
            return SignalStatus.DEFEND;
        }
        if (bullishStructure && volumeConfirm >= 14) {
            return SignalStatus.BULL;
        }
        if ((ma5 < ma10 && ma10 < ma20) || (price < ma20 && ma20Down)) {
            return SignalStatus.BEAR;
        }
        return SignalStatus.NEUTRAL;
    }

    private TechScoreResult missingResult(String ticker, String companyName, double price, int bars, double lastVolume) {
        String reason = bars < 6
                ? "insufficient_history(<6 bars)"
                : "missing_volume";
        List<TechChecklistItem> checklist = new ArrayList<>();
        checklist.add(item(
                ChecklistStatus.FAIL,
                "Data quality",
                String.format(Locale.US, "bars=%d last_volume=%.2f", Math.max(0, bars), lastVolume),
                "Need >=6 bars and valid volume"
        ));
        checklist.add(item(
                ChecklistStatus.FAIL,
                "MISSING reason",
                reason,
                "Technical score fallback to DEFEND/RISK"
        ));
        return TechScoreResult.builder()
                .ticker(safeText(ticker))
                .companyName(safeText(companyName))
                .price(isFinitePositive(price) ? price : 0.0)
                .trendStrength(0)
                .subscores(new TechSubscores(0, 0, 0, 0))
                .signalStatus(SignalStatus.DEFEND)
                .riskLevel(RiskLevel.RISK)
                .ma5(0.0)
                .ma10(0.0)
                .ma20(0.0)
                .bias(0.0)
                .volRatio(0.0)
                .stopLine(0.0)
                .stopPct(0.0)
                .dataStatus(DataStatus.MISSING)
                .checklist(checklist)
                .build();
    }

    private TechChecklistItem item(ChecklistStatus status, String label, String value, String rule) {
        return TechChecklistItem.builder()
                .status(status)
                .label(label)
                .value(value)
                .rule(rule)
                .build();
    }

    private int scoreTrendStructure(double price, double ma5, double ma10, double ma20) {
        if (ma5 > ma10 && ma10 > ma20 && price >= ma10) {
            return 45;
        }
        if (ma5 > ma10 && ma10 <= ma20) {
            return 28;
        }
        if (price >= ma20) {
            return 16;
        }
        return 8;
    }

    private int scoreBiasRisk(double bias) {
        if (bias > biasRisk) {
            return -18;
        }
        double abs = Math.abs(bias);
        if (abs <= biasSafe) {
            return 8;
        }
        if (bias > biasSafe) {
            return -6;
        }
        return -4;
    }

    private int scoreVolumeConfirm(double volRatio) {
        if (volRatio >= 1.5) {
            return 22;
        }
        if (volRatio >= 1.1) {
            return 14;
        }
        return 6;
    }

    private int scoreExecutionQuality(double stopPct) {
        if (stopPct <= stopMaxPctIn) {
            return 13;
        }
        if (stopPct <= stopMaxPctRisk) {
            return 8;
        }
        return 3;
    }

    private double resolveStopLine(double price, double ma10, double ma20, double swingLow, boolean bullish) {
        double stopLine;
        if (bullish) {
            stopLine = minPositive(ma10, swingLow);
        } else {
            stopLine = firstPositive(ma20, swingLow);
        }
        if (!isFinitePositive(stopLine)) {
            stopLine = price * (1.0 - stopMaxPctRisk);
        }
        if (stopLine >= price) {
            stopLine = price * (1.0 - Math.min(0.02, stopMaxPctIn));
        }
        return stopLine;
    }

    private double safeStopPct(double price, double stopLine) {
        if (!isFinitePositive(price) || !Double.isFinite(stopLine)) {
            return 1.0;
        }
        double value = (price - stopLine) / price;
        if (!Double.isFinite(value)) {
            return 1.0;
        }
        return Math.max(0.0, value);
    }

    private int resolveLongWindow(int barsSize) {
        if (barsSize >= maLong) {
            return maLong;
        }
        if (barsSize >= maMid) {
            return maMid;
        }
        return clampWindow(barsSize, barsSize, 6);
    }

    private int resolveVolumeWindow(int barsSize) {
        if (barsSize >= volumeAvgWindow) {
            return volumeAvgWindow;
        }
        if (barsSize >= 10) {
            return 10;
        }
        return clampWindow(barsSize, barsSize, 6);
    }

    private int clampWindow(int configured, int barsSize, int min) {
        int value = Math.max(1, configured);
        int capped = Math.min(value, Math.max(1, barsSize));
        if (barsSize >= min) {
            return Math.max(min, capped);
        }
        return capped;
    }

    private double lastClose(List<BarDaily> bars) {
        for (int i = bars.size() - 1; i >= 0; i--) {
            BarDaily b = bars.get(i);
            if (b != null && isFinitePositive(b.close)) {
                return b.close;
            }
        }
        return Double.NaN;
    }

    private double lastVolume(List<BarDaily> bars) {
        for (int i = bars.size() - 1; i >= 0; i--) {
            BarDaily b = bars.get(i);
            if (b != null && isFinitePositive(b.volume)) {
                return b.volume;
            }
        }
        return Double.NaN;
    }

    private double smaClose(List<BarDaily> bars, int period) {
        if (bars == null || bars.isEmpty() || period <= 0 || bars.size() < period) {
            return 0.0;
        }
        double sum = 0.0;
        int start = bars.size() - period;
        for (int i = start; i < bars.size(); i++) {
            BarDaily bar = bars.get(i);
            double close = bar == null ? Double.NaN : bar.close;
            if (!isFinitePositive(close)) {
                return 0.0;
            }
            sum += close;
        }
        return sum / period;
    }

    private double smaCloseAtOffset(List<BarDaily> bars, int period, int offset) {
        if (bars == null || bars.isEmpty() || period <= 0 || offset < 0) {
            return 0.0;
        }
        int endExclusive = bars.size() - offset;
        int start = endExclusive - period;
        if (start < 0 || endExclusive > bars.size()) {
            return 0.0;
        }
        double sum = 0.0;
        for (int i = start; i < endExclusive; i++) {
            BarDaily bar = bars.get(i);
            double close = bar == null ? Double.NaN : bar.close;
            if (!isFinitePositive(close)) {
                return 0.0;
            }
            sum += close;
        }
        return sum / period;
    }

    private double smaVolume(List<BarDaily> bars, int period) {
        if (bars == null || bars.isEmpty() || period <= 0 || bars.size() < period) {
            return 0.0;
        }
        double sum = 0.0;
        int start = bars.size() - period;
        for (int i = start; i < bars.size(); i++) {
            BarDaily bar = bars.get(i);
            double volume = bar == null ? Double.NaN : bar.volume;
            if (!isFinitePositive(volume)) {
                return 0.0;
            }
            sum += volume;
        }
        return sum / period;
    }

    private double recentLow(List<BarDaily> bars, int period) {
        if (bars == null || bars.isEmpty() || period <= 0) {
            return 0.0;
        }
        int start = Math.max(0, bars.size() - period);
        double min = Double.POSITIVE_INFINITY;
        for (int i = start; i < bars.size(); i++) {
            BarDaily bar = bars.get(i);
            if (bar == null || !isFinitePositive(bar.low)) {
                continue;
            }
            min = Math.min(min, bar.low);
        }
        if (!Double.isFinite(min)) {
            return 0.0;
        }
        return min;
    }

    private int positiveInt(Config config, String key, int fallback) {
        if (config == null) {
            return Math.max(1, fallback);
        }
        return Math.max(1, config.getInt(key, fallback));
    }

    private double positiveDouble(Config config, String key, double fallback) {
        if (config == null) {
            return Math.max(0.0, fallback);
        }
        return Math.max(0.0, config.getDouble(key, fallback));
    }

    private String safeText(String value) {
        return value == null ? "" : value.trim();
    }

    private boolean isFinitePositive(double value) {
        return Double.isFinite(value) && value > 0.0;
    }

    private double firstPositive(double first, double second) {
        if (isFinitePositive(first)) {
            return first;
        }
        if (isFinitePositive(second)) {
            return second;
        }
        return 0.0;
    }

    private double minPositive(double a, double b) {
        if (isFinitePositive(a) && isFinitePositive(b)) {
            return Math.min(a, b);
        }
        return firstPositive(a, b);
    }

    private int clampInt(int value, int min, int max) {
        if (value < min) {
            return min;
        }
        if (value > max) {
            return max;
        }
        return value;
    }
}
