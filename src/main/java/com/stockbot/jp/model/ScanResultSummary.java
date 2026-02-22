package com.stockbot.jp.model;

import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Aggregated scan summary for one run.
 */
public final class ScanResultSummary {
    public final int total;
    public final int fetchCoverage;
    public final int indicatorCoverage;

    // Tradable-denominator coverage (exclude stale/history_short/non-tradable/no_data).
    public final int tradableDenominator;
    public final int tradableIndicatorCoverage;

    // Precomputed percentages for report/debug output.
    public final double marketScanIndicatorCoverageRawPct;
    public final double marketScanIndicatorCoverageTradablePct;

    // Excluded reasons from tradable denominator.
    public final Map<String, Integer> breakdownDenominatorExcluded;

    public final Map<ScanFailureReason, Integer> failureCounts;
    public final Map<ScanFailureReason, Integer> requestFailureCounts;
    public final Map<DataInsufficientReason, Integer> insufficientCounts;

    public ScanResultSummary(
            int total,
            int fetchCoverage,
            int indicatorCoverage,
            Map<ScanFailureReason, Integer> failureCounts,
            Map<ScanFailureReason, Integer> requestFailureCounts,
            Map<DataInsufficientReason, Integer> insufficientCounts
    ) {
        this(
                total,
                fetchCoverage,
                indicatorCoverage,
                Math.max(0, total),
                Math.max(0, indicatorCoverage),
                Collections.emptyMap(),
                failureCounts,
                requestFailureCounts,
                insufficientCounts
        );
    }

    public ScanResultSummary(
            int total,
            int fetchCoverage,
            int indicatorCoverage,
            int tradableDenominator,
            int tradableIndicatorCoverage,
            Map<String, Integer> breakdownDenominatorExcluded,
            Map<ScanFailureReason, Integer> failureCounts,
            Map<ScanFailureReason, Integer> requestFailureCounts,
            Map<DataInsufficientReason, Integer> insufficientCounts
    ) {
        this.total = Math.max(0, total);
        this.fetchCoverage = Math.max(0, fetchCoverage);
        this.indicatorCoverage = Math.max(0, indicatorCoverage);

        this.tradableDenominator = Math.max(0, tradableDenominator);
        this.tradableIndicatorCoverage = Math.max(0, tradableIndicatorCoverage);
        this.marketScanIndicatorCoverageRawPct = pct(this.indicatorCoverage, this.total);
        this.marketScanIndicatorCoverageTradablePct = pct(this.tradableIndicatorCoverage, this.tradableDenominator);

        this.breakdownDenominatorExcluded = copyBreakdown(breakdownDenominatorExcluded);
        this.failureCounts = copyFailureCounts(failureCounts);
        this.requestFailureCounts = copyFailureCounts(requestFailureCounts);
        this.insufficientCounts = copyInsufficientCounts(insufficientCounts);
    }

    public int failureCount(ScanFailureReason reason) {
        if (reason == null) {
            return 0;
        }
        return failureCounts.getOrDefault(reason, 0);
    }

    public int requestFailureCount(ScanFailureReason reason) {
        if (reason == null) {
            return 0;
        }
        return requestFailureCounts.getOrDefault(reason, 0);
    }

    public int insufficientCount(DataInsufficientReason reason) {
        if (reason == null) {
            return 0;
        }
        return insufficientCounts.getOrDefault(reason, 0);
    }

    private Map<ScanFailureReason, Integer> copyFailureCounts(Map<ScanFailureReason, Integer> in) {
        Map<ScanFailureReason, Integer> out = new EnumMap<>(ScanFailureReason.class);
        for (ScanFailureReason reason : ScanFailureReason.values()) {
            out.put(reason, 0);
        }
        if (in != null) {
            for (Map.Entry<ScanFailureReason, Integer> e : in.entrySet()) {
                ScanFailureReason key = e.getKey();
                if (key == null) {
                    continue;
                }
                int value = e.getValue() == null ? 0 : Math.max(0, e.getValue());
                out.put(key, value);
            }
        }
        return Map.copyOf(out);
    }

    private Map<DataInsufficientReason, Integer> copyInsufficientCounts(Map<DataInsufficientReason, Integer> in) {
        Map<DataInsufficientReason, Integer> out = new EnumMap<>(DataInsufficientReason.class);
        for (DataInsufficientReason reason : DataInsufficientReason.values()) {
            out.put(reason, 0);
        }
        if (in != null) {
            for (Map.Entry<DataInsufficientReason, Integer> e : in.entrySet()) {
                DataInsufficientReason key = e.getKey();
                if (key == null) {
                    continue;
                }
                int value = e.getValue() == null ? 0 : Math.max(0, e.getValue());
                out.put(key, value);
            }
        }
        return Map.copyOf(out);
    }

    private Map<String, Integer> copyBreakdown(Map<String, Integer> in) {
        if (in == null || in.isEmpty()) {
            return Map.of();
        }
        Map<String, Integer> out = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> e : in.entrySet()) {
            String key = e.getKey() == null ? "" : e.getKey().trim();
            if (key.isEmpty()) {
                continue;
            }
            int value = e.getValue() == null ? 0 : Math.max(0, e.getValue());
            out.put(key, value);
        }
        return Map.copyOf(out);
    }

    private double pct(int numerator, int denominator) {
        if (denominator <= 0) {
            return 0.0;
        }
        return Math.max(0, numerator) * 100.0 / denominator;
    }
}
