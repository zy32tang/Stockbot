package com.stockbot.jp.indicator;

import java.util.ArrayList;
import java.util.List;

/**
 * Indicator completeness diagnostics for one ticker.
 */
public final class IndicatorCoverageResult {
    public final List<String> missingCoreIndicators;
    public final List<String> missingOptionalIndicators;

    public IndicatorCoverageResult(List<String> missingCoreIndicators, List<String> missingOptionalIndicators) {
        this.missingCoreIndicators = missingCoreIndicators == null ? List.of() : List.copyOf(missingCoreIndicators);
        this.missingOptionalIndicators = missingOptionalIndicators == null ? List.of() : List.copyOf(missingOptionalIndicators);
    }

    public boolean coreReady() {
        return missingCoreIndicators.isEmpty();
    }

    public boolean fullyReady() {
        return missingCoreIndicators.isEmpty() && missingOptionalIndicators.isEmpty();
    }

    public List<String> allMissing() {
        List<String> out = new ArrayList<>(missingCoreIndicators.size() + missingOptionalIndicators.size());
        out.addAll(missingCoreIndicators);
        out.addAll(missingOptionalIndicators);
        return out;
    }

    public static IndicatorCoverageResult ready() {
        return new IndicatorCoverageResult(List.of(), List.of());
    }
}
