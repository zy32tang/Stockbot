package com.stockbot.jp.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class FilterDecision {
    public final boolean passed;
    public final List<String> reasons;
    public final Map<String, Object> metrics;

    public FilterDecision(boolean passed, List<String> reasons, Map<String, Object> metrics) {
        this.passed = passed;
        this.reasons = reasons == null ? List.of() : Collections.unmodifiableList(reasons);
        this.metrics = metrics == null ? Map.of() : Collections.unmodifiableMap(metrics);
    }
}
