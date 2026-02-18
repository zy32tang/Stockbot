package com.stockbot.jp.model;

import java.util.Collections;
import java.util.List;

public final class RiskDecision {
    public final boolean passed;
    public final double penalty;
    public final List<String> flags;

    public RiskDecision(boolean passed, double penalty, List<String> flags) {
        this.passed = passed;
        this.penalty = penalty;
        this.flags = flags == null ? List.of() : Collections.unmodifiableList(flags);
    }
}
