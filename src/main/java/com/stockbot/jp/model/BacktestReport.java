package com.stockbot.jp.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder(toBuilder = true)
public final class BacktestReport {
    public final int runCount;
    public final int sampleCount;
    public final double avgReturnPct;
    public final double medianReturnPct;
    public final double winRatePct;
}


