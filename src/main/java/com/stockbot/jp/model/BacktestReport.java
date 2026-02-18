package com.stockbot.jp.model;

public final class BacktestReport {
    public final int runCount;
    public final int sampleCount;
    public final double avgReturnPct;
    public final double medianReturnPct;
    public final double winRatePct;

    public BacktestReport(int runCount, int sampleCount, double avgReturnPct, double medianReturnPct, double winRatePct) {
        this.runCount = runCount;
        this.sampleCount = sampleCount;
        this.avgReturnPct = avgReturnPct;
        this.medianReturnPct = medianReturnPct;
        this.winRatePct = winRatePct;
    }
}
