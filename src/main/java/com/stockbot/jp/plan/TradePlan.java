package com.stockbot.jp.plan;

public final class TradePlan {
    public final boolean valid;
    public final double entryLow;
    public final double entryHigh;
    public final double stopLoss;
    public final double takeProfit;
    public final double rrRatio;

    public TradePlan(boolean valid, double entryLow, double entryHigh, double stopLoss, double takeProfit, double rrRatio) {
        this.valid = valid;
        this.entryLow = entryLow;
        this.entryHigh = entryHigh;
        this.stopLoss = stopLoss;
        this.takeProfit = takeProfit;
        this.rrRatio = rrRatio;
    }

    public static TradePlan invalid() {
        return new TradePlan(false, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
    }
}
