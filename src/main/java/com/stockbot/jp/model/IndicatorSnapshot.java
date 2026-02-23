package com.stockbot.jp.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder(toBuilder = true)
public final class IndicatorSnapshot {
    public final double lastClose;
    public final double sma20;
    public final double sma60;
    public final double sma60Prev5;
    public final double sma60Slope;
    public final double sma120;
    public final double rsi14;
    public final double atr14;
    public final double atrPct;
    public final double bollingerUpper;
    public final double bollingerMiddle;
    public final double bollingerLower;
    public final double drawdown120Pct;
    public final double volatility20Pct;
    public final double avgVolume20;
    public final double volumeRatio20;
    public final double pctFromSma20;
    public final double pctFromSma60;
    public final double return3dPct;
    public final double return5dPct;
    public final double return10dPct;
    public final double lowLookback;
    public final double highLookback;
}


