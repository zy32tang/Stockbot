package com.stockbot.jp.model;

/**
 * 模块说明：IndicatorSnapshot（class）。
 * 主要职责：承载 model 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
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

/**
 * 方法说明：IndicatorSnapshot，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public IndicatorSnapshot(
            double lastClose,
            double sma20,
            double sma60,
            double sma60Prev5,
            double sma60Slope,
            double sma120,
            double rsi14,
            double atr14,
            double atrPct,
            double bollingerUpper,
            double bollingerMiddle,
            double bollingerLower,
            double drawdown120Pct,
            double volatility20Pct,
            double avgVolume20,
            double volumeRatio20,
            double pctFromSma20,
            double pctFromSma60,
            double return3dPct,
            double return5dPct,
            double return10dPct,
            double lowLookback,
            double highLookback
    ) {
        this.lastClose = lastClose;
        this.sma20 = sma20;
        this.sma60 = sma60;
        this.sma60Prev5 = sma60Prev5;
        this.sma60Slope = sma60Slope;
        this.sma120 = sma120;
        this.rsi14 = rsi14;
        this.atr14 = atr14;
        this.atrPct = atrPct;
        this.bollingerUpper = bollingerUpper;
        this.bollingerMiddle = bollingerMiddle;
        this.bollingerLower = bollingerLower;
        this.drawdown120Pct = drawdown120Pct;
        this.volatility20Pct = volatility20Pct;
        this.avgVolume20 = avgVolume20;
        this.volumeRatio20 = volumeRatio20;
        this.pctFromSma20 = pctFromSma20;
        this.pctFromSma60 = pctFromSma60;
        this.return3dPct = return3dPct;
        this.return5dPct = return5dPct;
        this.return10dPct = return10dPct;
        this.lowLookback = lowLookback;
        this.highLookback = highLookback;
    }
}
