package com.stockbot.jp.model;

import java.time.LocalDate;

public final class BarDaily {
    public final String ticker;
    public final LocalDate tradeDate;
    public final double open;
    public final double high;
    public final double low;
    public final double close;
    public final double volume;

    public BarDaily(String ticker, LocalDate tradeDate, double open, double high, double low, double close, double volume) {
        this.ticker = ticker;
        this.tradeDate = tradeDate;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
    }
}
