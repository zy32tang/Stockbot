package com.stockbot.jp.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.time.LocalDate;

@Value
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder(toBuilder = true)
public final class BarDaily {
    public final String ticker;
    public final LocalDate tradeDate;
    public final double open;
    public final double high;
    public final double low;
    public final double close;
    public final double volume;
}


