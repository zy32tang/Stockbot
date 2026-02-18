package com.stockbot.model;

import java.time.LocalDate;

public class DailyPrice {
    public final LocalDate date;
    public final double close;

    public DailyPrice(LocalDate date, double close) {
        this.date = date;
        this.close = close;
    }
}
