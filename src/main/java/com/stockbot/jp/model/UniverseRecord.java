package com.stockbot.jp.model;

public final class UniverseRecord {
    public final String ticker;
    public final String code;
    public final String name;
    public final String market;

    public UniverseRecord(String ticker, String code, String name, String market) {
        this.ticker = ticker;
        this.code = code;
        this.name = name;
        this.market = market;
    }
}
