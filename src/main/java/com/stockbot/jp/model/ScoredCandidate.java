package com.stockbot.jp.model;

public final class ScoredCandidate {
    public final String ticker;
    public final String code;
    public final String name;
    public final String market;
    public final double score;
    public final double close;
    public final String reasonsJson;
    public final String indicatorsJson;

    public ScoredCandidate(
            String ticker,
            String code,
            String name,
            String market,
            double score,
            double close,
            String reasonsJson,
            String indicatorsJson
    ) {
        this.ticker = ticker;
        this.code = code;
        this.name = name;
        this.market = market;
        this.score = score;
        this.close = close;
        this.reasonsJson = reasonsJson;
        this.indicatorsJson = indicatorsJson;
    }
}
