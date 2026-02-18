package com.stockbot.jp.model;

public final class CandidateRow {
    public final long runId;
    public final int rankNo;
    public final String ticker;
    public final String code;
    public final String name;
    public final double score;
    public final Double close;

    public CandidateRow(long runId, int rankNo, String ticker, String code, String name, double score, Double close) {
        this.runId = runId;
        this.rankNo = rankNo;
        this.ticker = ticker;
        this.code = code;
        this.name = name;
        this.score = score;
        this.close = close;
    }
}
