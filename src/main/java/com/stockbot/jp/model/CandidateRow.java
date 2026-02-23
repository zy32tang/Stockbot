package com.stockbot.jp.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder(toBuilder = true)
public final class CandidateRow {
    public final long runId;
    public final int rankNo;
    public final String ticker;
    public final String code;
    public final String name;
    public final double score;
    public final Double close;
}


