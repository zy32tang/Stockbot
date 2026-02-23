package com.stockbot.jp.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder(toBuilder = true)
public final class ScoredCandidate {
    public final String ticker;
    public final String code;
    public final String name;
    public final String market;
    public final double score;
    public final double close;
    public final String reasonsJson;
    public final String indicatorsJson;
}


