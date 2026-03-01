package com.stockbot.jp.tech;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

@Value
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder(toBuilder = true)
public final class TechSubscores {
    public final int trendStructure;
    public final int biasRisk;
    public final int volumeConfirm;
    public final int executionQuality;
}
