package com.stockbot.jp.tech;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.util.List;

@Value
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder(toBuilder = true)
public final class TechScoreResult {
    public final String ticker;
    public final String companyName;
    public final double price;
    public final int trendStrength;
    public final TechSubscores subscores;
    public final SignalStatus signalStatus;
    public final RiskLevel riskLevel;
    public final double ma5;
    public final double ma10;
    public final double ma20;
    public final double bias;
    public final double volRatio;
    public final double stopLine;
    public final double stopPct;
    public final DataStatus dataStatus;
    public final List<TechChecklistItem> checklist;
}
