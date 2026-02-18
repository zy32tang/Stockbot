package com.stockbot.jp.polymarket;

import java.util.List;

public final class PolymarketTopicSignal {
    public final String topic;
    public final double impliedProbabilityPct;
    public final double change24hPct;
    public final String oiDirection;
    public final List<String> likelyIndustries;
    public final List<PolymarketWatchImpact> watchImpacts;
    public final String sourceMarketTitle;

    public PolymarketTopicSignal(
            String topic,
            double impliedProbabilityPct,
            double change24hPct,
            String oiDirection,
            List<String> likelyIndustries,
            List<PolymarketWatchImpact> watchImpacts,
            String sourceMarketTitle
    ) {
        this.topic = topic == null ? "" : topic;
        this.impliedProbabilityPct = impliedProbabilityPct;
        this.change24hPct = change24hPct;
        this.oiDirection = oiDirection == null ? "-" : oiDirection;
        this.likelyIndustries = likelyIndustries == null ? List.of() : List.copyOf(likelyIndustries);
        this.watchImpacts = watchImpacts == null ? List.of() : List.copyOf(watchImpacts);
        this.sourceMarketTitle = sourceMarketTitle == null ? "" : sourceMarketTitle;
    }
}
