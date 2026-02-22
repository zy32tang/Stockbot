package com.stockbot.jp.polymarket;

import com.stockbot.jp.config.Config;
import com.stockbot.jp.model.WatchItem;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Backward-compatible rule provider.
 */
public final class PolymarketSignalProviderRule implements PolymarketSignalProvider {
    private final Config config;
    private final PolymarketClient client;

    public PolymarketSignalProviderRule(Config config, PolymarketClient client) {
        this.config = config;
        this.client = client;
    }

    @Override
    public PolymarketSignalReport collectSignals(List<WatchItem> watchlist, Instant now) {
        PolymarketClient.FetchResult fetched = client.fetchMarkets();
        if (!fetched.success) {
            return PolymarketSignalReport.empty("Data source failure: " + fetched.reason);
        }
        if (fetched.markets.isEmpty()) {
            return PolymarketSignalReport.empty("No match (markets=0)");
        }

        int topK = Math.max(1, config.getInt("polymarket.vector.top_k", config.getInt("polymarket.top_n", 3)));
        List<RuleScored> scored = new ArrayList<>();
        for (PolymarketClient.MarketRecord market : fetched.markets) {
            double score = scoreByKeyword(market, watchlist);
            if (score <= 0.0) {
                continue;
            }
            scored.add(new RuleScored(market, score));
        }
        scored.sort(Comparator.comparingDouble((RuleScored s) -> s.score).reversed());

        if (scored.isEmpty()) {
            return PolymarketSignalReport.empty("No match (rule mode did not hit watchlist tokens)");
        }

        List<PolymarketTopicSignal> out = new ArrayList<>();
        for (RuleScored s : scored) {
            List<PolymarketWatchImpact> impacts = buildImpacts(s.market, watchlist, s.score);
            out.add(new PolymarketTopicSignal(
                    "rule",
                    normalizePct(s.market.yesPrice),
                    s.market.change24hPct,
                    s.score >= 0.6 ? "up" : "flat",
                    List.of(blank(s.market.category, "general")),
                    impacts,
                    s.market.title
            ));
            if (out.size() >= topK) {
                break;
            }
        }
        String status = fetched.fromCache ? "rule mode (cache_hit)" : "rule mode";
        return new PolymarketSignalReport(true, status, out);
    }

    private List<PolymarketWatchImpact> buildImpacts(PolymarketClient.MarketRecord market, List<WatchItem> watchlist, double score) {
        if (watchlist == null || watchlist.isEmpty()) {
            return List.of();
        }
        List<PolymarketWatchImpact> out = new ArrayList<>();
        String marketText = tokenize(market.searchableText());
        for (WatchItem item : watchlist) {
            if (item == null) {
                continue;
            }
            String itemText = tokenize(item.displayCode + " " + item.displayNameLocal + " " + item.displayNameEn);
            if (!hasOverlap(marketText, itemText)) {
                continue;
            }
            String impact = inferImpact(market.yesPrice);
            if ("neutral".equals(impact)) {
                continue;
            }
            out.add(new PolymarketWatchImpact(
                    blank(item.displayCode, item.ticker),
                    impact,
                    Math.max(0.1, Math.min(0.95, score)),
                    "rule_match"
            ));
            if (out.size() >= 5) {
                break;
            }
        }
        return out;
    }

    private double scoreByKeyword(PolymarketClient.MarketRecord market, List<WatchItem> watchlist) {
        String marketText = tokenize(market.searchableText());
        if (marketText.isBlank()) {
            return 0.0;
        }
        Set<String> watchTokens = new LinkedHashSet<>();
        if (watchlist != null) {
            for (WatchItem item : watchlist) {
                if (item == null) {
                    continue;
                }
                for (String t : tokenize(item.displayCode + " " + item.displayNameLocal + " " + item.displayNameEn).split(" ")) {
                    if (!t.isBlank()) {
                        watchTokens.add(t);
                    }
                }
            }
        }
        if (watchTokens.isEmpty()) {
            return 0.0;
        }
        int hit = 0;
        for (String token : watchTokens) {
            if (marketText.contains(token)) {
                hit++;
            }
        }
        double base = hit / (double) watchTokens.size();
        double liquidity = Math.min(1.0, Math.log1p(Math.max(0.0, market.liquidity)) / 8.0);
        return 0.7 * base + 0.3 * liquidity;
    }

    private String inferImpact(double yesPrice) {
        if (!Double.isFinite(yesPrice)) {
            return "neutral";
        }
        if (yesPrice >= 0.55) {
            return "positive";
        }
        if (yesPrice <= 0.45) {
            return "negative";
        }
        return "neutral";
    }

    private boolean hasOverlap(String a, String b) {
        if (a.isBlank() || b.isBlank()) {
            return false;
        }
        for (String token : b.split(" ")) {
            if (!token.isBlank() && a.contains(token)) {
                return true;
            }
        }
        return false;
    }

    private String tokenize(String text) {
        if (text == null) {
            return "";
        }
        return text.toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9 ]", " ")
                .replaceAll("\\s+", " ")
                .trim();
    }

    private double normalizePct(double yesPrice) {
        if (!Double.isFinite(yesPrice)) {
            return 50.0;
        }
        if (yesPrice >= 0.0 && yesPrice <= 1.0) {
            return yesPrice * 100.0;
        }
        return Math.max(0.0, Math.min(100.0, yesPrice));
    }

    private String blank(String value, String fallback) {
        if (value == null || value.trim().isEmpty()) {
            return fallback;
        }
        return value.trim();
    }

    private static final class RuleScored {
        final PolymarketClient.MarketRecord market;
        final double score;

        private RuleScored(PolymarketClient.MarketRecord market, double score) {
            this.market = market;
            this.score = score;
        }
    }
}