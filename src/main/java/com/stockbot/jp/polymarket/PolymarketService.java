package com.stockbot.jp.polymarket;

import com.stockbot.data.OllamaClient;
import com.stockbot.data.http.HttpClientEx;
import com.stockbot.jp.config.Config;
import com.stockbot.jp.model.WatchItem;
import com.stockbot.jp.model.WatchlistAnalysis;
import com.stockbot.jp.vector.VectorSearchService;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Polymarket signal facade.
 */
public final class PolymarketService {
    private final Config config;
    private final PolymarketSignalProvider ruleProvider;
    private final PolymarketSignalProvider vectorProvider;

    public PolymarketService(Config config, HttpClientEx http, OllamaClient ollamaClient) {
        this(config, http, ollamaClient, null);
    }

    public PolymarketService(
            Config config,
            HttpClientEx http,
            OllamaClient ollamaClient,
            VectorSearchService vectorSearchService
    ) {
        this.config = config;
        PolymarketClient client = new PolymarketClient(config, http);
        this.ruleProvider = new PolymarketSignalProviderRule(config, client);
        this.vectorProvider = new PolymarketSignalProviderVector(config, client, vectorSearchService);
    }

    public PolymarketSignalReport collectSignals(List<WatchlistAnalysis> watchRows) {
        return collectSignals(toWatchItems(watchRows), Instant.now());
    }

    public PolymarketSignalReport collectSignals(List<WatchItem> watchlist, Instant now) {
        if (!config.getBoolean("polymarket.enabled", true)) {
            return PolymarketSignalReport.disabled("Disabled (config=polymarket.enabled=false)");
        }

        String mode = config.getString("polymarket.impact.mode", "vector").trim().toLowerCase(Locale.ROOT);
        PolymarketSignalProvider provider = "rule".equals(mode) ? ruleProvider : vectorProvider;
        try {
            PolymarketSignalReport report = provider.collectSignals(watchlist, now == null ? Instant.now() : now);
            if (report == null) {
                return PolymarketSignalReport.empty("Data source failure: provider_returned_null");
            }
            if (!report.enabled) {
                return report;
            }
            if (report.signals.isEmpty() && (report.statusMessage == null || report.statusMessage.trim().isEmpty())) {
                return PolymarketSignalReport.empty("No match: provider_no_match_reason");
            }
            return report;
        } catch (Exception e) {
            return PolymarketSignalReport.empty("Data source failure: runtime_error=" + e.getClass().getSimpleName());
        }
    }

    private List<WatchItem> toWatchItems(List<WatchlistAnalysis> watchRows) {
        if (watchRows == null || watchRows.isEmpty()) {
            return List.of();
        }
        List<WatchItem> out = new ArrayList<>();
        for (WatchlistAnalysis row : watchRows) {
            if (row == null) {
                continue;
            }
            String code = blank(row.code, row.ticker);
            String localName = blank(row.companyNameLocal, row.displayName);
            String enName = blank(row.displayName, localName);
            out.add(new WatchItem(
                    blank(row.ticker, ""),
                    blank(row.resolvedMarket, ""),
                    code.toUpperCase(Locale.ROOT),
                    localName,
                    enName
            ));
        }
        return out;
    }

    private String blank(String value, String fallback) {
        if (value == null || value.trim().isEmpty()) {
            return fallback == null ? "" : fallback;
        }
        return value.trim();
    }
}
