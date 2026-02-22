package com.stockbot.jp.polymarket;

import com.stockbot.data.http.HttpClientEx;
import com.stockbot.jp.config.Config;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Minimal Polymarket market fetch client.
 */
public final class PolymarketClient {
    private final Config config;
    private final HttpClientEx http;

    private volatile List<MarketRecord> cachedMarkets = List.of();
    private volatile Instant cacheUpdatedAt = Instant.EPOCH;

    public PolymarketClient(Config config, HttpClientEx http) {
        this.config = config;
        this.http = http;
    }

    public synchronized FetchResult fetchMarkets() {
        int ttlHours = Math.max(1, config.getInt("polymarket.refresh.ttl_hours", 12));
        if (!cachedMarkets.isEmpty()
                && cacheUpdatedAt != null
                && Duration.between(cacheUpdatedAt, Instant.now()).toHours() < ttlHours) {
            return FetchResult.success(cachedMarkets, true, "cache_hit");
        }

        String baseUrl = config.getString(
                "polymarket.baseUrl",
                config.getString("polymarket.gamma_base_url", "https://gamma-api.polymarket.com")
        );
        int timeoutSec = Math.max(5, config.getInt("polymarket.timeout_sec", 12));
        int maxMarkets = Math.max(10, config.getInt("polymarket.refresh.max_markets", 5000));
        String apiKey = config.getString("polymarket.apiKey", "");

        String url = baseUrl + "/markets?active=true&closed=false&limit=" + Math.min(5000, maxMarkets) + "&offset=0";
        Map<String, String> headers = new HashMap<>();
        if (!apiKey.isBlank()) {
            headers.put("Authorization", "Bearer " + apiKey.trim());
            headers.put("X-API-KEY", apiKey.trim());
        }

        try {
            String body = headers.isEmpty()
                    ? http.getText(url, timeoutSec)
                    : http.getText(url, timeoutSec, headers);
            List<MarketRecord> markets = parseMarkets(body);
            if (markets.size() > maxMarkets) {
                markets = new ArrayList<>(markets.subList(0, maxMarkets));
            }
            cachedMarkets = List.copyOf(markets);
            cacheUpdatedAt = Instant.now();
            return FetchResult.success(markets, false, "live_fetch");
        } catch (Exception e) {
            String reason = e.getMessage() == null ? "fetch_failed" : e.getMessage();
            return FetchResult.failure("data_source_failed: " + reason);
        }
    }

    private List<MarketRecord> parseMarkets(String body) {
        if (body == null || body.trim().isEmpty()) {
            return List.of();
        }
        JSONArray arr = null;
        String raw = body.trim();
        try {
            if (raw.startsWith("[")) {
                arr = new JSONArray(raw);
            } else {
                JSONObject root = new JSONObject(raw);
                if (root.opt("markets") instanceof JSONArray) {
                    arr = root.getJSONArray("markets");
                } else if (root.opt("data") instanceof JSONArray) {
                    arr = root.getJSONArray("data");
                }
            }
        } catch (Exception ignored) {
            return List.of();
        }
        if (arr == null || arr.isEmpty()) {
            return List.of();
        }

        List<MarketRecord> out = new ArrayList<>();
        for (int i = 0; i < arr.length(); i++) {
            JSONObject item = arr.optJSONObject(i);
            if (item == null) {
                continue;
            }
            String id = firstNonBlank(
                    item.optString("id", ""),
                    item.optString("marketId", ""),
                    item.optString("conditionId", "")
            );
            String title = firstNonBlank(item.optString("question", ""), item.optString("title", ""));
            String description = firstNonBlank(item.optString("description", ""), item.optString("subtitle", ""));
            String category = firstNonBlank(item.optString("category", ""), item.optString("tag", ""));
            Instant updatedAt = parseInstant(firstNonBlank(item.optString("updatedAt", ""), item.optString("endDate", "")));

            double liquidity = firstFinite(
                    item.optDouble("liquidity", Double.NaN),
                    item.optDouble("volume", Double.NaN),
                    item.optDouble("volume24hr", Double.NaN),
                    item.optDouble("openInterest", Double.NaN)
            );

            double yesPrice = firstFinite(
                    item.optDouble("outcomePrice", Double.NaN),
                    item.optDouble("yesPrice", Double.NaN),
                    extractOutcomePrice(item.opt("outcomes"), "yes")
            );
            double noPrice = firstFinite(
                    item.optDouble("noPrice", Double.NaN),
                    extractOutcomePrice(item.opt("outcomes"), "no")
            );
            double change24h = firstFinite(
                    item.optDouble("priceChange24h", Double.NaN),
                    item.optDouble("change24h", Double.NaN),
                    0.0
            );
            if (title.isBlank()) {
                continue;
            }
            out.add(new MarketRecord(id, title, description, category, updatedAt, liquidity, yesPrice, noPrice, change24h));
        }
        return out;
    }

    private double extractOutcomePrice(Object outcomesObject, String target) {
        if (!(outcomesObject instanceof JSONArray)) {
            return Double.NaN;
        }
        JSONArray outcomes = (JSONArray) outcomesObject;
        for (int i = 0; i < outcomes.length(); i++) {
            JSONObject item = outcomes.optJSONObject(i);
            if (item == null) {
                continue;
            }
            String name = item.optString("name", "").trim().toLowerCase(Locale.ROOT);
            if (!name.equals(target)) {
                continue;
            }
            return firstFinite(item.optDouble("price", Double.NaN), item.optDouble("probability", Double.NaN));
        }
        return Double.NaN;
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return "";
        }
        for (String value : values) {
            if (value != null && !value.trim().isEmpty()) {
                return value.trim();
            }
        }
        return "";
    }

    private double firstFinite(double... values) {
        if (values == null) {
            return Double.NaN;
        }
        for (double value : values) {
            if (Double.isFinite(value)) {
                return value;
            }
        }
        return Double.NaN;
    }

    private Instant parseInstant(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(raw.trim());
        } catch (Exception ignored) {
            return null;
        }
    }

    public static final class FetchResult {
        public final List<MarketRecord> markets;
        public final boolean success;
        public final String reason;
        public final boolean fromCache;

        private FetchResult(List<MarketRecord> markets, boolean success, String reason, boolean fromCache) {
            this.markets = markets == null ? List.of() : List.copyOf(markets);
            this.success = success;
            this.reason = reason == null ? "" : reason;
            this.fromCache = fromCache;
        }

        public static FetchResult success(List<MarketRecord> markets, boolean fromCache, String reason) {
            return new FetchResult(markets, true, reason, fromCache);
        }

        public static FetchResult failure(String reason) {
            return new FetchResult(List.of(), false, reason, false);
        }
    }

    public static final class MarketRecord {
        public final String id;
        public final String title;
        public final String description;
        public final String category;
        public final Instant updatedAt;
        public final double liquidity;
        public final double yesPrice;
        public final double noPrice;
        public final double change24hPct;

        public MarketRecord(
                String id,
                String title,
                String description,
                String category,
                Instant updatedAt,
                double liquidity,
                double yesPrice,
                double noPrice,
                double change24hPct
        ) {
            this.id = id == null ? "" : id;
            this.title = title == null ? "" : title;
            this.description = description == null ? "" : description;
            this.category = category == null ? "" : category;
            this.updatedAt = updatedAt;
            this.liquidity = Double.isFinite(liquidity) ? liquidity : 0.0;
            this.yesPrice = Double.isFinite(yesPrice) ? yesPrice : Double.NaN;
            this.noPrice = Double.isFinite(noPrice) ? noPrice : Double.NaN;
            this.change24hPct = Double.isFinite(change24hPct) ? change24hPct : 0.0;
        }

        public String searchableText() {
            return (title + " " + description + " " + category).trim();
        }
    }
}
