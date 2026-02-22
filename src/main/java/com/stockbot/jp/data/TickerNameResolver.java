package com.stockbot.jp.data;

import com.stockbot.data.http.HttpClientEx;
import com.stockbot.jp.config.Config;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolve ticker display name/code with local cache.
 */
public final class TickerNameResolver {
    private final HttpClientEx http;
    private final Path cachePath;
    private final long ttlSeconds;
    private final Map<String, CacheEntry> cache = new ConcurrentHashMap<>();

    public TickerNameResolver(Config config, HttpClientEx http) {
        this.http = http;
        String customPath = config.getString("ticker.name.cache.path", "");
        this.cachePath = customPath.isBlank()
                ? config.getPath("outputs.dir").resolve("cache").resolve("ticker_name_cache.json")
                : config.workingDir().resolve(customPath).normalize();
        long ttlHours = Math.max(24L, config.getLong("ticker.name.cache.ttl_hours", 24L * 7L));
        this.ttlSeconds = ttlHours * 3600L;
        loadCache();
    }

    public ResolvedTickerName resolve(String ticker, String marketHint) {
        ParsedTicker parsed = parseTicker(ticker, marketHint);
        if (parsed.displayCode.isEmpty()) {
            String fallback = sanitizeCode(ticker);
            return new ResolvedTickerName(parsed.market, fallback, fallback, fallback);
        }

        String key = parsed.market + ":" + parsed.displayCode;
        long now = Instant.now().getEpochSecond();
        CacheEntry hit = cache.get(key);
        if (hit != null && hit.expiresAtEpochSec > now) {
            return hit.toResolved();
        }

        ResolvedTickerName fetched = fetchFromYahoo(parsed);
        cache.put(key, new CacheEntry(
                fetched.market,
                fetched.displayCode,
                fetched.displayNameLocal,
                fetched.displayNameEn,
                now + ttlSeconds
        ));
        persistCache();
        return fetched;
    }

    private ParsedTicker parseTicker(String tickerRaw, String marketHint) {
        String ticker = safe(tickerRaw).toUpperCase(Locale.ROOT);
        if (ticker.isEmpty()) {
            return new ParsedTicker("US", "", "");
        }

        String market = parseMarket(marketHint);
        String displayCode = ticker;
        if (ticker.endsWith(".T")) {
            market = "JP";
            displayCode = ticker.substring(0, ticker.length() - 2);
        } else if (ticker.endsWith(".JP")) {
            market = "JP";
            displayCode = ticker.substring(0, ticker.length() - 3);
        } else if (ticker.endsWith(".US")) {
            market = "US";
            displayCode = ticker.substring(0, ticker.length() - 3);
        } else if (ticker.endsWith(".NQ") || ticker.endsWith(".N")) {
            market = "US";
            int cut = ticker.lastIndexOf('.');
            displayCode = cut > 0 ? ticker.substring(0, cut) : ticker;
        } else if (ticker.matches("\\d{4,6}")) {
            market = "JP";
            displayCode = ticker;
        } else if (ticker.matches("[A-Z]{1,6}")) {
            market = "US";
            displayCode = ticker;
        }

        displayCode = sanitizeCode(displayCode);
        String yahooSymbol = market.equals("JP") ? (displayCode + ".T") : displayCode;
        return new ParsedTicker(market, displayCode, yahooSymbol);
    }

    private ResolvedTickerName fetchFromYahoo(ParsedTicker parsed) {
        String shortName = "";
        String longName = "";
        try {
            String symbol = URLEncoder.encode(parsed.yahooSymbol, StandardCharsets.UTF_8);
            String quoteUrl = "https://query1.finance.yahoo.com/v7/finance/quote?symbols=" + symbol;
            String quoteBody = http.getText(quoteUrl, 15);
            JSONObject quoteRoot = new JSONObject(quoteBody);
            JSONObject quoteResponse = quoteRoot.optJSONObject("quoteResponse");
            JSONArray result = quoteResponse == null ? null : quoteResponse.optJSONArray("result");
            JSONObject item = (result == null || result.length() == 0) ? null : result.optJSONObject(0);
            if (item != null) {
                shortName = safe(item.optString("shortName", ""));
                longName = safe(item.optString("longName", ""));
            }
        } catch (Exception ignored) {
            // fallback below
        }

        if (shortName.isEmpty() && longName.isEmpty()) {
            try {
                String symbol = URLEncoder.encode(parsed.yahooSymbol, StandardCharsets.UTF_8);
                String chartUrl = "https://query1.finance.yahoo.com/v8/finance/chart/" + symbol + "?range=5d&interval=1d";
                String chartBody = http.getText(chartUrl, 15);
                JSONObject chartRoot = new JSONObject(chartBody);
                JSONObject chart = chartRoot.optJSONObject("chart");
                JSONArray result = chart == null ? null : chart.optJSONArray("result");
                JSONObject r0 = (result == null || result.length() == 0) ? null : result.optJSONObject(0);
                JSONObject meta = r0 == null ? null : r0.optJSONObject("meta");
                if (meta != null) {
                    shortName = safe(meta.optString("shortName", shortName));
                    longName = safe(meta.optString("longName", longName));
                }
            } catch (Exception ignored) {
                // final fallback to code below
            }
        }

        String localName = firstNonBlank(shortName, longName, parsed.displayCode);
        String enName = firstNonBlank(longName, shortName, localName, parsed.displayCode);
        return new ResolvedTickerName(parsed.market, parsed.displayCode, localName, enName);
    }

    private void loadCache() {
        try {
            if (!Files.exists(cachePath)) {
                return;
            }
            String body = Files.readString(cachePath);
            if (body == null || body.trim().isEmpty()) {
                return;
            }
            JSONObject root = new JSONObject(body);
            JSONArray items = root.optJSONArray("items");
            if (items == null) {
                return;
            }
            for (int i = 0; i < items.length(); i++) {
                JSONObject item = items.optJSONObject(i);
                if (item == null) {
                    continue;
                }
                String market = firstNonBlank(item.optString("market", ""), "US");
                String code = sanitizeCode(item.optString("displayCode", ""));
                if (code.isEmpty()) {
                    continue;
                }
                String key = market + ":" + code;
                cache.put(key, new CacheEntry(
                        market,
                        code,
                        firstNonBlank(item.optString("displayNameLocal", ""), code),
                        firstNonBlank(item.optString("displayNameEn", ""), code),
                        item.optLong("expiresAtEpochSec", 0L)
                ));
            }
        } catch (Exception ignored) {
            // cache corruption should not block runtime.
        }
    }

    private void persistCache() {
        try {
            Path parent = cachePath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            JSONArray items = new JSONArray();
            for (CacheEntry entry : cache.values()) {
                JSONObject item = new JSONObject();
                item.put("market", entry.market);
                item.put("displayCode", entry.displayCode);
                item.put("displayNameLocal", entry.displayNameLocal);
                item.put("displayNameEn", entry.displayNameEn);
                item.put("expiresAtEpochSec", entry.expiresAtEpochSec);
                items.put(item);
            }
            JSONObject root = new JSONObject();
            root.put("items", items);
            root.put("updatedAt", Instant.now().toString());
            Files.writeString(cachePath, root.toString());
        } catch (Exception ignored) {
            // best effort only.
        }
    }

    private String parseMarket(String raw) {
        String token = safe(raw).toUpperCase(Locale.ROOT);
        if ("JP".equals(token)) {
            return "JP";
        }
        return "US";
    }

    private String sanitizeCode(String raw) {
        String token = safe(raw).toUpperCase(Locale.ROOT);
        token = token.replaceAll("[^A-Z0-9]", "");
        return token;
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

    private String safe(String value) {
        return value == null ? "" : value.trim();
    }

    private static final class ParsedTicker {
        final String market;
        final String displayCode;
        final String yahooSymbol;

        private ParsedTicker(String market, String displayCode, String yahooSymbol) {
            this.market = market;
            this.displayCode = displayCode;
            this.yahooSymbol = yahooSymbol;
        }
    }

    private static final class CacheEntry {
        final String market;
        final String displayCode;
        final String displayNameLocal;
        final String displayNameEn;
        final long expiresAtEpochSec;

        private CacheEntry(String market, String displayCode, String displayNameLocal, String displayNameEn, long expiresAtEpochSec) {
            this.market = market;
            this.displayCode = displayCode;
            this.displayNameLocal = displayNameLocal;
            this.displayNameEn = displayNameEn;
            this.expiresAtEpochSec = expiresAtEpochSec;
        }

        private ResolvedTickerName toResolved() {
            return new ResolvedTickerName(market, displayCode, displayNameLocal, displayNameEn);
        }
    }

    public static final class ResolvedTickerName {
        public final String market;
        public final String displayCode;
        public final String displayNameLocal;
        public final String displayNameEn;

        public ResolvedTickerName(String market, String displayCode, String displayNameLocal, String displayNameEn) {
            this.market = market == null ? "US" : market;
            this.displayCode = displayCode == null ? "" : displayCode;
            this.displayNameLocal = displayNameLocal == null ? this.displayCode : displayNameLocal;
            this.displayNameEn = displayNameEn == null ? this.displayNameLocal : displayNameEn;
        }
    }
}
