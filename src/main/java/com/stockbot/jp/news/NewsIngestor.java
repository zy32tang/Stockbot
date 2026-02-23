package com.stockbot.jp.news;

import com.stockbot.data.http.HttpClientEx;
import com.stockbot.data.rss.RssParser;
import com.stockbot.jp.config.Config;
import com.stockbot.model.NewsItem;
import lombok.Builder;
import lombok.Value;
import org.jsoup.Jsoup;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * RSS multi-source ingestion with Jsoup text cleanup into news_item.
 */
public final class NewsIngestor {
    private final Config config;
    private final HttpClientEx httpClient;
    private final NewsItemDao newsItemDao;

    public NewsIngestor(Config config, HttpClientEx httpClient, NewsItemDao newsItemDao) {
        this.config = config;
        this.httpClient = httpClient;
        this.newsItemDao = newsItemDao;
    }

    public IngestResult ingest(String ticker, List<String> queries, String lang, String region) {
        int maxVariants = Math.max(1, config.getInt("news.query.max_variants", 6));
        int maxResultsPerVariant = Math.max(1, config.getInt("news.query.max_results_per_variant", 8));
        int timeoutSec = Math.max(5, config.getInt("news.fetch.timeout_sec", 25));

        List<String> normalizedQueries = normalizeQueries(ticker, queries, maxVariants);
        LinkedHashMap<String, NewsItemDao.UpsertItem> merged = new LinkedHashMap<>();
        Set<String> enabledSources = enabledSources();

        for (String source : enabledSources) {
            if ("google".equals(source) || "bing".equals(source)) {
                for (String query : normalizedQueries) {
                    List<NewsItem> fetched = "google".equals(source)
                            ? fetchGoogle(query, lang, region, maxResultsPerVariant, timeoutSec)
                            : fetchBing(query, lang, region, maxResultsPerVariant, timeoutSec);
                    mergeFetched(merged, fetched, lang, region);
                }
            } else if ("yahoo".equals(source)) {
                mergeFetched(merged, fetchYahoo(ticker, lang, region, maxResultsPerVariant, timeoutSec), lang, region);
            }
        }

        int upserted = 0;
        try {
            upserted = newsItemDao.upsertAll(new ArrayList<>(merged.values()));
        } catch (SQLException e) {
            System.err.println("WARN: news upsert failed ticker=" + safe(ticker) + ", err=" + e.getMessage());
        }

        String sourceLabel = String.join("+", enabledSources);
        return new IngestResult(merged.size(), upserted, sourceLabel.isEmpty() ? "google" : sourceLabel);
    }

    private List<String> normalizeQueries(String ticker, List<String> queries, int maxVariants) {
        LinkedHashSet<String> out = new LinkedHashSet<>();
        if (queries != null) {
            for (String raw : queries) {
                String text = cleanInline(raw);
                if (text.isEmpty()) {
                    continue;
                }
                out.add(text);
                if (out.size() >= maxVariants) {
                    break;
                }
            }
        }
        String fallback = cleanInline(ticker);
        if (out.isEmpty() && !fallback.isEmpty()) {
            out.add(fallback);
        }
        if (out.isEmpty()) {
            out.add("stock market");
        }
        return new ArrayList<>(out);
    }

    private Set<String> enabledSources() {
        LinkedHashSet<String> out = new LinkedHashSet<>();
        if (config.getBoolean("news.source.google_rss", true)) {
            out.add("google");
        }
        if (config.getBoolean("news.source.bing", true)) {
            out.add("bing");
        }
        if (config.getBoolean("news.source.yahoo_finance", true)) {
            out.add("yahoo");
        }
        if (out.isEmpty()) {
            out.add("google");
        }
        return out;
    }

    private void mergeFetched(
            LinkedHashMap<String, NewsItemDao.UpsertItem> merged,
            List<NewsItem> fetched,
            String lang,
            String region
    ) {
        if (fetched == null || fetched.isEmpty()) {
            return;
        }
        for (NewsItem item : fetched) {
            if (item == null) {
                continue;
            }
            String url = normalizeUrl(item.link);
            String title = cleanInline(item.title);
            if (url.isEmpty() || title.isEmpty()) {
                continue;
            }
            String content = cleanInline(item.description);
            if (content.isEmpty()) {
                content = title;
            }
            String source = cleanInline(item.source);
            if (source.isEmpty()) {
                source = hostOf(url);
            }
            OffsetDateTime publishedAt = item.publishedAt == null ? null : item.publishedAt.toOffsetDateTime();

            NewsItemDao.UpsertItem next = new NewsItemDao.UpsertItem(
                    url,
                    title,
                    content,
                    source,
                    safe(lang),
                    safe(region),
                    publishedAt
            );
            NewsItemDao.UpsertItem old = merged.get(url);
            if (old == null || isNewer(next, old)) {
                merged.put(url, next);
            }
        }
    }

    private boolean isNewer(NewsItemDao.UpsertItem a, NewsItemDao.UpsertItem b) {
        if (a == null) {
            return false;
        }
        if (b == null) {
            return true;
        }
        if (a.getPublishedAt() == null) {
            return false;
        }
        if (b.getPublishedAt() == null) {
            return true;
        }
        return a.getPublishedAt().isAfter(b.getPublishedAt());
    }

    private List<NewsItem> fetchGoogle(String query, String lang, String region, int limit, int timeoutSec) {
        try {
            String q = URLEncoder.encode(safe(query), StandardCharsets.UTF_8);
            String language = safe(lang).isEmpty() ? "en" : lang;
            String reg = safe(region).isEmpty() ? "US" : region.toUpperCase(Locale.ROOT);
            String url = "https://news.google.com/rss/search?q=" + q
                    + "&hl=" + language
                    + "&gl=" + reg
                    + "&ceid=" + reg + ":" + language;
            String xml = httpClient.getText(url, timeoutSec);
            return RssParser.parse(xml, Math.max(1, limit));
        } catch (Exception ignored) {
            return List.of();
        }
    }

    private List<NewsItem> fetchBing(String query, String lang, String region, int limit, int timeoutSec) {
        try {
            String q = URLEncoder.encode(safe(query), StandardCharsets.UTF_8);
            String language = safe(lang).isEmpty() ? "en" : lang.toLowerCase(Locale.ROOT);
            String reg = safe(region).isEmpty() ? "us" : region.toLowerCase(Locale.ROOT);
            String url = "https://www.bing.com/news/search?q=" + q + "&format=rss&setlang=" + language + "-" + reg;
            String xml = httpClient.getText(url, timeoutSec);
            return RssParser.parse(xml, Math.max(1, limit));
        } catch (Exception ignored) {
            return List.of();
        }
    }

    private List<NewsItem> fetchYahoo(String ticker, String lang, String region, int limit, int timeoutSec) {
        try {
            String normalizedTicker = safe(ticker).trim();
            if (normalizedTicker.isEmpty()) {
                return List.of();
            }
            String t = URLEncoder.encode(normalizedTicker, StandardCharsets.UTF_8);
            String reg = safe(region).isEmpty() ? "US" : region.toUpperCase(Locale.ROOT);
            String language = safe(lang).isEmpty() ? "en" : lang.toLowerCase(Locale.ROOT);
            String url = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=" + t
                    + "&region=" + reg
                    + "&lang=" + language + "-" + reg.toLowerCase(Locale.ROOT);
            String xml = httpClient.getText(url, timeoutSec);
            return RssParser.parse(xml, Math.max(1, limit));
        } catch (Exception ignored) {
            return List.of();
        }
    }

    private String cleanInline(String raw) {
        if (raw == null) {
            return "";
        }
        return Jsoup.parse(raw).text()
                .replace('\u3000', ' ')
                .replaceAll("\\s+", " ")
                .trim();
    }

    private String normalizeUrl(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return "";
        }
        try {
            URI uri = URI.create(raw.trim());
            String scheme = uri.getScheme();
            String host = uri.getHost();
            if (host == null || host.isEmpty()) {
                return raw.trim();
            }
            String query = uri.getQuery();
            if (query != null && !query.isEmpty()) {
                List<String> kept = new ArrayList<>();
                for (String token : query.split("&")) {
                    String key = token;
                    int eq = token.indexOf('=');
                    if (eq > 0) {
                        key = token.substring(0, eq);
                    }
                    String lower = key.toLowerCase(Locale.ROOT);
                    if (lower.startsWith("utm_")
                            || "guccounter".equals(lower)
                            || "guce_referrer".equals(lower)
                            || "guce_referrer_sig".equals(lower)) {
                        continue;
                    }
                    kept.add(token);
                }
                query = kept.isEmpty() ? null : String.join("&", kept);
            }
            URI normalized = new URI(
                    scheme == null ? "https" : scheme.toLowerCase(Locale.ROOT),
                    uri.getUserInfo(),
                    host.toLowerCase(Locale.ROOT),
                    uri.getPort(),
                    uri.getPath(),
                    query,
                    null
            );
            return normalized.toString();
        } catch (Exception ignored) {
            return raw.trim();
        }
    }

    private String hostOf(String url) {
        try {
            URI uri = URI.create(url);
            return uri.getHost() == null ? "" : uri.getHost().toLowerCase(Locale.ROOT);
        } catch (Exception ignored) {
            return "";
        }
    }

    private String safe(String value) {
        return value == null ? "" : value;
    }

    @Value
    public static final class IngestResult {
        public final int fetchedCount;
        public final int upsertedCount;
        public final String sourceLabel;

        @Builder(toBuilder = true)
        public IngestResult(int fetchedCount, int upsertedCount, String sourceLabel) {
            this.fetchedCount = Math.max(0, fetchedCount);
            this.upsertedCount = Math.max(0, upsertedCount);
            this.sourceLabel = sourceLabel == null ? "" : sourceLabel;
        }
    }
}
