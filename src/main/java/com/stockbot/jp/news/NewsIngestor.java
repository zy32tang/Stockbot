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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * RSS multi-source ingestion with Jsoup text cleanup into news_item.
 */
public final class NewsIngestor {
    private static final Set<String> SUPPORTED_SOURCES = Set.of(
            "google",
            "bing",
            "yahoo",
            "cnbc",
            "marketwatch",
            "wsj",
            "nytimes",
            "yahoonews",
            "investing",
            "ft",
            "guardian",
            "seekingalpha"
    );
    private static final Set<String> TOKEN_STOPWORDS = Set.of(
            "stock", "stocks", "market", "news", "share", "shares", "company", "companies",
            "co", "inc", "corp", "ltd", "limited", "plc", "class", "the", "and", "or",
            "a", "an", "for", "to", "of", "in", "on", "at", "jp", "us", "cn", "hk",
            "t", "ss", "sz", "earnings", "revenue", "guidance", "outlook", "forecast",
            "analyst", "rating", "price", "target", "dividend", "buyback", "merger",
            "acquisition", "partnership", "lawsuit", "regulation", "supply", "chain",
            "production", "capacity", "demand", "orders"
    );
    private static final Set<String> INVALID_TEXT_TOKENS = Set.of(
            "",
            "null",
            "undefined",
            "n/a",
            "na",
            "none",
            "unknown",
            "unkown",
            "-",
            "--"
    );

    private final Config config;
    private final HttpClientEx httpClient;
    private final NewsItemDao newsItemDao;

    public NewsIngestor(Config config, HttpClientEx httpClient, NewsItemDao newsItemDao) {
        this.config = config;
        this.httpClient = httpClient;
        this.newsItemDao = newsItemDao;
    }

    public IngestResult ingest(String ticker, List<String> queries, String lang, String region) {
        int timeoutSec = Math.max(5, config.getInt("news.fetch.timeout_sec", 25));
        TuningProfile tuning = resolveTuningProfile();
        List<String> normalizedQueries = normalizeQueries(ticker, queries, tuning.queryVariants);
        Set<String> enabledSources = enabledSources();
        logNewsTuning(tuning);

        LinkedHashMap<String, NewsItemDao.UpsertItem> merged = new LinkedHashMap<>();
        List<FetchTask> tasks = buildFetchTasks(
                ticker,
                normalizedQueries,
                enabledSources,
                safe(lang),
                safe(region),
                tuning.maxResultsPerVariant,
                timeoutSec
        );
        executeFetchTasks(tasks, tuning.newsConcurrent, merged, safe(lang), safe(region));

        int upserted = 0;
        try {
            upserted = newsItemDao.upsertAll(new ArrayList<>(merged.values()));
        } catch (SQLException e) {
            System.err.println("WARN: news upsert failed ticker=" + safe(ticker) + ", err=" + e.getMessage());
        }

        String sourceLabel = String.join("+", enabledSources);
        return new IngestResult(merged.size(), upserted, sourceLabel.isEmpty() ? "google" : sourceLabel);
    }

    TuningSnapshot tuningSnapshot() {
        TuningProfile tuning = resolveTuningProfile();
        return new TuningSnapshot(
                tuning.profile,
                tuning.queryVariants,
                tuning.maxResultsPerVariant,
                tuning.newsConcurrent,
                tuning.vectorExpandTopK,
                tuning.vectorExpandRounds
        );
    }

    Set<String> enabledSourcesSnapshot() {
        return enabledSources();
    }

    private void executeFetchTasks(
            List<FetchTask> tasks,
            int requestedConcurrency,
            LinkedHashMap<String, NewsItemDao.UpsertItem> merged,
            String lang,
            String region
    ) {
        if (tasks == null || tasks.isEmpty()) {
            return;
        }
        int poolSize = Math.max(1, Math.min(requestedConcurrency, tasks.size()));
        ExecutorService pool = Executors.newFixedThreadPool(poolSize);
        CompletionService<FetchTaskResult> completion = new ExecutorCompletionService<>(pool);
        int submitted = 0;
        try {
            for (FetchTask task : tasks) {
                if (task == null || task.fetcher == null) {
                    continue;
                }
                completion.submit(() -> runFetchTask(task));
                submitted++;
            }
            for (int i = 0; i < submitted; i++) {
                Future<FetchTaskResult> future = completion.take();
                try {
                    FetchTaskResult result = future.get();
                    if (result == null || result.items == null || result.items.isEmpty()) {
                        continue;
                    }
                    mergeFetched(merged, result.items, lang, region);
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause() == null ? e : e.getCause();
                    System.err.println("WARN: news fetch task failed err=" + cause.getMessage());
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            pool.shutdown();
        }
    }

    private FetchTaskResult runFetchTask(FetchTask task) {
        try {
            List<NewsItem> fetched = task.fetcher.call();
            if (fetched == null || fetched.isEmpty()) {
                return new FetchTaskResult(task.source, task.query, List.of());
            }
            List<NewsItem> sorted = new ArrayList<>(fetched);
            sorted.sort(Comparator.comparing(this::publishedAtOrMin).reversed());
            return new FetchTaskResult(task.source, task.query, sorted);
        } catch (Exception e) {
            System.err.println(String.format(
                    Locale.US,
                    "WARN: news fetch source=%s query=%s failed err=%s",
                    safe(task.source),
                    safe(task.query),
                    safe(e.getMessage())
            ));
            return new FetchTaskResult(task.source, task.query, List.of());
        }
    }

    private List<FetchTask> buildFetchTasks(
            String ticker,
            List<String> queries,
            Set<String> enabledSources,
            String lang,
            String region,
            int maxResultsPerVariant,
            int timeoutSec
    ) {
        List<FetchTask> tasks = new ArrayList<>();
        Set<String> relevanceTokens = buildRelevanceTokens(ticker, queries);
        for (String source : enabledSources) {
            if ("google".equals(source)) {
                for (String query : queries) {
                    tasks.add(new FetchTask(
                            source,
                            query,
                            () -> fetchGoogle(query, lang, region, maxResultsPerVariant, timeoutSec)
                    ));
                }
                continue;
            }
            if ("bing".equals(source)) {
                for (String query : queries) {
                    tasks.add(new FetchTask(
                            source,
                            query,
                            () -> fetchBing(query, lang, region, maxResultsPerVariant, timeoutSec)
                    ));
                }
                continue;
            }
            if ("yahoo".equals(source)) {
                tasks.add(new FetchTask(
                        source,
                        normalizeToken(ticker),
                        () -> fetchYahoo(ticker, lang, region, maxResultsPerVariant, timeoutSec)
                ));
                continue;
            }
            if ("cnbc".equals(source)) {
                tasks.add(new FetchTask(
                        source,
                        "",
                        () -> filterByRelevance(fetchRss("https://www.cnbc.com/id/100003114/device/rss/rss.html", staticFeedFetchLimit(maxResultsPerVariant), timeoutSec), relevanceTokens, maxResultsPerVariant)
                ));
                continue;
            }
            if ("marketwatch".equals(source)) {
                tasks.add(new FetchTask(
                        source,
                        "",
                        () -> filterByRelevance(fetchRss("https://feeds.marketwatch.com/marketwatch/topstories/", staticFeedFetchLimit(maxResultsPerVariant), timeoutSec), relevanceTokens, maxResultsPerVariant)
                ));
                continue;
            }
            if ("wsj".equals(source)) {
                tasks.add(new FetchTask(
                        source,
                        "",
                        () -> filterByRelevance(fetchRss("https://feeds.a.dj.com/rss/RSSMarketsMain.xml", staticFeedFetchLimit(maxResultsPerVariant), timeoutSec), relevanceTokens, maxResultsPerVariant)
                ));
                continue;
            }
            if ("nytimes".equals(source)) {
                tasks.add(new FetchTask(
                        source,
                        "",
                        () -> filterByRelevance(fetchRss("https://rss.nytimes.com/services/xml/rss/nyt/Business.xml", staticFeedFetchLimit(maxResultsPerVariant), timeoutSec), relevanceTokens, maxResultsPerVariant)
                ));
                continue;
            }
            if ("yahoonews".equals(source)) {
                tasks.add(new FetchTask(
                        source,
                        "",
                        () -> filterByRelevance(fetchRss("https://news.yahoo.com/rss", staticFeedFetchLimit(maxResultsPerVariant), timeoutSec), relevanceTokens, maxResultsPerVariant)
                ));
                continue;
            }
            if ("investing".equals(source)) {
                tasks.add(new FetchTask(
                        source,
                        "",
                        () -> filterByRelevance(fetchRss("https://www.investing.com/rss/news.rss", staticFeedFetchLimit(maxResultsPerVariant), timeoutSec), relevanceTokens, maxResultsPerVariant)
                ));
                continue;
            }
            if ("ft".equals(source)) {
                tasks.add(new FetchTask(
                        source,
                        "",
                        () -> filterByRelevance(fetchRss("https://www.ft.com/companies?format=rss", staticFeedFetchLimit(maxResultsPerVariant), timeoutSec), relevanceTokens, maxResultsPerVariant)
                ));
                continue;
            }
            if ("guardian".equals(source)) {
                tasks.add(new FetchTask(
                        source,
                        "",
                        () -> filterByRelevance(fetchRss("https://www.theguardian.com/business/rss", staticFeedFetchLimit(maxResultsPerVariant), timeoutSec), relevanceTokens, maxResultsPerVariant)
                ));
                continue;
            }
            if ("seekingalpha".equals(source)) {
                tasks.add(new FetchTask(
                        source,
                        "",
                        () -> filterByRelevance(fetchRss("https://seekingalpha.com/feed.xml", staticFeedFetchLimit(maxResultsPerVariant), timeoutSec), relevanceTokens, maxResultsPerVariant)
                ));
            }
        }
        return tasks;
    }

    private List<String> normalizeQueries(String ticker, List<String> queries, int maxVariants) {
        LinkedHashSet<String> out = new LinkedHashSet<>();
        if (queries != null) {
            for (String raw : queries) {
                String text = normalizeToken(raw);
                if (text.isEmpty()) {
                    continue;
                }
                out.add(text);
                if (out.size() >= maxVariants) {
                    break;
                }
            }
        }
        String fallback = normalizeToken(ticker);
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
        String sourcesCsv = safe(config.getString("watchlist.news.sources", ""));
        if (!sourcesCsv.isBlank()) {
            for (String raw : sourcesCsv.split(",")) {
                String source = safe(raw).toLowerCase(Locale.ROOT);
                if (!source.isEmpty() && SUPPORTED_SOURCES.contains(source)) {
                    out.add(source);
                }
            }
        }
        if (out.isEmpty()) {
            if (config.getBoolean("news.source.google_rss", true)) {
                out.add("google");
            }
            if (config.getBoolean("news.source.bing", true)) {
                out.add("bing");
            }
            if (config.getBoolean("news.source.yahoo_finance", true)) {
                out.add("yahoo");
            }
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
            String title = normalizeToken(item.title);
            if (url.isEmpty() || title.isEmpty()) {
                continue;
            }
            String content = normalizeToken(item.description);
            if (content.isEmpty()) {
                content = title;
            }
            String source = normalizeToken(item.source);
            if (source.isEmpty()) {
                source = hostOf(url);
            }
            OffsetDateTime publishedAt = item.publishedAt == null ? null : item.publishedAt.toOffsetDateTime();

            NewsItemDao.UpsertItem next = new NewsItemDao.UpsertItem(
                    url,
                    title,
                    content,
                    source,
                    normalizeToken(lang),
                    normalizeToken(region),
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

    private List<NewsItem> fetchRss(String url, int limit, int timeoutSec) {
        try {
            String xml = httpClient.getText(url, timeoutSec);
            return RssParser.parse(xml, Math.max(1, limit));
        } catch (Exception ignored) {
            return List.of();
        }
    }

    private List<NewsItem> filterByRelevance(List<NewsItem> items, Set<String> tokens, int limit) {
        if (items == null || items.isEmpty()) {
            return List.of();
        }
        int max = Math.max(1, limit);
        if (tokens == null || tokens.isEmpty()) {
            return items.size() <= max ? items : new ArrayList<>(items.subList(0, max));
        }
        List<NewsItem> out = new ArrayList<>();
        for (NewsItem item : items) {
            if (item == null) {
                continue;
            }
            if (!isRelevant(item, tokens)) {
                continue;
            }
            out.add(item);
            if (out.size() >= max) {
                break;
            }
        }
        return out;
    }

    private boolean isRelevant(NewsItem item, Set<String> tokens) {
        String text = (normalizeToken(item.title) + " "
                + normalizeToken(item.description) + " "
                + normalizeToken(item.link) + " "
                + normalizeToken(item.source)).toLowerCase(Locale.ROOT);
        if (text.isEmpty()) {
            return false;
        }
        for (String token : tokens) {
            if (!token.isEmpty() && text.contains(token)) {
                return true;
            }
        }
        return false;
    }

    private Set<String> buildRelevanceTokens(String ticker, List<String> queries) {
        LinkedHashSet<String> out = new LinkedHashSet<>();
        addToken(out, ticker);
        if (queries != null) {
            for (String query : queries) {
                addToken(out, query);
                if (query == null) {
                    continue;
                }
                for (String raw : query.split("[^\\p{L}\\p{N}.]+")) {
                    addToken(out, raw);
                }
            }
        }
        return out;
    }

    private void addToken(Set<String> out, String raw) {
        if (out == null || raw == null) {
            return;
        }
        String normalized = normalizeToken(raw).toLowerCase(Locale.ROOT);
        if (normalized.length() < 2) {
            return;
        }
        if (normalized.matches("\\d{1,2}")) {
            return;
        }
        if (TOKEN_STOPWORDS.contains(normalized)) {
            return;
        }
        out.add(normalized);
    }

    private int staticFeedFetchLimit(int maxResultsPerVariant) {
        return Math.max(maxResultsPerVariant * 4, 24);
    }

    private String normalizeToken(String raw) {
        if (raw == null) {
            return "";
        }
        String text = Jsoup.parse(raw).text()
                .replace('\u3000', ' ')
                .replaceAll("\\s+", " ")
                .trim();
        if (text.isEmpty()) {
            return "";
        }
        String lower = text.toLowerCase(Locale.ROOT);
        if (INVALID_TEXT_TOKENS.contains(lower)) {
            return "";
        }
        return text;
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
        return value == null ? "" : value.trim();
    }

    private OffsetDateTime publishedAtOrMin(NewsItem item) {
        if (item == null || item.publishedAt == null) {
            return OffsetDateTime.MIN;
        }
        return item.publishedAt.toOffsetDateTime();
    }

    private TuningProfile resolveTuningProfile() {
        int cpu = Runtime.getRuntime().availableProcessors();
        long maxMemGb = Runtime.getRuntime().maxMemory() / (1024L * 1024L * 1024L);
        boolean autoTune = config.getBoolean("news.performance.auto_tune", true);
        String profile = safe(config.getString("news.performance.profile", "accuracy")).toLowerCase(Locale.ROOT);
        if (profile.isEmpty()) {
            profile = "accuracy";
        }

        int queryVariants = Math.max(1, config.getInt("news.query.max_variants", 6));
        int maxResults = Math.max(1, config.getInt("news.query.max_results_per_variant", 8));
        int newsConcurrent = Math.max(1, config.getInt("news.concurrent", 10));
        int vectorTopK = Math.max(1, config.getInt("news.vector.query_expand.top_k", 8));
        int vectorRounds = Math.max(1, config.getInt("news.vector.query_expand.rounds", 2));

        if (autoTune) {
            if ("default".equalsIgnoreCase(config.sourceOf("news.query.max_variants"))) {
                int tuned = "accuracy".equals(profile)
                        ? Math.max(queryVariants, Math.min(16, Math.max(8, cpu / 2 + 4)))
                        : Math.max(queryVariants, Math.min(10, Math.max(6, cpu / 3 + 3)));
                queryVariants = tuned;
            }
            if ("default".equalsIgnoreCase(config.sourceOf("news.query.max_results_per_variant"))) {
                int tuned = "accuracy".equals(profile)
                        ? Math.max(maxResults, maxMemGb <= 4 ? 10 : 12)
                        : Math.max(maxResults, 8);
                maxResults = tuned;
            }
            if ("default".equalsIgnoreCase(config.sourceOf("news.concurrent"))) {
                int tuned = "accuracy".equals(profile)
                        ? Math.max(newsConcurrent, Math.min(24, Math.max(10, cpu)))
                        : Math.max(newsConcurrent, Math.min(16, Math.max(6, cpu / 2)));
                if (maxMemGb <= 4) {
                    tuned = Math.min(tuned, 12);
                }
                newsConcurrent = tuned;
            }
            if ("default".equalsIgnoreCase(config.sourceOf("news.vector.query_expand.top_k"))) {
                int tuned = "accuracy".equals(profile)
                        ? Math.max(vectorTopK, 12)
                        : Math.max(vectorTopK, 8);
                vectorTopK = tuned;
            }
            if ("default".equalsIgnoreCase(config.sourceOf("news.vector.query_expand.rounds"))) {
                int tuned = "accuracy".equals(profile)
                        ? Math.max(vectorRounds, 3)
                        : Math.max(vectorRounds, 2);
                vectorRounds = tuned;
            }
        }
        return new TuningProfile(
                cpu,
                maxMemGb,
                profile,
                queryVariants,
                maxResults,
                newsConcurrent,
                vectorTopK,
                vectorRounds
        );
    }

    private void logNewsTuning(TuningProfile tuning) {
        if (tuning == null) {
            return;
        }
        System.out.println(String.format(
                Locale.US,
                "[NEWS_TUNING] cpu=%d max_mem_gb=%d profile=%s concurrent=%d variants=%d results_per_variant=%d vector_expand_top_k=%d vector_expand_rounds=%d",
                tuning.cpu,
                tuning.maxMemGb,
                tuning.profile,
                tuning.newsConcurrent,
                tuning.queryVariants,
                tuning.maxResultsPerVariant,
                tuning.vectorExpandTopK,
                tuning.vectorExpandRounds
        ));
    }

    @Value
    private static final class TuningProfile {
        int cpu;
        long maxMemGb;
        String profile;
        int queryVariants;
        int maxResultsPerVariant;
        int newsConcurrent;
        int vectorExpandTopK;
        int vectorExpandRounds;
    }

    @Value
    static final class TuningSnapshot {
        String profile;
        int queryVariants;
        int maxResultsPerVariant;
        int newsConcurrent;
        int vectorExpandTopK;
        int vectorExpandRounds;
    }

    @Value
    private static final class FetchTask {
        String source;
        String query;
        Callable<List<NewsItem>> fetcher;
    }

    @Value
    private static final class FetchTaskResult {
        String source;
        String query;
        List<NewsItem> items;
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
