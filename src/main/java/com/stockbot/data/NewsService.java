package com.stockbot.data;

import com.stockbot.data.http.HttpClientEx;
import com.stockbot.data.rss.RssParser;
import com.stockbot.model.NewsItem;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class NewsService {
    private static final ZonedDateTime MIN_ZDT = ZonedDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
    private static final Set<String> SUPPORTED_SOURCES = Set.of(
            "google",
            "bing",
            "yahoo",
            "cnbc",
            "marketwatch",
            "wsj",
            "nytimes",
            "yahoonews"
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

    private final HttpClientEx http;
    private final String lang;
    private final String region;
    private final int maxItems;
    private final List<String> sources;
    private final int queryVariants;

    public NewsService(HttpClientEx http, String lang, String region, int maxItems) {
        this(http, lang, region, maxItems, "google,bing,yahoo,cnbc,marketwatch,wsj,nytimes,yahoonews", 4);
    }

    public NewsService(HttpClientEx http, String lang, String region, int maxItems, String sourcesCsv, int queryVariants) {
        this.http = http;
        this.lang = lang;
        this.region = region;
        this.maxItems = maxItems;
        this.sources = parseSources(sourcesCsv);
        this.queryVariants = Math.max(1, queryVariants);
    }

    public String sourceLabel() {
        return sources.stream()
                .map(NewsService::sourceDisplayName)
                .collect(Collectors.joining("+"));
    }

    public List<NewsItem> fetchNews(String ticker, List<String> queries) {
        try {
            List<String> qlist = normalizeQueries(ticker, queries);
            Set<String> tokens = relevanceTokens(ticker, qlist);
            Map<String, NewsItem> merged = new LinkedHashMap<>();

            for (String source : sources) {
                if ("google".equals(source)) {
                    for (String q : qlist) merge(merged, fetchGoogleNewsRss(q, maxItems));
                    continue;
                }
                if ("bing".equals(source)) {
                    for (String q : qlist) merge(merged, fetchBingNewsRss(q, maxItems));
                    continue;
                }
                if ("yahoo".equals(source)) {
                    merge(merged, fetchYahooFinanceRss(ticker, maxItems));
                    continue;
                }
                if ("cnbc".equals(source)) {
                    merge(merged, filterByRelevance(fetchCnbcTopNewsRss(staticFeedFetchLimit()), tokens, maxItems));
                    continue;
                }
                if ("marketwatch".equals(source)) {
                    merge(merged, filterByRelevance(fetchMarketWatchTopStoriesRss(staticFeedFetchLimit()), tokens, maxItems));
                    continue;
                }
                if ("wsj".equals(source)) {
                    merge(merged, filterByRelevance(fetchWsjMarketsRss(staticFeedFetchLimit()), tokens, maxItems));
                    continue;
                }
                if ("nytimes".equals(source)) {
                    merge(merged, filterByRelevance(fetchNyTimesBusinessRss(staticFeedFetchLimit()), tokens, maxItems));
                    continue;
                }
                if ("yahoonews".equals(source)) {
                    merge(merged, filterByRelevance(fetchYahooNewsTopRss(staticFeedFetchLimit()), tokens, maxItems));
                }
            }

            List<NewsItem> out = new ArrayList<>(merged.values());
            out.sort(Comparator.comparing(NewsService::newsTimeOrMin).reversed());
            if (out.size() > maxItems) return out.subList(0, maxItems);
            return out;
        } catch (Exception e) {
            return List.of();
        }
    }

    public List<NewsItem> fetchCnbcTopNewsRss(int limit) {
        return fetchRss("https://www.cnbc.com/id/100003114/device/rss/rss.html", limit);
    }

    public List<NewsItem> fetchMarketWatchTopStoriesRss(int limit) {
        return fetchRss("https://feeds.marketwatch.com/marketwatch/topstories/", limit);
    }

    public List<NewsItem> fetchWsjMarketsRss(int limit) {
        return fetchRss("https://feeds.a.dj.com/rss/RSSMarketsMain.xml", limit);
    }

    public List<NewsItem> fetchNyTimesBusinessRss(int limit) {
        return fetchRss("https://rss.nytimes.com/services/xml/rss/nyt/Business.xml", limit);
    }

    public List<NewsItem> fetchYahooNewsTopRss(int limit) {
        return fetchRss("https://news.yahoo.com/rss", limit);
    }

    public List<NewsItem> fetchGoogleNewsRss(String query) {
        return fetchGoogleNewsRss(query, maxItems);
    }

    public List<NewsItem> fetchGoogleNewsRss(String query, int limit) {
        try {
            String q = URLEncoder.encode(query, StandardCharsets.UTF_8);
            String url = "https://news.google.com/rss/search?q=" + q + "&hl=" + lang + "&gl=" + region + "&ceid=" + region + ":" + lang;
            String xml = http.getText(url, 30);
            return RssParser.parse(xml, Math.max(1, limit));
        } catch (Exception e) {
            return List.of();
        }
    }

    public List<NewsItem> fetchBingNewsRss(String query, int limit) {
        try {
            String q = URLEncoder.encode(query, StandardCharsets.UTF_8);
            String langTag = (lang == null || lang.trim().isEmpty() ? "en" : lang.trim().toLowerCase(Locale.ROOT))
                    + "-"
                    + (region == null || region.trim().isEmpty() ? "us" : region.trim().toLowerCase(Locale.ROOT));
            String url = "https://www.bing.com/news/search?q=" + q + "&format=rss&setlang=" + langTag;
            String xml = http.getText(url, 30);
            return RssParser.parse(xml, Math.max(1, limit));
        } catch (Exception e) {
            return List.of();
        }
    }

    public List<NewsItem> fetchYahooFinanceRss(String ticker, int limit) {
        try {
            String t = URLEncoder.encode(ticker == null ? "" : ticker.trim(), StandardCharsets.UTF_8);
            if (t.isEmpty()) return List.of();
            String langTag = (lang == null || lang.trim().isEmpty() ? "en" : lang.trim().toLowerCase(Locale.ROOT))
                    + "-"
                    + (region == null || region.trim().isEmpty() ? "us" : region.trim().toUpperCase(Locale.ROOT));
            String reg = region == null || region.trim().isEmpty() ? "US" : region.trim().toUpperCase(Locale.ROOT);
            String url = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=" + t
                    + "&region=" + reg + "&lang=" + langTag;
            String xml = http.getText(url, 30);
            return RssParser.parse(xml, Math.max(1, limit));
        } catch (Exception e) {
            return List.of();
        }
    }

    private List<NewsItem> fetchRss(String url, int limit) {
        try {
            String xml = http.getText(url, 30);
            return RssParser.parse(xml, Math.max(1, limit));
        } catch (Exception e) {
            return List.of();
        }
    }

    private static ZonedDateTime newsTimeOrMin(NewsItem ni) {
        return ni == null || ni.publishedAt == null ? MIN_ZDT : ni.publishedAt;
    }

    private void merge(Map<String, NewsItem> merged, List<NewsItem> items) {
        if (items == null || items.isEmpty()) return;
        for (NewsItem ni : items) {
            if (ni == null) continue;
            String key = dedupKey(ni);
            NewsItem old = merged.get(key);
            if (old == null || isNewer(ni, old)) merged.put(key, ni);
        }
    }

    private static boolean isNewer(NewsItem a, NewsItem b) {
        ZonedDateTime ta = a.publishedAt;
        ZonedDateTime tb = b.publishedAt;
        if (ta == null) return false;
        if (tb == null) return true;
        return ta.isAfter(tb);
    }

    private static String dedupKey(NewsItem ni) {
        String title = ni.title == null ? "" : ni.title.trim().toLowerCase(Locale.ROOT);
        String link = ni.link == null ? "" : ni.link.trim().toLowerCase(Locale.ROOT);
        if (!title.isEmpty()) return title;
        return link;
    }

    private List<String> normalizeQueries(String ticker, List<String> queries) {
        Set<String> out = new LinkedHashSet<>();
        if (queries != null) {
            for (String q : queries) {
                if (q == null) continue;
                String t = q.trim();
                if (t.isEmpty()) continue;
                out.add(t);
                if (out.size() >= queryVariants) break;
            }
        }
        if (out.isEmpty() && ticker != null && !ticker.trim().isEmpty()) out.add(ticker.trim());
        if (out.isEmpty()) out.add("stock market");
        return new ArrayList<>(out);
    }

    private static List<String> parseSources(String csv) {
        Set<String> out = new LinkedHashSet<>();
        if (csv != null) {
            for (String raw : csv.split(",")) {
                String s = raw == null ? "" : raw.trim().toLowerCase(Locale.ROOT);
                if (!s.isEmpty() && SUPPORTED_SOURCES.contains(s)) {
                    out.add(s);
                }
            }
        }
        if (out.isEmpty()) out.add("google");
        return new ArrayList<>(out);
    }

    private static String sourceDisplayName(String source) {
        if ("google".equals(source)) return "GoogleNewsRSS";
        if ("bing".equals(source)) return "BingNewsRSS";
        if ("yahoo".equals(source)) return "YahooFinanceRSS";
        if ("cnbc".equals(source)) return "CNBCRSS";
        if ("marketwatch".equals(source)) return "MarketWatchRSS";
        if ("wsj".equals(source)) return "WSJMarketsRSS";
        if ("nytimes".equals(source)) return "NYTimesBusinessRSS";
        if ("yahoonews".equals(source)) return "YahooNewsRSS";
        return source;
    }

    private int staticFeedFetchLimit() {
        return Math.max(maxItems * 4, 24);
    }

    private static List<NewsItem> filterByRelevance(List<NewsItem> items, Set<String> tokens, int limit) {
        if (items == null || items.isEmpty()) return List.of();
        int max = Math.max(1, limit);
        if (tokens == null || tokens.isEmpty()) {
            if (items.size() <= max) return items;
            return new ArrayList<>(items.subList(0, max));
        }

        List<NewsItem> out = new ArrayList<>();
        for (NewsItem ni : items) {
            if (ni == null) continue;
            if (!isRelevant(ni, tokens)) continue;
            out.add(ni);
            if (out.size() >= max) break;
        }
        return out;
    }

    private static boolean isRelevant(NewsItem ni, Set<String> tokens) {
        String text = normalizeText(ni.title) + " " + normalizeText(ni.link) + " " + normalizeText(ni.source);
        for (String token : tokens) {
            if (text.contains(token)) return true;
        }
        return false;
    }

    private static Set<String> relevanceTokens(String ticker, List<String> queries) {
        Set<String> out = new LinkedHashSet<>();
        addToken(out, ticker);
        addToken(out, numericCode(ticker));

        if (queries != null) {
            for (String q : queries) {
                if (q == null) continue;
                addToken(out, q);
                for (String raw : q.split("[^\\p{L}\\p{N}.]+")) {
                    addToken(out, raw);
                }
            }
        }
        return out;
    }

    private static void addToken(Set<String> out, String raw) {
        if (raw == null) return;
        String t = raw.trim().toLowerCase(Locale.ROOT);
        if (t.startsWith("\"") && t.endsWith("\"") && t.length() > 1) {
            t = t.substring(1, t.length() - 1).trim();
        }
        if (t.length() < 2) return;
        if (t.matches("\\d{1,2}")) return;
        if (TOKEN_STOPWORDS.contains(t)) return;
        out.add(t);
    }

    private static String numericCode(String ticker) {
        if (ticker == null) return "";
        String t = ticker.trim().toUpperCase(Locale.ROOT);
        int dot = t.indexOf('.');
        if (dot <= 0) return "";
        String head = t.substring(0, dot);
        if (head.matches("\\d{4,6}")) return head;
        return "";
    }

    private static String normalizeText(String text) {
        if (text == null) return "";
        return text.trim().toLowerCase(Locale.ROOT);
    }
}
