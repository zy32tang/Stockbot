package com.stockbot.jp.news;

import com.stockbot.data.http.HttpClientEx;
import com.stockbot.jp.config.Config;
import com.stockbot.model.NewsItem;
import lombok.Builder;
import lombok.Value;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * End-to-end watchlist news pipeline:
 * RSS ingest -> DB -> embedding -> pgvector search -> dedupe/cluster -> LangChain summary.
 */
public final class WatchlistNewsPipeline {
    private static final DateTimeFormatter DIGEST_TS_FMT = DateTimeFormatter.ofPattern("MM-dd HH:mm");

    private final Config config;
    private final NewsItemDao newsItemDao;
    private final NewsIngestor newsIngestor;
    private final OllamaEmbeddingService embeddingService;
    private final LangChainSummaryService summaryService;

    public WatchlistNewsPipeline(Config config, HttpClientEx httpClient, NewsItemDao newsItemDao) {
        this.config = config;
        this.newsItemDao = newsItemDao;
        this.newsIngestor = new NewsIngestor(config, httpClient, newsItemDao);
        this.embeddingService = new OllamaEmbeddingService(config, httpClient, newsItemDao);
        this.summaryService = new LangChainSummaryService(config);
    }

    public PipelineResult processTicker(
            String ticker,
            String companyName,
            String industryZh,
            String industryEn,
            List<String> queries,
            String lang,
            String region
    ) {
        NewsIngestor.IngestResult ingestResult = newsIngestor.ingest(ticker, queries, lang, region);
        int embedded = embeddingService.embedMissing(Math.max(20, config.getInt("news.embedding.batch_size", 200)));

        String queryText = buildQueryText(ticker, companyName, industryZh, industryEn, queries);
        float[] queryEmbedding = embeddingService.embedText(queryText);
        if (queryEmbedding.length == 0) {
            return PipelineResult.empty(ingestResult.sourceLabel + "->pgvector", ingestResult.fetchedCount, embedded);
        }

        int topK = Math.max(1, config.getInt("vector.memory.news.top_k", 12));
        int lookbackDays = Math.max(1, config.getInt("news.lookback_days", 7));
        List<NewsItemDao.NewsItemRecord> matches;
        try {
            matches = newsItemDao.searchSimilar(
                    queryEmbedding,
                    new NewsItemDao.SearchOptions(topK * 3, lookbackDays, lang, region)
            );
        } catch (SQLException e) {
            System.err.println("WARN: news search failed ticker=" + safe(ticker) + ", err=" + e.getMessage());
            matches = List.of();
        }
        if (matches.isEmpty()) {
            return PipelineResult.empty(ingestResult.sourceLabel + "->pgvector", ingestResult.fetchedCount, embedded);
        }

        double dedupThreshold = clamp(config.getDouble("news.dedup.cosine_threshold", 0.97), 0.7, 0.9999);
        double clusterThreshold = clamp(config.getDouble("news.cluster.cosine_threshold", 0.90), 0.5, 0.9999);
        List<NewsItemDao.NewsItemRecord> deduped = deduplicate(matches, dedupThreshold);

        deduped.sort(Comparator
                .comparingDouble((NewsItemDao.NewsItemRecord it) -> it.getSimilarity()).reversed()
                .thenComparing((NewsItemDao.NewsItemRecord it) -> timestampOrMin(it.getPublishedAt())).reversed());
        if (deduped.size() > topK) {
            deduped = new ArrayList<>(deduped.subList(0, topK));
        }

        List<NewsCluster> clusters = cluster(deduped, clusterThreshold);
        String summaryHtml = summaryService.summarize(
                ticker,
                companyName,
                toSummaryClusters(clusters)
        );
        List<String> digestLines = buildDigestLines(clusters);
        List<NewsItem> topNews = toNewsItems(deduped);

        return new PipelineResult(
                topNews,
                digestLines,
                summaryHtml,
                ingestResult.sourceLabel + "->pgvector",
                ingestResult.fetchedCount,
                embedded,
                clusters
        );
    }

    private List<NewsItemDao.NewsItemRecord> deduplicate(List<NewsItemDao.NewsItemRecord> items, double threshold) {
        List<NewsItemDao.NewsItemRecord> sorted = new ArrayList<>(items);
        sorted.sort(Comparator
                .comparing((NewsItemDao.NewsItemRecord it) -> timestampOrMin(it.getPublishedAt())).reversed()
                .thenComparingInt((NewsItemDao.NewsItemRecord it) -> safe(it.getContent()).length()).reversed());

        List<NewsItemDao.NewsItemRecord> kept = new ArrayList<>();
        for (NewsItemDao.NewsItemRecord item : sorted) {
            if (item == null) {
                continue;
            }
            int duplicateIdx = -1;
            for (int i = 0; i < kept.size(); i++) {
                double sim = cosine(item.getEmbedding(), kept.get(i).getEmbedding());
                if (sim >= threshold) {
                    duplicateIdx = i;
                    break;
                }
            }
            if (duplicateIdx < 0) {
                kept.add(item);
            } else if (prefer(item, kept.get(duplicateIdx))) {
                kept.set(duplicateIdx, item);
            }
        }
        return kept;
    }

    private boolean prefer(NewsItemDao.NewsItemRecord next, NewsItemDao.NewsItemRecord old) {
        if (next == null) {
            return false;
        }
        if (old == null) {
            return true;
        }
        OffsetDateTime nt = next.getPublishedAt();
        OffsetDateTime ot = old.getPublishedAt();
        if (nt != null && ot != null && nt.isAfter(ot)) {
            return true;
        }
        if (nt != null && ot == null) {
            return true;
        }
        int nextInfo = safe(next.getTitle()).length() + safe(next.getContent()).length();
        int oldInfo = safe(old.getTitle()).length() + safe(old.getContent()).length();
        return nextInfo > oldInfo;
    }

    private List<NewsCluster> cluster(List<NewsItemDao.NewsItemRecord> items, double threshold) {
        List<NewsCluster> clusters = new ArrayList<>();
        for (NewsItemDao.NewsItemRecord item : items) {
            if (item == null) {
                continue;
            }
            int best = -1;
            double bestSim = -1.0;
            for (int i = 0; i < clusters.size(); i++) {
                NewsCluster cluster = clusters.get(i);
                double sim = cosine(item.getEmbedding(), cluster.centroid);
                if (sim >= threshold && sim > bestSim) {
                    best = i;
                    bestSim = sim;
                }
            }
            if (best < 0) {
                clusters.add(NewsCluster.from(item));
            } else {
                clusters.get(best).add(item);
            }
        }

        for (NewsCluster cluster : clusters) {
            cluster.refreshLabel();
        }
        clusters.sort(Comparator
                .comparing((NewsCluster c) -> timestampOrMin(c.latestPublishedAt)).reversed()
                .thenComparingInt(c -> c.items.size()).reversed());
        return clusters;
    }

    private List<LangChainSummaryService.ClusterInput> toSummaryClusters(List<NewsCluster> clusters) {
        List<LangChainSummaryService.ClusterInput> out = new ArrayList<>();
        for (NewsCluster cluster : clusters) {
            if (cluster == null || cluster.items.isEmpty()) {
                continue;
            }
            List<String> lines = new ArrayList<>();
            for (int i = 0; i < cluster.items.size() && i < 4; i++) {
                NewsItemDao.NewsItemRecord item = cluster.items.get(i);
                lines.add(safe(item.getTitle()));
            }
            out.add(new LangChainSummaryService.ClusterInput(cluster.label, lines));
        }
        return out;
    }

    private List<String> buildDigestLines(List<NewsCluster> clusters) {
        int maxDigestItems = Math.max(3, config.getInt("watchlist.news.digest_items", 8));
        List<String> out = new ArrayList<>();
        for (NewsCluster cluster : clusters) {
            if (cluster == null || cluster.items.isEmpty()) {
                continue;
            }
            NewsItemDao.NewsItemRecord top = cluster.items.get(0);
            StringBuilder line = new StringBuilder();
            line.append(safe(top.getTitle()));
            if (!safe(top.getSource()).isEmpty()) {
                line.append(" | ").append(top.getSource());
            }
            if (top.getPublishedAt() != null) {
                line.append(" | ").append(DIGEST_TS_FMT.format(top.getPublishedAt()));
            }
            out.add(line.toString());
            if (out.size() >= maxDigestItems) {
                break;
            }
        }
        return out;
    }

    private List<NewsItem> toNewsItems(List<NewsItemDao.NewsItemRecord> items) {
        List<NewsItem> out = new ArrayList<>();
        for (NewsItemDao.NewsItemRecord item : items) {
            if (item == null) {
                continue;
            }
            out.add(new NewsItem(
                    safe(item.getTitle()),
                    safe(item.getContent()),
                    safe(item.getUrl()),
                    safe(item.getSource()),
                    item.getPublishedAt() == null ? null : item.getPublishedAt().toZonedDateTime()
            ));
        }
        return out;
    }

    private String buildQueryText(
            String ticker,
            String companyName,
            String industryZh,
            String industryEn,
            List<String> queries
    ) {
        List<String> tokens = new ArrayList<>();
        addIfPresent(tokens, ticker);
        addIfPresent(tokens, companyName);
        addIfPresent(tokens, industryZh);
        addIfPresent(tokens, industryEn);
        if (queries != null) {
            for (int i = 0; i < queries.size() && i < 5; i++) {
                addIfPresent(tokens, queries.get(i));
            }
        }
        if (tokens.isEmpty()) {
            tokens.add("market");
        }
        return String.join(" ", tokens);
    }

    private void addIfPresent(List<String> out, String value) {
        String text = safe(value).replace('\u3000', ' ').replaceAll("\\s+", " ").trim();
        if (!text.isEmpty()) {
            out.add(text);
        }
    }

    private double cosine(float[] a, float[] b) {
        if (a == null || b == null || a.length == 0 || b.length == 0) {
            return 0.0;
        }
        int size = Math.min(a.length, b.length);
        double dot = 0.0;
        double na = 0.0;
        double nb = 0.0;
        for (int i = 0; i < size; i++) {
            double va = a[i];
            double vb = b[i];
            dot += va * vb;
            na += va * va;
            nb += vb * vb;
        }
        if (na <= 0.0 || nb <= 0.0) {
            return 0.0;
        }
        return dot / Math.sqrt(na * nb);
    }

    private OffsetDateTime timestampOrMin(OffsetDateTime value) {
        return value == null ? OffsetDateTime.MIN : value;
    }

    private double clamp(double value, double min, double max) {
        if (!Double.isFinite(value)) {
            return min;
        }
        if (value < min) {
            return min;
        }
        if (value > max) {
            return max;
        }
        return value;
    }

    private String safe(String value) {
        return value == null ? "" : value;
    }

    @Value
    public static final class PipelineResult {
        public final List<NewsItem> newsItems;
        public final List<String> digestLines;
        public final String summaryHtml;
        public final String sourceLabel;
        public final int ingestedCount;
        public final int embeddedCount;
        public final List<NewsCluster> clusters;

        @Builder(toBuilder = true)
        public PipelineResult(
                List<NewsItem> newsItems,
                List<String> digestLines,
                String summaryHtml,
                String sourceLabel,
                int ingestedCount,
                int embeddedCount,
                List<NewsCluster> clusters
        ) {
            this.newsItems = newsItems == null ? List.of() : List.copyOf(newsItems);
            this.digestLines = digestLines == null ? List.of() : List.copyOf(digestLines);
            this.summaryHtml = summaryHtml == null ? "" : summaryHtml;
            this.sourceLabel = sourceLabel == null ? "" : sourceLabel;
            this.ingestedCount = Math.max(0, ingestedCount);
            this.embeddedCount = Math.max(0, embeddedCount);
            this.clusters = clusters == null ? List.of() : List.copyOf(clusters);
        }

        static PipelineResult empty(String sourceLabel, int ingestedCount, int embeddedCount) {
            return new PipelineResult(
                    List.of(),
                    List.of(),
                    "<p>No material event clusters found in recent news.</p>",
                    sourceLabel,
                    ingestedCount,
                    embeddedCount,
                    List.of()
            );
        }
    }

    public static final class NewsCluster {
        public String label;
        public final List<NewsItemDao.NewsItemRecord> items;
        public float[] centroid;
        public OffsetDateTime latestPublishedAt;

        private NewsCluster(NewsItemDao.NewsItemRecord seed) {
            this.items = new ArrayList<>();
            this.centroid = seed.getEmbedding() == null ? new float[0] : seed.getEmbedding().clone();
            add(seed);
            refreshLabel();
        }

        static NewsCluster from(NewsItemDao.NewsItemRecord seed) {
            return new NewsCluster(seed);
        }

        void add(NewsItemDao.NewsItemRecord item) {
            if (item == null) {
                return;
            }
            if (items.isEmpty()) {
                items.add(item);
                latestPublishedAt = item.getPublishedAt();
                if (item.getEmbedding() != null) {
                    centroid = item.getEmbedding().clone();
                }
                return;
            }
            int nextCount = items.size() + 1;
            if (item.getEmbedding() != null && item.getEmbedding().length > 0 && centroid != null && centroid.length > 0) {
                int size = Math.min(centroid.length, item.getEmbedding().length);
                for (int i = 0; i < size; i++) {
                    centroid[i] = (float) ((centroid[i] * items.size() + item.getEmbedding()[i]) / nextCount);
                }
            }
            items.add(item);
            if (latestPublishedAt == null || (item.getPublishedAt() != null && item.getPublishedAt().isAfter(latestPublishedAt))) {
                latestPublishedAt = item.getPublishedAt();
            }
            items.sort(Comparator
                    .comparingDouble((NewsItemDao.NewsItemRecord it) -> it.getSimilarity()).reversed()
                    .thenComparing((NewsItemDao.NewsItemRecord it) -> it.getPublishedAt() == null ? OffsetDateTime.MIN : it.getPublishedAt()).reversed());
        }

        void refreshLabel() {
            if (items.isEmpty()) {
                this.label = "cluster";
                return;
            }
            String base = items.get(0).getTitle() == null ? "" : items.get(0).getTitle().trim();
            if (base.isEmpty()) {
                base = "event cluster";
            }
            if (items.size() > 1) {
                base = base + " (+" + (items.size() - 1) + ")";
            }
            this.label = base;
        }
    }
}
