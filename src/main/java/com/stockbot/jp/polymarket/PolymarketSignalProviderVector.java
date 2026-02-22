package com.stockbot.jp.polymarket;

import com.stockbot.jp.config.Config;
import com.stockbot.jp.model.WatchItem;
import com.stockbot.jp.vector.VectorSearchService;
import org.json.JSONObject;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Vector signal provider backed by docs/VectorSearchService persistent retrieval.
 */
public final class PolymarketSignalProviderVector implements PolymarketSignalProvider {
    private static final int EMBED_DIM = 1536;
    private static final String DOC_TYPE = "POLYMARKET_MARKET";
    private static final String DOC_SOURCE = "polymarket";

    private final Config config;
    private final PolymarketClient client;
    private final VectorSearchService vectorSearchService;

    public PolymarketSignalProviderVector(Config config, PolymarketClient client) {
        this(config, client, null);
    }

    public PolymarketSignalProviderVector(
            Config config,
            PolymarketClient client,
            VectorSearchService vectorSearchService
    ) {
        this.config = config;
        this.client = client;
        this.vectorSearchService = vectorSearchService;
    }

    @Override
    public PolymarketSignalReport collectSignals(List<WatchItem> watchlist, Instant now) {
        if (watchlist == null || watchlist.isEmpty()) {
            return PolymarketSignalReport.empty("No match (watchlist is empty)");
        }

        PolymarketClient.FetchResult fetched = client.fetchMarkets();
        if (!fetched.success) {
            return PolymarketSignalReport.empty("Data source failure: " + fetched.reason);
        }
        if (fetched.markets.isEmpty()) {
            return PolymarketSignalReport.empty("No match (markets=0)");
        }

        ScoringParams scoring = loadScoringParams();
        if (vectorSearchService == null) {
            return collectByMemorySimilarity(
                    watchlist,
                    fetched.markets,
                    now,
                    scoring,
                    "vector mode (memory_fallback, reason=no_docs_backend, " + sourceMode(fetched.fromCache) + ")"
            );
        }

        try {
            return collectByPersistentVector(watchlist, fetched, now, scoring);
        } catch (Exception e) {
            String reason = e.getClass().getSimpleName();
            return collectByMemorySimilarity(
                    watchlist,
                    fetched.markets,
                    now,
                    scoring,
                    "vector mode (memory_fallback, reason=" + reason + ", " + sourceMode(fetched.fromCache) + ")"
            );
        }
    }

    private PolymarketSignalReport collectByPersistentVector(
            List<WatchItem> watchlist,
            PolymarketClient.FetchResult fetched,
            Instant now,
            ScoringParams scoring
    ) throws Exception {
        int upserted = upsertMarketsToDocs(fetched.markets);
        int retrievalTopK = Math.max(
                scoring.topK * 8,
                Math.max(10, config.getInt("vector.memory.signal.top_k", 10))
        );
        int matchedRows = 0;

        Map<String, PolymarketClient.MarketRecord> idLookup = new HashMap<>();
        Map<String, PolymarketClient.MarketRecord> titleLookup = new HashMap<>();
        for (PolymarketClient.MarketRecord market : fetched.markets) {
            if (market == null) {
                continue;
            }
            String id = safe(market.id, "");
            if (!id.isEmpty()) {
                idLookup.put(id, market);
            }
            String titleKey = normalizeText(market.title);
            if (!titleKey.isEmpty()) {
                titleLookup.put(titleKey, market);
            }
        }

        Map<String, AggregatedMarket> aggregate = new LinkedHashMap<>();
        boolean liquidityMissing = false;
        for (WatchItem item : watchlist) {
            if (item == null) {
                continue;
            }
            String query = buildWatchQuery(item);
            if (query.isBlank()) {
                continue;
            }
            float[] queryVec = embed(query);
            List<VectorSearchService.DocMatch> matches = vectorSearchService.searchSimilar(
                    query,
                    queryVec,
                    retrievalTopK,
                    new VectorSearchService.SearchFilters(DOC_TYPE, null, null)
            );
            if (matches == null || matches.isEmpty()) {
                continue;
            }
            for (VectorSearchService.DocMatch match : matches) {
                if (match == null) {
                    continue;
                }
                PolymarketClient.MarketRecord market = resolveMarket(match, idLookup, titleLookup);
                if (market == null || safe(market.title, "").isEmpty()) {
                    continue;
                }
                double similarity = similarityFromDistance(match.distance);
                if (similarity <= 0.0) {
                    similarity = cosine(queryVec, embed(market.searchableText()));
                }
                if (!Double.isFinite(similarity) || similarity <= 0.0) {
                    continue;
                }

                double recency = recencyScore(market.updatedAt, now);
                double liquidity = liquidityScore(market.liquidity);
                if (market.liquidity <= 0.0) {
                    liquidityMissing = true;
                }
                double confidence = confidenceScore(market.yesPrice, market.noPrice);
                double score = scoring.wSim * similarity
                        + scoring.wRecency * recency
                        + scoring.wLiquidity * liquidity
                        + scoring.wConfidence * confidence;

                String key = marketKey(market, match);
                AggregatedMarket row = aggregate.computeIfAbsent(key, k -> new AggregatedMarket(market));
                row.observe(item, similarity, score, scoring.minSimilarity, recency, liquidity, confidence);
                matchedRows++;
            }
        }

        List<ScoredMarket> scored = new ArrayList<>();
        List<ScoredMarket> nearMiss = new ArrayList<>();
        for (AggregatedMarket row : aggregate.values()) {
            ScoredMarket one = row.toScoredMarket();
            if (one.similarity >= scoring.minSimilarity && !one.impacts.isEmpty()) {
                scored.add(one);
            } else {
                nearMiss.add(one);
            }
        }

        String status = "vector mode (docs_persistent, " + sourceMode(fetched.fromCache)
                + ", docs_upserted=" + upserted
                + ", query_matches=" + matchedRows + ")";
        return buildReport(scored, nearMiss, scoring.topK, scoring.minSimilarity, liquidityMissing, status);
    }

    private PolymarketSignalReport collectByMemorySimilarity(
            List<WatchItem> watchlist,
            List<PolymarketClient.MarketRecord> markets,
            Instant now,
            ScoringParams scoring,
            String status
    ) {
        List<ScoredMarket> scored = new ArrayList<>();
        List<ScoredMarket> nearMiss = new ArrayList<>();
        boolean liquidityMissing = false;

        for (PolymarketClient.MarketRecord market : markets) {
            if (market == null) {
                continue;
            }
            float[] marketVec = embed(market.searchableText());
            double bestSimilarity = -1.0;
            double bestScore = -1.0;
            Map<String, PolymarketWatchImpact> impacts = new LinkedHashMap<>();

            for (WatchItem item : watchlist) {
                if (item == null) {
                    continue;
                }
                String query = buildWatchQuery(item);
                if (query.isBlank()) {
                    continue;
                }
                float[] queryVec = embed(query);
                double similarity = cosine(queryVec, marketVec);
                if (!Double.isFinite(similarity) || similarity <= 0.0) {
                    continue;
                }

                double recency = recencyScore(market.updatedAt, now);
                double liquidity = liquidityScore(market.liquidity);
                if (market.liquidity <= 0.0) {
                    liquidityMissing = true;
                }
                double confidence = confidenceScore(market.yesPrice, market.noPrice);
                double score = scoring.wSim * similarity
                        + scoring.wRecency * recency
                        + scoring.wLiquidity * liquidity
                        + scoring.wConfidence * confidence;

                bestSimilarity = Math.max(bestSimilarity, similarity);
                bestScore = Math.max(bestScore, score);
                if (similarity >= scoring.minSimilarity) {
                    putImpact(impacts, item, market, score, similarity, recency, liquidity, confidence);
                }
            }

            if (bestSimilarity < 0.0 || bestScore < 0.0) {
                continue;
            }
            ScoredMarket one = new ScoredMarket(
                    market,
                    new ArrayList<>(impacts.values()),
                    bestScore,
                    bestSimilarity
            );
            if (bestSimilarity >= scoring.minSimilarity && !one.impacts.isEmpty()) {
                scored.add(one);
            } else {
                nearMiss.add(one);
            }
        }

        return buildReport(scored, nearMiss, scoring.topK, scoring.minSimilarity, liquidityMissing, status);
    }

    private PolymarketSignalReport buildReport(
            List<ScoredMarket> scored,
            List<ScoredMarket> nearMiss,
            int topK,
            double minSimilarity,
            boolean liquidityMissing,
            String status
    ) {
        scored.sort(Comparator.comparingDouble((ScoredMarket s) -> s.score).reversed());
        nearMiss.sort(Comparator.comparingDouble((ScoredMarket s) -> s.similarity).reversed());

        if (scored.isEmpty()) {
            StringBuilder reason = new StringBuilder();
            reason.append("No match (min_similarity=")
                    .append(String.format(Locale.US, "%.2f", minSimilarity))
                    .append(")");
            int n = Math.min(3, nearMiss.size());
            if (n > 0) {
                reason.append("; closest below threshold: ");
                List<String> parts = new ArrayList<>();
                for (int i = 0; i < n; i++) {
                    ScoredMarket x = nearMiss.get(i);
                    parts.add(x.market.title + " (sim=" + String.format(Locale.US, "%.2f", x.similarity) + ")");
                }
                reason.append(String.join(" | ", parts));
            }
            if (liquidityMissing) {
                reason.append("; liquidity missing treated as 0");
            }
            return PolymarketSignalReport.empty(reason.toString());
        }

        List<PolymarketTopicSignal> out = new ArrayList<>();
        for (ScoredMarket x : scored) {
            out.add(new PolymarketTopicSignal(
                    "vector",
                    normalizePct(x.market.yesPrice),
                    x.market.change24hPct,
                    x.score >= 0.60 ? "up" : (x.score <= 0.40 ? "down" : "flat"),
                    List.of(safe(x.market.category, "general")),
                    x.impacts,
                    x.market.title
            ));
            if (out.size() >= topK) {
                break;
            }
        }

        return new PolymarketSignalReport(true, appendLiquidity(status, liquidityMissing), out);
    }

    private int upsertMarketsToDocs(List<PolymarketClient.MarketRecord> markets) throws Exception {
        int success = 0;
        Exception firstError = null;
        for (PolymarketClient.MarketRecord market : markets) {
            if (market == null || safe(market.title, "").isEmpty()) {
                continue;
            }
            try {
                vectorSearchService.upsertDoc(new VectorSearchService.Doc(
                        DOC_TYPE,
                        null,
                        trimToNull(market.title),
                        marketPayload(market).toString(),
                        null,
                        DOC_SOURCE,
                        market.updatedAt,
                        contentHashForMarket(market),
                        embed(market.searchableText())
                ));
                success++;
            } catch (Exception e) {
                if (firstError == null) {
                    firstError = e;
                }
            }
        }
        if (success <= 0 && firstError != null) {
            throw firstError;
        }
        return success;
    }

    private JSONObject marketPayload(PolymarketClient.MarketRecord market) {
        JSONObject payload = new JSONObject();
        payload.put("id", safe(market.id, ""));
        payload.put("title", safe(market.title, ""));
        payload.put("description", safe(market.description, ""));
        payload.put("category", safe(market.category, ""));
        payload.put("updated_at", market.updatedAt == null ? "" : market.updatedAt.toString());
        payload.put("liquidity", market.liquidity);
        payload.put("yes_price", market.yesPrice);
        payload.put("no_price", market.noPrice);
        payload.put("change24h_pct", market.change24hPct);
        return payload;
    }

    private PolymarketClient.MarketRecord resolveMarket(
            VectorSearchService.DocMatch match,
            Map<String, PolymarketClient.MarketRecord> idLookup,
            Map<String, PolymarketClient.MarketRecord> titleLookup
    ) {
        PolymarketClient.MarketRecord payloadRecord = parseRecordFromDoc(match);
        if (payloadRecord == null) {
            return null;
        }
        String id = safe(payloadRecord.id, "");
        if (!id.isEmpty() && idLookup.containsKey(id)) {
            return idLookup.get(id);
        }
        String titleKey = normalizeText(payloadRecord.title);
        if (!titleKey.isEmpty() && titleLookup.containsKey(titleKey)) {
            return titleLookup.get(titleKey);
        }
        return payloadRecord;
    }

    private PolymarketClient.MarketRecord parseRecordFromDoc(VectorSearchService.DocMatch match) {
        if (match == null) {
            return null;
        }
        JSONObject payload = safeJson(match.content);
        String id = safe(payload.optString("id", ""), "");
        String title = safe(payload.optString("title", ""), safe(match.title, ""));
        String description = safe(payload.optString("description", ""), "");
        String category = safe(payload.optString("category", ""), "");
        Instant updatedAt = parseInstant(payload.optString("updated_at", ""));
        double liquidity = asFinite(payload.opt("liquidity"), 0.0);
        double yesPrice = asFinite(payload.opt("yes_price"), Double.NaN);
        double noPrice = asFinite(payload.opt("no_price"), Double.NaN);
        double change24hPct = asFinite(payload.opt("change24h_pct"), 0.0);
        if (title.isEmpty()) {
            return null;
        }
        return new PolymarketClient.MarketRecord(
                id,
                title,
                description,
                category,
                updatedAt,
                liquidity,
                yesPrice,
                noPrice,
                change24hPct
        );
    }

    private String contentHashForMarket(PolymarketClient.MarketRecord market) {
        String key = safe(market.id, "");
        if (key.isEmpty()) {
            key = normalizeText(safe(market.title, "") + "|" + safe(market.category, ""));
        }
        if (key.isEmpty()) {
            key = Integer.toHexString(System.identityHashCode(market));
        }
        return "poly_market:" + key;
    }

    private String marketKey(PolymarketClient.MarketRecord market, VectorSearchService.DocMatch match) {
        String id = safe(market.id, "");
        if (!id.isEmpty()) {
            return id;
        }
        String contentHash = match == null ? "" : safe(match.contentHash, "");
        if (!contentHash.isEmpty()) {
            return contentHash;
        }
        String title = normalizeText(safe(market.title, ""));
        if (!title.isEmpty()) {
            return title;
        }
        return "market_" + Integer.toHexString(System.identityHashCode(market));
    }

    private void putImpact(
            Map<String, PolymarketWatchImpact> impacts,
            WatchItem item,
            PolymarketClient.MarketRecord market,
            double score,
            double similarity,
            double recency,
            double liquidity,
            double confidence
    ) {
        String code = safe(item == null ? "" : item.displayCode, safe(item == null ? "" : item.ticker, ""));
        if (code.isEmpty()) {
            return;
        }
        PolymarketWatchImpact candidate = new PolymarketWatchImpact(
                code,
                inferImpact(market.yesPrice),
                clamp01(score),
                String.format(
                        Locale.US,
                        "sim=%.2f rec=%.2f liq=%.2f conf=%.2f",
                        similarity,
                        recency,
                        liquidity,
                        confidence
                )
        );
        PolymarketWatchImpact existing = impacts.get(code);
        if (existing == null || candidate.confidence > existing.confidence) {
            impacts.put(code, candidate);
        }
    }

    private String buildWatchQuery(WatchItem item) {
        return (safe(item.displayCode, "") + " "
                + safe(item.displayNameLocal, "") + " "
                + safe(item.displayNameEn, "") + " "
                + safe(item.market, "")).trim();
    }

    private float[] embed(String text) {
        float[] vec = new float[EMBED_DIM];
        if (text == null || text.isBlank()) {
            return vec;
        }
        String normalized = normalizeText(text)
                .toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9 ]", " ")
                .replaceAll("\\s+", " ")
                .trim();
        if (normalized.isEmpty()) {
            return vec;
        }
        String[] tokens = normalized.split(" ");
        for (String token : tokens) {
            if (token.isBlank()) {
                continue;
            }
            int idx = Math.floorMod(token.hashCode(), EMBED_DIM);
            vec[idx] += 1.0f;
        }
        return vec;
    }

    private double cosine(float[] a, float[] b) {
        if (a == null || b == null || a.length != b.length) {
            return 0.0;
        }
        double dot = 0.0;
        double na = 0.0;
        double nb = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += a[i] * b[i];
            na += a[i] * a[i];
            nb += b[i] * b[i];
        }
        if (na <= 0.0 || nb <= 0.0) {
            return 0.0;
        }
        return dot / (Math.sqrt(na) * Math.sqrt(nb));
    }

    private double similarityFromDistance(Double distance) {
        if (distance == null || !Double.isFinite(distance)) {
            return 0.0;
        }
        double similarity = 1.0 - distance;
        if (similarity > 1.0) {
            return 1.0;
        }
        if (similarity < -1.0) {
            return -1.0;
        }
        return similarity;
    }

    private double recencyScore(Instant updatedAt, Instant now) {
        if (updatedAt == null || now == null) {
            return 0.0;
        }
        long hours = Math.max(0L, Duration.between(updatedAt, now).toHours());
        return Math.exp(-hours / 72.0);
    }

    private double liquidityScore(double liquidity) {
        if (!Double.isFinite(liquidity) || liquidity <= 0.0) {
            return 0.0;
        }
        return Math.min(1.0, Math.log1p(liquidity) / 10.0);
    }

    private double confidenceScore(double yesPrice, double noPrice) {
        if (!Double.isFinite(yesPrice)) {
            return 0.0;
        }
        double yes = yesPrice > 1.0 ? yesPrice / 100.0 : yesPrice;
        if (yes < 0.0 || yes > 1.0) {
            return 0.0;
        }
        double center = Math.abs(yes - 0.5) * 2.0;
        if (Double.isFinite(noPrice)) {
            double no = noPrice > 1.0 ? noPrice / 100.0 : noPrice;
            if (no >= 0.0 && no <= 1.0) {
                center = Math.max(center, Math.abs(no - 0.5) * 2.0);
            }
        }
        return clamp01(center);
    }

    private String inferImpact(double yesPrice) {
        if (!Double.isFinite(yesPrice)) {
            return "neutral";
        }
        double p = yesPrice > 1.0 ? yesPrice / 100.0 : yesPrice;
        if (p >= 0.55) {
            return "positive";
        }
        if (p <= 0.45) {
            return "negative";
        }
        return "neutral";
    }

    private double normalizePct(double value) {
        if (!Double.isFinite(value)) {
            return 50.0;
        }
        if (value >= 0.0 && value <= 1.0) {
            return value * 100.0;
        }
        return Math.max(0.0, Math.min(100.0, value));
    }

    private double clamp01(double value) {
        if (!Double.isFinite(value)) {
            return 0.0;
        }
        if (value < 0.0) {
            return 0.0;
        }
        if (value > 1.0) {
            return 1.0;
        }
        return value;
    }

    private ScoringParams loadScoringParams() {
        double minSimilarity = config.getDouble("polymarket.vector.min_similarity", 0.35);
        int topK = Math.max(1, config.getInt("polymarket.vector.top_k", 5));
        return new ScoringParams(
                minSimilarity,
                topK,
                config.getDouble("polymarket.weights.sim", 0.55),
                config.getDouble("polymarket.weights.recency", 0.20),
                config.getDouble("polymarket.weights.liquidity", 0.15),
                config.getDouble("polymarket.weights.confidence", 0.10)
        );
    }

    private String sourceMode(boolean fromCache) {
        return fromCache ? "cache_hit" : "live_fetch";
    }

    private String appendLiquidity(String status, boolean liquidityMissing) {
        if (!liquidityMissing || status == null || status.isBlank()) {
            return safe(status, "");
        }
        if (status.endsWith(")")) {
            return status.substring(0, status.length() - 1) + ", liquidity_missing_as_zero)";
        }
        return status + ", liquidity_missing_as_zero";
    }

    private JSONObject safeJson(String raw) {
        if (raw == null || raw.isBlank()) {
            return new JSONObject();
        }
        try {
            return new JSONObject(raw);
        } catch (Exception ignored) {
            return new JSONObject();
        }
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

    private double asFinite(Object value, double fallback) {
        if (value == null || value == JSONObject.NULL) {
            return fallback;
        }
        if (value instanceof Number) {
            double d = ((Number) value).doubleValue();
            return Double.isFinite(d) ? d : fallback;
        }
        try {
            double d = Double.parseDouble(value.toString().trim());
            return Double.isFinite(d) ? d : fallback;
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private String trimToNull(String value) {
        String out = safe(value, "").trim();
        return out.isEmpty() ? null : out;
    }

    private String normalizeText(String value) {
        return safe(value, "").replace("\r", " ").replace("\n", " ").replaceAll("\\s{2,}", " ").trim();
    }

    private String safe(String value, String fallback) {
        if (value == null || value.trim().isEmpty()) {
            return fallback == null ? "" : fallback;
        }
        return value.trim();
    }

    private static final class ScoringParams {
        final double minSimilarity;
        final int topK;
        final double wSim;
        final double wRecency;
        final double wLiquidity;
        final double wConfidence;

        private ScoringParams(
                double minSimilarity,
                int topK,
                double wSim,
                double wRecency,
                double wLiquidity,
                double wConfidence
        ) {
            this.minSimilarity = minSimilarity;
            this.topK = topK;
            this.wSim = wSim;
            this.wRecency = wRecency;
            this.wLiquidity = wLiquidity;
            this.wConfidence = wConfidence;
        }
    }

    private final class AggregatedMarket {
        private PolymarketClient.MarketRecord market;
        private final Map<String, PolymarketWatchImpact> impacts = new LinkedHashMap<>();
        private double bestScore = -1.0;
        private double bestSimilarity = -1.0;

        private AggregatedMarket(PolymarketClient.MarketRecord market) {
            this.market = market;
        }

        private void observe(
                WatchItem item,
                double similarity,
                double score,
                double minSimilarity,
                double recency,
                double liquidity,
                double confidence
        ) {
            if (market == null && item != null) {
                return;
            }
            bestScore = Math.max(bestScore, score);
            bestSimilarity = Math.max(bestSimilarity, similarity);
            if (similarity >= minSimilarity) {
                putImpact(impacts, item, market, score, similarity, recency, liquidity, confidence);
            }
        }

        private ScoredMarket toScoredMarket() {
            return new ScoredMarket(
                    market,
                    new ArrayList<>(impacts.values()),
                    bestScore,
                    bestSimilarity
            );
        }
    }

    private static final class ScoredMarket {
        final PolymarketClient.MarketRecord market;
        final List<PolymarketWatchImpact> impacts;
        final double score;
        final double similarity;

        private ScoredMarket(
                PolymarketClient.MarketRecord market,
                List<PolymarketWatchImpact> impacts,
                double score,
                double similarity
        ) {
            this.market = market;
            this.impacts = impacts == null ? List.of() : List.copyOf(impacts);
            this.score = score;
            this.similarity = similarity;
        }
    }
}
