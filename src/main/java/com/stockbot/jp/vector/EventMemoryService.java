package com.stockbot.jp.vector;

import com.stockbot.jp.config.Config;
import com.stockbot.jp.db.BarDailyDao;
import com.stockbot.model.NewsItem;
import org.json.JSONArray;
import org.json.JSONObject;

import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;

/**
 * Event memory service backed by pgvector docs table.
 */
public final class EventMemoryService {
    private final Config config;
    private final VectorSearchService vectorSearchService;
    private final BarDailyDao barDailyDao;
    private final ZoneId marketZone;

    public EventMemoryService(Config config, VectorSearchService vectorSearchService, BarDailyDao barDailyDao) {
        this.config = config;
        this.vectorSearchService = vectorSearchService;
        this.barDailyDao = barDailyDao;
        this.marketZone = ZoneId.of(config.getString("app.zone", "Asia/Tokyo"));
    }

    public VectorSearchService vectorSearchService() {
        return vectorSearchService;
    }

    public MemoryInsights buildInsights(
            String watchItem,
            String ticker,
            String industryZh,
            String industryEn,
            List<NewsItem> news,
            String technicalStatus,
            String risk,
            String technicalReasonsJson,
            Instant asOf
    ) {
        NewsSimilaritySummary newsSummary = buildNewsSimilarity(watchItem, ticker, industryZh, industryEn, news);
        SignalExplanationSummary signalSummary = buildSignalExplanation(
                ticker,
                technicalStatus,
                risk,
                technicalReasonsJson,
                asOf
        );
        return new MemoryInsights(newsSummary, signalSummary);
    }

    public List<String> retrieveImpactedTickers(String queryText, int topK) {
        String keyword = normalizeText(queryText);
        if (keyword.isEmpty()) {
            return List.of();
        }
        List<VectorSearchService.DocMatch> matches;
        try {
            matches = vectorSearchService.searchSimilar(
                    keyword,
                    Math.max(1, topK),
                    new VectorSearchService.SearchFilters("NEWS", null, null)
            );
        } catch (Exception e) {
            return List.of();
        }
        Map<String, Integer> tickerHits = new LinkedHashMap<>();
        for (VectorSearchService.DocMatch match : matches) {
            if (match == null) {
                continue;
            }
            JSONObject payload = safeJson(match.content);
            String hitTicker = normalizeTicker(firstNonBlank(
                    match.ticker,
                    payload.optString("ticker", "")
            ));
            if (!hitTicker.isEmpty()) {
                increment(tickerHits, hitTicker);
            }
            JSONArray impactTickers = payload.optJSONArray("impact_tickers");
            if (impactTickers != null) {
                for (int i = 0; i < impactTickers.length(); i++) {
                    String t = normalizeTicker(impactTickers.optString(i, ""));
                    if (!t.isEmpty()) {
                        increment(tickerHits, t);
                    }
                }
            }
        }
        return topKeys(tickerHits, 10);
    }

    private NewsSimilaritySummary buildNewsSimilarity(
            String watchItem,
            String ticker,
            String industryZh,
            String industryEn,
            List<NewsItem> news
    ) {
        if (news == null || news.isEmpty()) {
            return NewsSimilaritySummary.empty();
        }

        int maxNewsItems = Math.max(1, config.getInt("vector.memory.news.max_items", 3));
        int topK = Math.max(2, config.getInt("vector.memory.news.top_k", 8));
        int maxCases = Math.max(1, config.getInt("vector.memory.news.max_cases", 5));
        Set<String> seenMatchHashes = new LinkedHashSet<>();
        Map<String, Integer> tickerHits = new LinkedHashMap<>();
        Map<String, Integer> industryHits = new LinkedHashMap<>();
        List<String> representativeCases = new ArrayList<>();
        ReturnStats stats1d = new ReturnStats();
        ReturnStats stats3d = new ReturnStats();
        ReturnStats stats5d = new ReturnStats();

        int analyzedNews = 0;
        int matchedEvents = 0;
        String error = "";

        for (NewsItem item : news) {
            if (analyzedNews >= maxNewsItems) {
                break;
            }
            if (item == null) {
                continue;
            }
            String title = normalizeText(item.title);
            if (title.isEmpty()) {
                continue;
            }
            analyzedNews++;
            Instant publishedAt = item.publishedAt == null ? null : item.publishedAt.toInstant();

            String insertedHash;
            try {
                insertedHash = vectorSearchService.upsertDoc(new VectorSearchService.Doc(
                        "NEWS",
                        trimToNull(ticker),
                        trimToNull(title),
                        buildNewsDocContent(watchItem, ticker, industryZh, industryEn, item, publishedAt),
                        null,
                        trimToNull(item.source),
                        publishedAt,
                        null,
                        null
                ));
            } catch (Exception e) {
                error = joinError(error, "upsert:" + e.getClass().getSimpleName());
                continue;
            }

            List<VectorSearchService.DocMatch> matches;
            try {
                matches = vectorSearchService.searchSimilar(
                        title,
                        topK,
                        new VectorSearchService.SearchFilters("NEWS", null, null)
                );
            } catch (Exception e) {
                error = joinError(error, "search:" + e.getClass().getSimpleName());
                continue;
            }

            for (VectorSearchService.DocMatch match : matches) {
                if (match == null || safe(match.contentHash).isEmpty()) {
                    continue;
                }
                if (match.contentHash.equals(insertedHash)) {
                    continue;
                }
                if (!seenMatchHashes.add(match.contentHash)) {
                    continue;
                }
                matchedEvents++;

                JSONObject payload = safeJson(match.content);
                String hitTicker = normalizeTicker(firstNonBlank(
                        match.ticker,
                        payload.optString("ticker", "")
                ));
                if (!hitTicker.isEmpty()) {
                    increment(tickerHits, hitTicker);
                }
                JSONArray impactTickers = payload.optJSONArray("impact_tickers");
                if (impactTickers != null) {
                    for (int i = 0; i < impactTickers.length(); i++) {
                        String t = normalizeTicker(impactTickers.optString(i, ""));
                        if (!t.isEmpty()) {
                            increment(tickerHits, t);
                        }
                    }
                }

                String industry = normalizeText(payload.optString("industry_en", ""));
                if (!industry.isEmpty()) {
                    increment(industryHits, industry);
                }
                JSONArray industries = payload.optJSONArray("beneficiary_industries");
                if (industries != null) {
                    for (int i = 0; i < industries.length(); i++) {
                        String entry = normalizeText(industries.optString(i, ""));
                        if (!entry.isEmpty()) {
                            increment(industryHits, entry);
                        }
                    }
                }

                String caseLine = buildCaseLine(match, payload);
                if (!caseLine.isEmpty() && representativeCases.size() < maxCases) {
                    representativeCases.add(caseLine);
                }

                appendForwardReturns(stats1d, hitTicker, match.publishedAt, 1);
                appendForwardReturns(stats3d, hitTicker, match.publishedAt, 3);
                appendForwardReturns(stats5d, hitTicker, match.publishedAt, 5);
            }
        }

        return new NewsSimilaritySummary(
                analyzedNews,
                matchedEvents,
                stats1d,
                stats3d,
                stats5d,
                topKeys(tickerHits, 8),
                topKeys(industryHits, 6),
                representativeCases,
                error
        );
    }

    private SignalExplanationSummary buildSignalExplanation(
            String ticker,
            String technicalStatus,
            String risk,
            String technicalReasonsJson,
            Instant asOf
    ) {
        String reasonText = buildReasonText(technicalStatus, risk, technicalReasonsJson);
        if (reasonText.isEmpty()) {
            return SignalExplanationSummary.empty();
        }

        String normalizedTicker = normalizeTicker(ticker);
        int topK = Math.max(2, config.getInt("vector.memory.signal.top_k", 6));
        int maxCases = Math.max(1, config.getInt("vector.memory.signal.max_cases", 5));
        String hash;
        try {
            hash = vectorSearchService.upsertDoc(new VectorSearchService.Doc(
                    "AI_REPORT",
                    trimToNull(normalizedTicker),
                    trimToNull(normalizedTicker + " " + normalizeText(technicalStatus) + " signal reason"),
                    buildSignalDocContent(normalizedTicker, technicalStatus, risk, reasonText, asOf),
                    null,
                    "stockbot",
                    asOf,
                    null,
                    null
            ));
        } catch (Exception e) {
            return SignalExplanationSummary.withError("upsert:" + e.getClass().getSimpleName());
        }

        List<VectorSearchService.DocMatch> matches;
        try {
            matches = vectorSearchService.searchSimilar(
                    reasonText,
                    topK,
                    new VectorSearchService.SearchFilters("AI_REPORT", null, null)
            );
        } catch (Exception e) {
            return SignalExplanationSummary.withError("search:" + e.getClass().getSimpleName());
        }

        int similarCount = 0;
        List<String> cases = new ArrayList<>();
        for (VectorSearchService.DocMatch match : matches) {
            if (match == null || safe(match.contentHash).isEmpty()) {
                continue;
            }
            if (match.contentHash.equals(hash)) {
                continue;
            }
            if (asOf != null && match.publishedAt != null && !match.publishedAt.isBefore(asOf)) {
                continue;
            }
            similarCount++;
            if (cases.size() < maxCases) {
                JSONObject payload = safeJson(match.content);
                String caseTicker = normalizeTicker(firstNonBlank(match.ticker, payload.optString("ticker", "")));
                String caseStatus = normalizeText(payload.optString("technical_status", ""));
                String date = match.publishedAt == null ? "-" : LocalDate.ofInstant(match.publishedAt, marketZone).toString();
                String line = (date + " " + firstNonBlank(caseTicker, "-")
                        + " " + firstNonBlank(caseStatus, "UNKNOWN")).trim();
                if (!line.isEmpty()) {
                    cases.add(line);
                }
            }
        }

        return new SignalExplanationSummary(
                normalizeText(technicalStatus),
                similarCount,
                cases,
                ""
        );
    }

    private void appendForwardReturns(ReturnStats stats, String ticker, Instant publishedAt, int offsetDays) {
        if (stats == null || ticker == null || ticker.trim().isEmpty() || publishedAt == null) {
            return;
        }
        try {
            LocalDate date = LocalDate.ofInstant(publishedAt, marketZone);
            OptionalDouble base = barDailyDao.closeOnOrAfterWithOffset(ticker, date, 0);
            OptionalDouble target = barDailyDao.closeOnOrAfterWithOffset(ticker, date, offsetDays);
            if (base.isPresent() && target.isPresent() && base.getAsDouble() > 0.0) {
                double pct = (target.getAsDouble() - base.getAsDouble()) * 100.0 / base.getAsDouble();
                stats.add(pct);
            }
        } catch (SQLException ignored) {
            // Best effort. Missing bars should not break report generation.
        }
    }

    private String buildNewsDocContent(
            String watchItem,
            String ticker,
            String industryZh,
            String industryEn,
            NewsItem item,
            Instant publishedAt
    ) {
        JSONObject payload = new JSONObject();
        payload.put("kind", "NEWS_EVENT");
        payload.put("watch_item", normalizeText(watchItem));
        payload.put("ticker", normalizeTicker(ticker));
        payload.put("title", normalizeText(item == null ? "" : item.title));
        payload.put("source", normalizeText(item == null ? "" : item.source));
        payload.put("industry_zh", normalizeText(industryZh));
        payload.put("industry_en", normalizeText(industryEn));
        if (publishedAt != null) {
            payload.put("published_at", publishedAt.toString());
        }
        JSONArray impactTickers = new JSONArray();
        String normalizedTicker = normalizeTicker(ticker);
        if (!normalizedTicker.isEmpty()) {
            impactTickers.put(normalizedTicker);
        }
        payload.put("impact_tickers", impactTickers);
        JSONArray industries = new JSONArray();
        if (!normalizeText(industryEn).isEmpty()) {
            industries.put(normalizeText(industryEn));
        }
        if (!normalizeText(industryZh).isEmpty()) {
            industries.put(normalizeText(industryZh));
        }
        payload.put("beneficiary_industries", industries);
        payload.put("summary", normalizeText(item == null ? "" : item.title));
        return payload.toString();
    }

    private String buildSignalDocContent(
            String ticker,
            String technicalStatus,
            String risk,
            String reasonText,
            Instant asOf
    ) {
        JSONObject payload = new JSONObject();
        payload.put("kind", "SIGNAL_REASON");
        payload.put("ticker", normalizeTicker(ticker));
        payload.put("technical_status", normalizeText(technicalStatus));
        payload.put("risk", normalizeText(risk));
        payload.put("reason_text", normalizeText(reasonText));
        if (asOf != null) {
            payload.put("as_of", asOf.toString());
        }
        return payload.toString();
    }

    private String buildReasonText(String technicalStatus, String risk, String technicalReasonsJson) {
        JSONObject reasonRoot = safeJson(technicalReasonsJson);
        List<String> tokens = new ArrayList<>();
        String status = normalizeText(technicalStatus);
        if (!status.isEmpty()) {
            tokens.add(status);
        }
        String riskText = normalizeText(risk);
        if (!riskText.isEmpty()) {
            tokens.add(riskText);
        }
        JSONArray filterReasons = reasonRoot.optJSONArray("filter_reasons");
        if (filterReasons != null) {
            for (int i = 0; i < filterReasons.length(); i++) {
                String entry = normalizeText(filterReasons.optString(i, ""));
                if (!entry.isEmpty()) {
                    tokens.add(entry);
                }
                if (tokens.size() >= 8) {
                    break;
                }
            }
        }
        JSONArray riskFlags = reasonRoot.optJSONArray("risk_flags");
        if (riskFlags != null && tokens.size() < 12) {
            for (int i = 0; i < riskFlags.length(); i++) {
                String entry = normalizeText(riskFlags.optString(i, ""));
                if (!entry.isEmpty()) {
                    tokens.add(entry);
                }
                if (tokens.size() >= 12) {
                    break;
                }
            }
        }
        return normalizeText(String.join(" ", tokens));
    }

    private String buildCaseLine(VectorSearchService.DocMatch match, JSONObject payload) {
        if (match == null) {
            return "";
        }
        String date = match.publishedAt == null ? "-" : LocalDate.ofInstant(match.publishedAt, marketZone).toString();
        String ticker = normalizeTicker(firstNonBlank(match.ticker, payload.optString("ticker", "")));
        String title = normalizeText(firstNonBlank(match.title, payload.optString("title", "")));
        if (title.length() > 90) {
            title = title.substring(0, 87) + "...";
        }
        String base = (date + " " + firstNonBlank(ticker, "-") + " " + title).trim();
        return normalizeText(base);
    }

    private List<String> topKeys(Map<String, Integer> counts, int limit) {
        if (counts == null || counts.isEmpty()) {
            return List.of();
        }
        List<Map.Entry<String, Integer>> entries = new ArrayList<>(counts.entrySet());
        entries.sort(Comparator.comparingInt((Map.Entry<String, Integer> e) -> e.getValue()).reversed());
        List<String> out = new ArrayList<>();
        for (Map.Entry<String, Integer> e : entries) {
            if (e.getKey() == null || e.getKey().trim().isEmpty()) {
                continue;
            }
            out.add(e.getKey().trim());
            if (out.size() >= limit) {
                break;
            }
        }
        return out;
    }

    private void increment(Map<String, Integer> map, String key) {
        if (map == null) {
            return;
        }
        String normalized = normalizeText(key);
        if (normalized.isEmpty()) {
            return;
        }
        map.put(normalized, map.getOrDefault(normalized, 0) + 1);
    }

    private JSONObject safeJson(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return new JSONObject();
        }
        try {
            return new JSONObject(raw);
        } catch (Exception ignored) {
            return new JSONObject();
        }
    }

    private String normalizeTicker(String ticker) {
        String value = safe(ticker);
        if (value.isEmpty()) {
            return "";
        }
        String lower = value.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".jp")) {
            return lower;
        }
        if (lower.endsWith(".t")) {
            return lower.substring(0, lower.length() - 2) + ".jp";
        }
        if (lower.matches("\\d{4,6}")) {
            return lower + ".jp";
        }
        return value.toUpperCase(Locale.ROOT);
    }

    private String normalizeText(String value) {
        String text = safe(value).replace("\r", " ").replace("\n", " ").trim();
        return text.replaceAll("\\s{2,}", " ");
    }

    private String trimToNull(String value) {
        String text = safe(value);
        return text.isEmpty() ? null : text;
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return "";
        }
        for (String value : values) {
            if (value == null) {
                continue;
            }
            String text = value.trim();
            if (!text.isEmpty()) {
                return text;
            }
        }
        return "";
    }

    private String joinError(String base, String suffix) {
        String left = safe(base);
        String right = safe(suffix);
        if (left.isEmpty()) {
            return right;
        }
        if (right.isEmpty()) {
            return left;
        }
        return left + "|" + right;
    }

    private String safe(String value) {
        return value == null ? "" : value.trim();
    }

    private static final class ReturnStats {
        int samples;
        int positive;
        double total;

        void add(double value) {
            if (!Double.isFinite(value)) {
                return;
            }
            samples++;
            total += value;
            if (value > 0.0) {
                positive++;
            }
        }

        double avg() {
            if (samples <= 0) {
                return Double.NaN;
            }
            return total / samples;
        }

        double winRatePct() {
            if (samples <= 0) {
                return Double.NaN;
            }
            return positive * 100.0 / samples;
        }
    }

    private static final class NewsSimilaritySummary {
        final int analyzedNews;
        final int matchedEvents;
        final ReturnStats stats1d;
        final ReturnStats stats3d;
        final ReturnStats stats5d;
        final List<String> impactedTickers;
        final List<String> beneficiaryIndustries;
        final List<String> representativeCases;
        final String error;

        private NewsSimilaritySummary(
                int analyzedNews,
                int matchedEvents,
                ReturnStats stats1d,
                ReturnStats stats3d,
                ReturnStats stats5d,
                List<String> impactedTickers,
                List<String> beneficiaryIndustries,
                List<String> representativeCases,
                String error
        ) {
            this.analyzedNews = Math.max(0, analyzedNews);
            this.matchedEvents = Math.max(0, matchedEvents);
            this.stats1d = stats1d == null ? new ReturnStats() : stats1d;
            this.stats3d = stats3d == null ? new ReturnStats() : stats3d;
            this.stats5d = stats5d == null ? new ReturnStats() : stats5d;
            this.impactedTickers = impactedTickers == null ? List.of() : List.copyOf(impactedTickers);
            this.beneficiaryIndustries = beneficiaryIndustries == null ? List.of() : List.copyOf(beneficiaryIndustries);
            this.representativeCases = representativeCases == null ? List.of() : List.copyOf(representativeCases);
            this.error = error == null ? "" : error;
        }

        static NewsSimilaritySummary empty() {
            return new NewsSimilaritySummary(
                    0,
                    0,
                    new ReturnStats(),
                    new ReturnStats(),
                    new ReturnStats(),
                    List.of(),
                    List.of(),
                    List.of(),
                    ""
            );
        }
    }

    private static final class SignalExplanationSummary {
        final String technicalStatus;
        final int similarCases;
        final List<String> cases;
        final String error;

        private SignalExplanationSummary(String technicalStatus, int similarCases, List<String> cases, String error) {
            this.technicalStatus = technicalStatus == null ? "" : technicalStatus;
            this.similarCases = Math.max(0, similarCases);
            this.cases = cases == null ? List.of() : List.copyOf(cases);
            this.error = error == null ? "" : error;
        }

        static SignalExplanationSummary empty() {
            return new SignalExplanationSummary("", 0, List.of(), "");
        }

        static SignalExplanationSummary withError(String error) {
            return new SignalExplanationSummary("", 0, List.of(), error);
        }
    }

    public static final class MemoryInsights {
        private final NewsSimilaritySummary news;
        private final SignalExplanationSummary signal;

        private MemoryInsights(NewsSimilaritySummary news, SignalExplanationSummary signal) {
            this.news = news == null ? NewsSimilaritySummary.empty() : news;
            this.signal = signal == null ? SignalExplanationSummary.empty() : signal;
        }

        public static MemoryInsights empty() {
            return new MemoryInsights(NewsSimilaritySummary.empty(), SignalExplanationSummary.empty());
        }

        public JSONObject toJson() {
            JSONObject root = new JSONObject();

            JSONObject newsSimilarity = new JSONObject();
            newsSimilarity.put("analyzed_news", news.analyzedNews);
            newsSimilarity.put("matched_events", news.matchedEvents);
            newsSimilarity.put("avg_return_1d_pct", round2(news.stats1d.avg()));
            newsSimilarity.put("avg_return_3d_pct", round2(news.stats3d.avg()));
            newsSimilarity.put("avg_return_5d_pct", round2(news.stats5d.avg()));
            newsSimilarity.put("win_rate_3d_pct", round2(news.stats3d.winRatePct()));
            newsSimilarity.put("sample_1d", news.stats1d.samples);
            newsSimilarity.put("sample_3d", news.stats3d.samples);
            newsSimilarity.put("sample_5d", news.stats5d.samples);
            newsSimilarity.put("beneficiary_industries", new JSONArray(news.beneficiaryIndustries));
            newsSimilarity.put("similar_cases", new JSONArray(news.representativeCases));
            newsSimilarity.put("error", news.error);
            root.put("news_similarity", newsSimilarity);

            JSONObject tickerImpact = new JSONObject();
            tickerImpact.put("impacted_tickers", new JSONArray(news.impactedTickers));
            tickerImpact.put("matched_events", news.matchedEvents);
            tickerImpact.put("error", news.error);
            root.put("ticker_impact", tickerImpact);

            JSONObject signalMemory = new JSONObject();
            signalMemory.put("technical_status", signal.technicalStatus);
            signalMemory.put("similar_cases", signal.similarCases);
            signalMemory.put("cases", new JSONArray(signal.cases));
            signalMemory.put("error", signal.error);
            root.put("signal_explanation_memory", signalMemory);

            return root;
        }

        public List<String> toDigestLines() {
            List<String> out = new ArrayList<>();
            if (news.analyzedNews > 0 || news.matchedEvents > 0) {
                out.add(String.format(
                        Locale.US,
                        "[NewsSimilarity] matched=%d, avg3d=%s%%, industries=%s",
                        news.matchedEvents,
                        formatRound2(news.stats3d.avg()),
                        news.beneficiaryIndustries.isEmpty()
                                ? "-"
                                : String.join("/", news.beneficiaryIndustries.subList(0, Math.min(3, news.beneficiaryIndustries.size())))
                ));
                out.add("[TickerImpact] " + (news.impactedTickers.isEmpty()
                        ? "none"
                        : String.join(", ", news.impactedTickers.subList(0, Math.min(6, news.impactedTickers.size())))));
            }
            if (!signal.technicalStatus.isEmpty() || signal.similarCases > 0) {
                out.add("[SignalMemory] status=" + (signal.technicalStatus.isEmpty() ? "-" : signal.technicalStatus)
                        + ", similar_cases=" + signal.similarCases);
            }
            return out;
        }

        private static Object round2(double value) {
            if (!Double.isFinite(value)) {
                return JSONObject.NULL;
            }
            return Math.round(value * 100.0) / 100.0;
        }

        private static String formatRound2(double value) {
            if (!Double.isFinite(value)) {
                return "-";
            }
            return String.format(Locale.US, "%.2f", value);
        }
    }
}
