package com.stockbot.jp.polymarket;

import com.stockbot.data.OllamaClient;
import com.stockbot.data.http.HttpClientEx;
import com.stockbot.jp.config.Config;
import com.stockbot.jp.model.WatchlistAnalysis;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.InputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 模块说明：PolymarketService（class）。
 * 主要职责：承载 polymarket 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class PolymarketService {
    private final Config config;
    private final HttpClientEx http;
    private final OllamaClient ollamaClient;

/**
 * 方法说明：PolymarketService，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public PolymarketService(Config config, HttpClientEx http, OllamaClient ollamaClient) {
        this.config = config;
        this.http = http;
        this.ollamaClient = ollamaClient;
    }

/**
 * 方法说明：collectSignals，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public PolymarketSignalReport collectSignals(List<WatchlistAnalysis> watchRows) {
        boolean enabled = config.getBoolean("polymarket.enabled", true);
        if (!enabled) {
            return PolymarketSignalReport.disabled("未启用（config: polymarket.enabled=false）");
        }

        List<String> keywords = config.getList("polymarket.keywords");
        if (keywords.isEmpty()) {
            return PolymarketSignalReport.empty("无匹配市场（polymarket.keywords 为空）");
        }

        int timeoutSec = Math.max(3, config.getInt("polymarket.timeout_sec", 12));
        int searchLimit = Math.max(5, config.getInt("polymarket.search_limit", 40));
        int topN = Math.max(1, config.getInt("polymarket.top_n", 3));
        int impactLimit = Math.max(1, config.getInt("polymarket.watch_impact_limit", 4));
        String gammaBase = config.getString("polymarket.gamma_base_url", "https://gamma-api.polymarket.com");
        String dataBase = config.getString("polymarket.data_base_url", "https://data-api.polymarket.com");

        List<TopicConfig> topicConfigs = loadTopicConfig();
        List<MarketCandidate> markets = new ArrayList<>();
        List<String> errors = new ArrayList<>();

        for (String keyword : keywords) {
            String kw = keyword == null ? "" : keyword.trim();
            if (kw.isEmpty()) {
                continue;
            }
            try {
                String encoded = URLEncoder.encode(kw, StandardCharsets.UTF_8);
                String url = gammaBase + "/markets?active=true&closed=false&limit=" + searchLimit + "&offset=0&search=" + encoded;
                String body = http.getText(url, timeoutSec);
                markets.addAll(parseMarkets(body, kw));
            } catch (Exception e) {
                errors.add(kw + ": " + safeError(e));
            }
        }

        if (markets.isEmpty()) {
            String message = errors.isEmpty() ? "无匹配市场" : ("接口失败：" + String.join(" | ", errors));
            return PolymarketSignalReport.empty(message);
        }

        Map<String, MarketCandidate> byId = new LinkedHashMap<>();
        for (MarketCandidate market : markets) {
            if (market == null) {
                continue;
            }
            String key = !market.conditionId.isEmpty() ? market.conditionId : market.title.toLowerCase(Locale.ROOT);
            MarketCandidate old = byId.get(key);
            if (old == null || market.rankScore() > old.rankScore()) {
                byId.put(key, market);
            }
        }

        List<MarketCandidate> ranked = new ArrayList<>(byId.values());
        ranked.sort(Comparator.comparingDouble(MarketCandidate::rankScore).reversed());
        if (ranked.size() > topN) {
            ranked = new ArrayList<>(ranked.subList(0, topN));
        }

        String impactMode = config.getString("polymarket.impact.mode", "rule").toLowerCase(Locale.ROOT);
        List<PolymarketTopicSignal> signals = new ArrayList<>();

        for (MarketCandidate market : ranked) {
            TopicConfig topic = classifyTopic(market, topicConfigs);
            OiSnapshot oi = fetchOpenInterest(dataBase, market.conditionId, timeoutSec);
            String oiDirection = directionFromDelta(Double.isFinite(oi.change24h) ? oi.change24h : market.oiChange24h);
            List<String> industries = topic == null ? List.of("Unknown") : topic.allIndustries();

            List<PolymarketWatchImpact> impacts;
            if ("llm".equals(impactMode)) {
                impacts = llmImpact(topic, market, watchRows, impactLimit);
                if (impacts.isEmpty()) {
                    impacts = ruleImpact(topic, market, watchRows, impactLimit);
                }
            } else {
                impacts = ruleImpact(topic, market, watchRows, impactLimit);
            }

            signals.add(new PolymarketTopicSignal(
                    topic == null ? "other" : topic.name,
                    market.impliedProbabilityPct,
                    market.change24hPct,
                    oiDirection,
                    industries,
                    impacts,
                    market.title
            ));
        }

        if (signals.isEmpty()) {
            return PolymarketSignalReport.empty("无匹配市场");
        }

        String status = errors.isEmpty() ? "数据正常" : ("部分接口失败：" + String.join(" | ", errors));
        return new PolymarketSignalReport(true, status, signals);
    }

/**
 * 方法说明：ruleImpact，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private List<PolymarketWatchImpact> ruleImpact(
            TopicConfig topic,
            MarketCandidate market,
            List<WatchlistAnalysis> watchRows,
            int limit
    ) {
        if (watchRows == null || watchRows.isEmpty()) {
            return List.of();
        }

        List<PolymarketWatchImpact> out = new ArrayList<>();
        Set<String> used = new LinkedHashSet<>();
        for (WatchlistAnalysis row : watchRows) {
            if (row == null) {
                continue;
            }
            String key = (safe(row.code) + "|" + safe(row.ticker)).toLowerCase(Locale.ROOT);
            if (used.contains(key)) {
                continue;
            }
            used.add(key);

            String text = (safe(row.displayName) + " " + safe(row.companyNameLocal) + " " + safe(row.industryEn) + " " + safe(row.industryZh))
                    .toLowerCase(Locale.ROOT);

            String impact = "neutral";
            double confidence = 0.25;
            String rationale = "行业关联度有限。";

            if (topic != null) {
                if (matchAny(text, topic.benefitIndustries) || matchAny(text, topic.benefitWatchKeywords)) {
                    impact = "positive";
                    confidence = 0.6;
                    rationale = "主题与公司行业/业务关键词正相关。";
                } else if (matchAny(text, topic.hurtIndustries) || matchAny(text, topic.hurtWatchKeywords)) {
                    impact = "negative";
                    confidence = 0.6;
                    rationale = "主题可能对公司行业形成压力。";
                }
            }

            if ("neutral".equals(impact)) {
                continue;
            }

            String code = safe(row.code).isEmpty() ? safe(row.ticker).toUpperCase(Locale.ROOT) : safe(row.code).toUpperCase(Locale.ROOT);
            out.add(new PolymarketWatchImpact(code, impact, confidence, rationale));
            if (out.size() >= limit) {
                break;
            }
        }
        return out;
    }

/**
 * 方法说明：llmImpact，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private List<PolymarketWatchImpact> llmImpact(
            TopicConfig topic,
            MarketCandidate market,
            List<WatchlistAnalysis> watchRows,
            int limit
    ) {
        if (ollamaClient == null || watchRows == null || watchRows.isEmpty()) {
            return List.of();
        }
        try {
            JSONArray watchArr = new JSONArray();
            for (WatchlistAnalysis row : watchRows) {
                JSONObject item = new JSONObject();
                item.put("code", safe(row.code).isEmpty() ? safe(row.ticker).toUpperCase(Locale.ROOT) : safe(row.code).toUpperCase(Locale.ROOT));
                item.put("name", safe(row.companyNameLocal).isEmpty() ? safe(row.displayName) : safe(row.companyNameLocal));
                item.put("industry", safe(row.industryEn));
                item.put("keywords", safe(row.displayName));
                watchArr.put(item);
                if (watchArr.length() >= Math.max(3, limit * 2)) {
                    break;
                }
            }

            JSONObject req = new JSONObject();
            req.put("event", market.title);
            req.put("topic", topic == null ? "other" : topic.name);
            req.put("watchlist", watchArr);

            String prompt = "Return JSON array only. Each element fields: code, impact(positive|negative|neutral), confidence(0~1), rationale. Input: " + req;
            String response = ollamaClient.summarize(prompt);
            JSONArray arr = tryParseArray(response);
            if (arr == null) {
                return List.of();
            }

            List<PolymarketWatchImpact> out = new ArrayList<>();
            for (int i = 0; i < arr.length(); i++) {
                JSONObject item = arr.optJSONObject(i);
                if (item == null) {
                    continue;
                }
                String impact = safe(item.optString("impact", "neutral")).toLowerCase(Locale.ROOT);
                if (!"positive".equals(impact) && !"negative".equals(impact) && !"neutral".equals(impact)) {
                    impact = "neutral";
                }
                if ("neutral".equals(impact)) {
                    continue;
                }
                out.add(new PolymarketWatchImpact(
                        safe(item.optString("code", "")).toUpperCase(Locale.ROOT),
                        impact,
                        item.optDouble("confidence", 0.3),
                        safe(item.optString("rationale", "模型未给出理由"))
                ));
                if (out.size() >= limit) {
                    break;
                }
            }
            return out;
        } catch (Exception ignored) {
            return List.of();
        }
    }

/**
 * 方法说明：tryParseArray，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private JSONArray tryParseArray(String text) {
        if (text == null || text.trim().isEmpty()) {
            return null;
        }
        String raw = text.trim();
        try {
            return new JSONArray(raw);
        } catch (Exception ignored) {
            int from = raw.indexOf('[');
            int to = raw.lastIndexOf(']');
            if (from >= 0 && to > from) {
                try {
                    return new JSONArray(raw.substring(from, to + 1));
                } catch (Exception ignored2) {
                    return null;
                }
            }
            return null;
        }
    }

/**
 * 方法说明：fetchOpenInterest，负责拉取外部数据并做基础处理。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private OiSnapshot fetchOpenInterest(String dataBase, String conditionId, int timeoutSec) {
        if (conditionId == null || conditionId.trim().isEmpty()) {
            return OiSnapshot.empty();
        }
        String encoded = URLEncoder.encode(conditionId.trim(), StandardCharsets.UTF_8);
        String[] urls = new String[] {
                dataBase + "/markets/" + encoded,
                dataBase + "/markets?condition_id=" + encoded + "&limit=1"
        };
        for (String url : urls) {
            try {
                String body = http.getText(url, timeoutSec);
                OiSnapshot parsed = parseOi(body);
                if (parsed.hasValue()) {
                    return parsed;
                }
            } catch (Exception ignored) {
                // best effort.
            }
        }
        return OiSnapshot.empty();
    }

/**
 * 方法说明：parseOi，负责解析输入内容并转换结构。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private OiSnapshot parseOi(String body) {
        if (body == null || body.trim().isEmpty()) {
            return OiSnapshot.empty();
        }
        try {
            String raw = body.trim();
            if (raw.startsWith("[")) {
                JSONArray arr = new JSONArray(raw);
                if (arr.length() == 0) {
                    return OiSnapshot.empty();
                }
                JSONObject item = arr.optJSONObject(0);
                return item == null ? OiSnapshot.empty() : parseOiObject(item);
            }
            JSONObject root = new JSONObject(raw);
            if (root.has("data") && root.opt("data") instanceof JSONObject) {
                return parseOiObject(root.optJSONObject("data"));
            }
            if (root.has("data") && root.opt("data") instanceof JSONArray) {
                JSONArray data = root.optJSONArray("data");
                if (data != null && data.length() > 0 && data.optJSONObject(0) != null) {
                    return parseOiObject(data.optJSONObject(0));
                }
            }
            return parseOiObject(root);
        } catch (Exception ignored) {
            return OiSnapshot.empty();
        }
    }

/**
 * 方法说明：parseOiObject，负责解析输入内容并转换结构。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private OiSnapshot parseOiObject(JSONObject obj) {
        if (obj == null) {
            return OiSnapshot.empty();
        }
        double oi = firstFinite(
                obj.optDouble("openInterest", Double.NaN),
                obj.optDouble("open_interest", Double.NaN),
                obj.optDouble("oi", Double.NaN)
        );
        double oiChange = firstFinite(
                obj.optDouble("openInterestChange24h", Double.NaN),
                obj.optDouble("open_interest_change_24h", Double.NaN),
                obj.optDouble("oiChange24h", Double.NaN)
        );
        return new OiSnapshot(oi, oiChange);
    }

/**
 * 方法说明：parseMarkets，负责解析输入内容并转换结构。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private List<MarketCandidate> parseMarkets(String body, String keyword) {
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
                if (root.has("markets") && root.opt("markets") instanceof JSONArray) {
                    arr = root.optJSONArray("markets");
                } else if (root.has("data") && root.opt("data") instanceof JSONArray) {
                    arr = root.optJSONArray("data");
                }
            }
        } catch (Exception ignored) {
            return List.of();
        }
        if (arr == null || arr.length() == 0) {
            return List.of();
        }

        List<MarketCandidate> out = new ArrayList<>();
        for (int i = 0; i < arr.length(); i++) {
            JSONObject item = arr.optJSONObject(i);
            if (item == null) {
                continue;
            }
            String title = firstNonEmpty(
                    item.optString("question", ""),
                    item.optString("title", ""),
                    item.optString("name", "")
            );
            if (title.isEmpty()) {
                continue;
            }
            String conditionId = firstNonEmpty(
                    item.optString("conditionId", ""),
                    item.optString("condition_id", ""),
                    item.optString("id", "")
            );
            double prob = parseProbability(item);
            double change24h = parseChange24h(item);
            double volume = firstFinite(
                    item.optDouble("volume", Double.NaN),
                    item.optDouble("volume24hr", Double.NaN),
                    item.optDouble("volume24h", Double.NaN),
                    item.optDouble("liquidity", Double.NaN)
            );
            double oiChange = firstFinite(
                    item.optDouble("openInterestChange24h", Double.NaN),
                    item.optDouble("oiChange24h", Double.NaN)
            );
            List<String> tags = parseTags(item);

            out.add(new MarketCandidate(
                    conditionId,
                    title,
                    prob,
                    change24h,
                    volume,
                    oiChange,
                    tags,
                    keyword
            ));
        }
        return out;
    }

/**
 * 方法说明：parseTags，负责解析输入内容并转换结构。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private List<String> parseTags(JSONObject item) {
        JSONArray tagArr = item.optJSONArray("tags");
        if (tagArr == null || tagArr.length() == 0) {
            return List.of();
        }
        List<String> tags = new ArrayList<>();
        for (int i = 0; i < tagArr.length(); i++) {
            Object raw = tagArr.opt(i);
            if (raw instanceof JSONObject) {
                JSONObject obj = (JSONObject) raw;
                String tag = firstNonEmpty(obj.optString("slug", ""), obj.optString("name", ""));
                if (!tag.isEmpty()) {
                    tags.add(tag);
                }
            } else {
                String tag = safe(String.valueOf(raw));
                if (!tag.isEmpty()) {
                    tags.add(tag);
                }
            }
        }
        return tags;
    }

/**
 * 方法说明：parseProbability，负责解析输入内容并转换结构。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double parseProbability(JSONObject item) {
        double prob = firstFinite(
                item.optDouble("probability", Double.NaN),
                item.optDouble("probabilityYes", Double.NaN),
                item.optDouble("yesPrice", Double.NaN),
                item.optDouble("lastTradePrice", Double.NaN),
                item.optDouble("price", Double.NaN)
        );

        if (!Double.isFinite(prob)) {
            JSONArray prices = item.optJSONArray("outcomePrices");
            if (prices != null && prices.length() > 0) {
                prob = prices.optDouble(0, Double.NaN);
            }
        }

        if (!Double.isFinite(prob)) {
            return Double.NaN;
        }
        if (prob <= 1.0) {
            return prob * 100.0;
        }
        return prob;
    }

/**
 * 方法说明：parseChange24h，负责解析输入内容并转换结构。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double parseChange24h(JSONObject item) {
        double change = firstFinite(
                item.optDouble("priceChange24h", Double.NaN),
                item.optDouble("change24h", Double.NaN),
                item.optDouble("oneDayPriceChange", Double.NaN),
                item.optDouble("probabilityChange24h", Double.NaN)
        );
        if (!Double.isFinite(change)) {
            return 0.0;
        }
        if (Math.abs(change) <= 1.0) {
            return change * 100.0;
        }
        return change;
    }

/**
 * 方法说明：classifyTopic，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private TopicConfig classifyTopic(MarketCandidate market, List<TopicConfig> topics) {
        if (topics == null || topics.isEmpty()) {
            return null;
        }
        String text = (safe(market.title) + " " + String.join(" ", market.tags) + " " + safe(market.keyword))
                .toLowerCase(Locale.ROOT);
        TopicConfig best = null;
        int bestHit = 0;
        for (TopicConfig topic : topics) {
            int hit = 0;
            for (String keyword : topic.keywords) {
                if (!keyword.isEmpty() && text.contains(keyword)) {
                    hit++;
                }
            }
            if (hit > bestHit) {
                bestHit = hit;
                best = topic;
            }
        }
        return best;
    }

/**
 * 方法说明：loadTopicConfig，负责加载配置或数据。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private List<TopicConfig> loadTopicConfig() {
        String override = safe(config.getString("polymarket.topic_map_path", ""));
        try {
            String raw;
            if (!override.isEmpty()) {
                java.nio.file.Path path = config.workingDir().resolve(override).normalize();
                raw = java.nio.file.Files.readString(path, StandardCharsets.UTF_8);
            } else {
                InputStream in = PolymarketService.class.getClassLoader()
                        .getResourceAsStream("polymarket/topic_industry_map.json");
                if (in == null) {
                    return List.of();
                }
                raw = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }

            JSONObject root = new JSONObject(raw);
            JSONArray arr = root.optJSONArray("topics");
            if (arr == null || arr.length() == 0) {
                return List.of();
            }
            List<TopicConfig> out = new ArrayList<>();
            for (int i = 0; i < arr.length(); i++) {
                JSONObject item = arr.optJSONObject(i);
                if (item == null) {
                    continue;
                }
                String name = safe(item.optString("topic", ""));
                if (name.isEmpty()) {
                    continue;
                }
                out.add(new TopicConfig(
                        name,
                        toLowerList(item.optJSONArray("keywords")),
                        toLowerList(item.optJSONArray("benefit_industries")),
                        toLowerList(item.optJSONArray("hurt_industries")),
                        toLowerList(item.optJSONArray("benefit_watch_keywords")),
                        toLowerList(item.optJSONArray("hurt_watch_keywords"))
                ));
            }
            return out;
        } catch (Exception ignored) {
            return List.of();
        }
    }

/**
 * 方法说明：toLowerList，负责转换数据结构用于后续处理。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private List<String> toLowerList(JSONArray arr) {
        if (arr == null || arr.length() == 0) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (int i = 0; i < arr.length(); i++) {
            String s = safe(arr.optString(i, "")).toLowerCase(Locale.ROOT);
            if (!s.isEmpty()) {
                out.add(s);
            }
        }
        return out;
    }

/**
 * 方法说明：directionFromDelta，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String directionFromDelta(double change) {
        if (!Double.isFinite(change)) {
            return "-";
        }
        if (change > 0.2) {
            return "UP";
        }
        if (change < -0.2) {
            return "DOWN";
        }
        return "FLAT";
    }

/**
 * 方法说明：matchAny，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private boolean matchAny(String text, List<String> keywords) {
        if (text == null || text.isEmpty() || keywords == null || keywords.isEmpty()) {
            return false;
        }
        for (String keyword : keywords) {
            String kw = keyword == null ? "" : keyword.trim().toLowerCase(Locale.ROOT);
            if (!kw.isEmpty() && text.contains(kw)) {
                return true;
            }
        }
        return false;
    }

/**
 * 方法说明：safe，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String safe(String text) {
        return text == null ? "" : text.trim();
    }

/**
 * 方法说明：safeError，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String safeError(Exception e) {
        String msg = e == null ? "" : e.getMessage();
        if (msg == null || msg.trim().isEmpty()) {
            return e == null ? "unknown" : e.getClass().getSimpleName();
        }
        return msg.trim();
    }

/**
 * 方法说明：firstNonEmpty，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String firstNonEmpty(String... values) {
        if (values == null) {
            return "";
        }
        for (String v : values) {
            String t = safe(v);
            if (!t.isEmpty()) {
                return t;
            }
        }
        return "";
    }

/**
 * 方法说明：firstFinite，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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

    private static final class MarketCandidate {
        final String conditionId;
        final String title;
        final double impliedProbabilityPct;
        final double change24hPct;
        final double volume;
        final double oiChange24h;
        final List<String> tags;
        final String keyword;

        private MarketCandidate(
                String conditionId,
                String title,
                double impliedProbabilityPct,
                double change24hPct,
                double volume,
                double oiChange24h,
                List<String> tags,
                String keyword
        ) {
            this.conditionId = conditionId == null ? "" : conditionId;
            this.title = title == null ? "" : title;
            this.impliedProbabilityPct = impliedProbabilityPct;
            this.change24hPct = change24hPct;
            this.volume = Double.isFinite(volume) ? Math.max(0.0, volume) : 0.0;
            this.oiChange24h = oiChange24h;
            this.tags = tags == null ? List.of() : List.copyOf(tags);
            this.keyword = keyword == null ? "" : keyword;
        }

/**
 * 方法说明：rankScore，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
        private double rankScore() {
            double volPart = Math.log10(1.0 + volume);
            double changePart = Math.abs(change24hPct) / 10.0;
            return volPart + changePart;
        }
    }

    private static final class TopicConfig {
        final String name;
        final List<String> keywords;
        final List<String> benefitIndustries;
        final List<String> hurtIndustries;
        final List<String> benefitWatchKeywords;
        final List<String> hurtWatchKeywords;

        private TopicConfig(
                String name,
                List<String> keywords,
                List<String> benefitIndustries,
                List<String> hurtIndustries,
                List<String> benefitWatchKeywords,
                List<String> hurtWatchKeywords
        ) {
            this.name = name == null ? "" : name;
            this.keywords = keywords == null ? List.of() : List.copyOf(keywords);
            this.benefitIndustries = benefitIndustries == null ? List.of() : List.copyOf(benefitIndustries);
            this.hurtIndustries = hurtIndustries == null ? List.of() : List.copyOf(hurtIndustries);
            this.benefitWatchKeywords = benefitWatchKeywords == null ? List.of() : List.copyOf(benefitWatchKeywords);
            this.hurtWatchKeywords = hurtWatchKeywords == null ? List.of() : List.copyOf(hurtWatchKeywords);
        }

/**
 * 方法说明：allIndustries，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
        private List<String> allIndustries() {
            LinkedHashSet<String> merged = new LinkedHashSet<>();
            for (String item : benefitIndustries) {
                if (item != null && !item.trim().isEmpty()) {
                    merged.add(item);
                }
            }
            for (String item : hurtIndustries) {
                if (item != null && !item.trim().isEmpty()) {
                    merged.add(item);
                }
            }
            if (merged.isEmpty()) {
                return List.of("Unknown");
            }
            return new ArrayList<>(merged);
        }
    }

    private static final class OiSnapshot {
        final double value;
        final double change24h;

        private OiSnapshot(double value, double change24h) {
            this.value = value;
            this.change24h = change24h;
        }

/**
 * 方法说明：hasValue，负责判断条件是否满足。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
        private boolean hasValue() {
            return Double.isFinite(value);
        }

/**
 * 方法说明：empty，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
        private static OiSnapshot empty() {
            return new OiSnapshot(Double.NaN, Double.NaN);
        }
    }
}
