package com.stockbot.data;

import com.stockbot.data.http.HttpClientEx;
import com.stockbot.model.DailyPrice;
import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 模块说明：IndustryService（class）。
 * 主要职责：承载 data 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public class IndustryService {
    private static final String UNKNOWN = "Unknown";

    private final HttpClientEx http;
    private final Map<String, String> fallbackIndustryMap = new HashMap<>();
    private final Map<String, String> fallbackCompanyNameMap = new HashMap<>();
    private final Map<String, String> industryZhMap = new HashMap<>();
    private final Map<String, Profile> profileCache = new ConcurrentHashMap<>();

    private volatile boolean quoteSummaryEnabled = true;

/**
 * 方法说明：IndustryService，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public IndustryService() {
        this(new HttpClientEx());
    }

/**
 * 方法说明：IndustryService，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public IndustryService(HttpClientEx http) {
        this.http = http;
        seedFallbacks();
        seedIndustryZh();
    }

/**
 * 方法说明：industryOf，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public String industryOf(String ticker) {
        String key = normalizeTicker(ticker);
        Profile profile = profileOf(key);
        if (!isBlank(profile.industry)) return profile.industry;

        String fallback = fallbackIndustryMap.get(key);
        return isBlank(fallback) ? UNKNOWN : fallback;
    }

/**
 * 方法说明：industryZhOf，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public String industryZhOf(String ticker) {
        return industryZh(industryOf(ticker));
    }

/**
 * 方法说明：companyNameOf，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public String companyNameOf(String ticker) {
        String key = normalizeTicker(ticker);
        Profile profile = profileOf(key);
        if (!isBlank(profile.displayName)) return profile.displayName;

        String fallback = fallbackCompanyNameMap.get(key);
        if (!isBlank(fallback)) return fallback;
        return key;
    }

/**
 * 方法说明：industryZh，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public String industryZh(String industryEn) {
        if (isBlank(industryEn)) return "未知";
        String zh = industryZhMap.get(industryEn.trim().toLowerCase(Locale.ROOT));
        if (!isBlank(zh)) return zh;
        return "未知";
    }

/**
 * 方法说明：displayNameOf，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public String displayNameOf(String ticker) {
        String key = normalizeTicker(ticker);
        String name = companyNameOf(key);
        String industryEn = industryOf(key);
        String industryZh = industryZh(industryEn);
        String nameAndTicker = key.equalsIgnoreCase(name) ? key : (name + " " + key);
        return nameAndTicker + " (" + industryZh + "/" + industryEn + ")";
    }

/**
 * 方法说明：scoreIndustryTrend，负责计算评分并输出分值。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public double scoreIndustryTrend(String ticker, String industry, List<DailyPrice> history, Double pctChange) {
        double score = scoreIndustryTrend(industry);

        Double trend20vs60 = trendFromHistory(history, 20, 60);
        if (trend20vs60 != null) score += trend20vs60 * 220.0;

        if (pctChange != null) score += pctChange * 1.2;

        return clamp(score, 0.0, 100.0);
    }

/**
 * 方法说明：scoreIndustryTrend，负责计算评分并输出分值。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public double scoreIndustryTrend(String industry) {
        if (isBlank(industry)) return 50.0;
        String text = industry.toLowerCase(Locale.ROOT);
        if (text.contains("semiconductor")) return 56.0;
        if (text.contains("gaming")) return 53.0;
        if (text.contains("financial")) return 52.0;
        if (text.contains("insurance")) return 51.0;
        if (text.contains("etf")) return 50.0;
        return 50.0;
    }

/**
 * 方法说明：profileOf，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private Profile profileOf(String ticker) {
        if (isBlank(ticker)) return Profile.EMPTY;
        return profileCache.computeIfAbsent(ticker, this::fetchProfileSafe);
    }

/**
 * 方法说明：fetchProfileSafe，负责拉取外部数据并做基础处理。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private Profile fetchProfileSafe(String ticker) {
        try {
            return fetchProfile(ticker);
        } catch (Exception ignored) {
            return Profile.EMPTY;
        }
    }

/**
 * 方法说明：fetchProfile，负责拉取外部数据并做基础处理。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private Profile fetchProfile(String ticker) {
        Profile profile = fetchFromChart(ticker);

        if (quoteSummaryEnabled) {
            try {
                Profile quoteProfile = fetchFromQuoteSummary(ticker);
                profile = merge(profile, quoteProfile);
            } catch (Exception e) {
                String msg = e.getMessage() == null ? "" : e.getMessage();
                if (msg.contains("HTTP 401")) quoteSummaryEnabled = false;
            }
        }

        if (isBlank(profile.industry)) {
            String fallbackIndustry = fallbackIndustryMap.get(ticker);
            if (!isBlank(fallbackIndustry)) profile = new Profile(profile.displayName, fallbackIndustry);
        }

        if (isBlank(profile.displayName)) {
            String fallbackName = fallbackCompanyNameMap.get(ticker);
            if (!isBlank(fallbackName)) profile = new Profile(fallbackName, profile.industry);
        }

        return profile;
    }

/**
 * 方法说明：fetchFromChart，负责拉取外部数据并做基础处理。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private Profile fetchFromChart(String ticker) {
        try {
            String encoded = URLEncoder.encode(ticker, StandardCharsets.UTF_8);
            String url = "https://query1.finance.yahoo.com/v8/finance/chart/" + encoded + "?range=5d&interval=1d";
            String body = http.getText(url, 30);

            JSONObject root = new JSONObject(body);
            JSONObject chart = root.optJSONObject("chart");
            JSONArray result = chart == null ? null : chart.optJSONArray("result");
            JSONObject r0 = (result == null || result.length() == 0) ? null : result.optJSONObject(0);
            JSONObject meta = r0 == null ? null : r0.optJSONObject("meta");
            if (meta == null) return Profile.EMPTY;

            String name = firstNonBlank(
                    meta.optString("longName", null),
                    meta.optString("shortName", null),
                    meta.optString("symbol", null)
            );

            String instrumentType = clean(meta.optString("instrumentType", null));
            String inferredIndustry = inferIndustry(name, instrumentType, ticker);
            return new Profile(clean(name), inferredIndustry);
        } catch (Exception ignored) {
            return Profile.EMPTY;
        }
    }

/**
 * 方法说明：fetchFromQuoteSummary，负责拉取外部数据并做基础处理。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private Profile fetchFromQuoteSummary(String ticker) throws Exception {
        String encoded = URLEncoder.encode(ticker, StandardCharsets.UTF_8);
        String quoteSummaryUrl = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/" + encoded
                + "?modules=price,assetProfile";
        String body = http.getText(quoteSummaryUrl, 30);

        JSONObject root = new JSONObject(body);
        JSONObject quoteSummary = root.optJSONObject("quoteSummary");
        JSONArray result = quoteSummary == null ? null : quoteSummary.optJSONArray("result");
        JSONObject r0 = (result == null || result.length() == 0) ? null : result.optJSONObject(0);
        if (r0 == null) return Profile.EMPTY;

        JSONObject price = r0.optJSONObject("price");
        JSONObject asset = r0.optJSONObject("assetProfile");

        String companyName = firstNonBlank(
                price == null ? null : price.optString("shortName", null),
                price == null ? null : price.optString("longName", null),
                price == null ? null : price.optString("displayName", null)
        );

        String industry = firstNonBlank(
                asset == null ? null : asset.optString("industry", null),
                asset == null ? null : asset.optString("sector", null)
        );

        return new Profile(clean(companyName), clean(industry));
    }

/**
 * 方法说明：merge，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static Profile merge(Profile base, Profile override) {
        if (base == null) base = Profile.EMPTY;
        if (override == null) override = Profile.EMPTY;

        String name = !isBlank(override.displayName) ? override.displayName : base.displayName;
        String industry = !isBlank(override.industry) ? override.industry : base.industry;
        return new Profile(name, industry);
    }

/**
 * 方法说明：inferIndustry，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static String inferIndustry(String name, String instrumentType, String ticker) {
        String t = normalizeTicker(ticker);
        String n = name == null ? "" : name.toLowerCase(Locale.ROOT);
        String i = instrumentType == null ? "" : instrumentType.toLowerCase(Locale.ROOT);

        if (i.contains("etf") || n.contains(" etf") || n.contains("index fund")) return "ETF";
        if (n.contains("semiconductor") || n.contains("electron")) return "Semiconductor";
        if (n.contains("insurance") || n.contains("sompo")) return "Insurance";
        if (n.contains("bank") || n.contains("financial") || n.contains("trust")) return "Financial Services";
        if (n.contains("electric") || n.contains("electronics")) return "Electrical Equipment";
        if (n.contains("telecom") || n.contains("telephone")) return "Telecom Services";
        if (n.contains("gaming") || n.contains("nintendo")) return "Gaming";
        if (n.contains("gold")) return "Precious Metals";
        if (n.contains("textile") || n.contains("boseki")) return "Textiles";
        if (n.contains("hitachi")) return "Industrials";

        if ("1540.T".equals(t)) return "ETF";
        return UNKNOWN;
    }

/**
 * 方法说明：seedFallbacks，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void seedFallbacks() {
        fallbackIndustryMap.put("7974.T", "Gaming");
        fallbackIndustryMap.put("8035.T", "Semiconductor");
        fallbackIndustryMap.put("8306.T", "Financial Services");
        fallbackIndustryMap.put("8630.T", "Insurance");
        fallbackIndustryMap.put("5801.T", "Electrical Equipment");
        fallbackIndustryMap.put("3110.T", "Textiles");
        fallbackIndustryMap.put("6501.T", "Industrials");
        fallbackIndustryMap.put("1540.T", "ETF");

        fallbackCompanyNameMap.put("7974.T", "Nintendo");
        fallbackCompanyNameMap.put("8035.T", "Tokyo Electron");
        fallbackCompanyNameMap.put("8306.T", "Mitsubishi UFJ Financial Group");
        fallbackCompanyNameMap.put("8630.T", "Sompo Holdings");
        fallbackCompanyNameMap.put("5801.T", "Furukawa Electric");
        fallbackCompanyNameMap.put("3110.T", "Nitto Boseki");
        fallbackCompanyNameMap.put("6501.T", "Hitachi");
        fallbackCompanyNameMap.put("1540.T", "Japan Physical Gold ETF");
    }

/**
 * 方法说明：seedIndustryZh，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void seedIndustryZh() {
        putIndustryZh("gaming", "游戏");
        putIndustryZh("semiconductor", "半导体");
        putIndustryZh("technology", "科技");
        putIndustryZh("software", "软件");
        putIndustryZh("hardware", "硬件");
        putIndustryZh("bank", "银行");
        putIndustryZh("insurance", "保险");
        putIndustryZh("financial services", "金融服务");
        putIndustryZh("asset management", "资产管理");
        putIndustryZh("electrical equipment", "电气设备");
        putIndustryZh("telecom services", "通信服务");
        putIndustryZh("textiles", "纺织");
        putIndustryZh("precious metals", "贵金属");
        putIndustryZh("consumer cyclical", "可选消费");
        putIndustryZh("consumer defensive", "必选消费");
        putIndustryZh("industrials", "工业");
        putIndustryZh("basic materials", "基础材料");
        putIndustryZh("energy", "能源");
        putIndustryZh("utilities", "公用事业");
        putIndustryZh("real estate", "房地产");
        putIndustryZh("communication services", "通信服务");
        putIndustryZh("healthcare", "医疗健康");
        putIndustryZh("biotechnology", "生物科技");
        putIndustryZh("transportation", "运输");
        putIndustryZh("etf", "交易型基金");
        putIndustryZh("unknown", "未知");
    }

/**
 * 方法说明：putIndustryZh，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void putIndustryZh(String en, String zh) {
        industryZhMap.put(en.trim().toLowerCase(Locale.ROOT), zh);
    }

/**
 * 方法说明：trendFromHistory，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static Double trendFromHistory(List<DailyPrice> history, int shortWindow, int longWindow) {
        if (history == null || history.size() < longWindow) return null;
        Double maShort = movingAverage(history, shortWindow);
        Double maLong = movingAverage(history, longWindow);
        if (maShort == null || maLong == null || maLong == 0.0) return null;
        return (maShort - maLong) / maLong;
    }

/**
 * 方法说明：movingAverage，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static Double movingAverage(List<DailyPrice> history, int window) {
        if (history == null || history.size() < window || window <= 0) return null;
        double sum = 0.0;
        for (int i = history.size() - window; i < history.size(); i++) {
            sum += history.get(i).close;
        }
        return sum / window;
    }

/**
 * 方法说明：clamp，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static double clamp(double value, double min, double max) {
        if (value < min) return min;
        if (value > max) return max;
        return value;
    }

/**
 * 方法说明：normalizeTicker，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static String normalizeTicker(String ticker) {
        if (ticker == null) return "";
        return ticker.trim().toUpperCase(Locale.ROOT);
    }

/**
 * 方法说明：clean，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static String clean(String s) {
        if (s == null) return null;
        String out = s.trim();
        return out.isEmpty() ? null : out;
    }

/**
 * 方法说明：isBlank，负责判断条件是否满足。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

/**
 * 方法说明：firstNonBlank，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static String firstNonBlank(String... values) {
        if (values == null) return null;
        for (String v : values) {
            String c = clean(v);
            if (c != null) return c;
        }
        return null;
    }

    private static class Profile {
        static final Profile EMPTY = new Profile(null, null);

        final String displayName;
        final String industry;

        Profile(String displayName, String industry) {
            this.displayName = displayName;
            this.industry = industry;
        }
    }
}
