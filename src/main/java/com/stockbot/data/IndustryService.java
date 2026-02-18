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

public class IndustryService {
    private static final String UNKNOWN = "Unknown";

    private final HttpClientEx http;
    private final Map<String, String> fallbackIndustryMap = new HashMap<>();
    private final Map<String, String> fallbackCompanyNameMap = new HashMap<>();
    private final Map<String, String> industryZhMap = new HashMap<>();
    private final Map<String, Profile> profileCache = new ConcurrentHashMap<>();

    private volatile boolean quoteSummaryEnabled = true;

    public IndustryService() {
        this(new HttpClientEx());
    }

    public IndustryService(HttpClientEx http) {
        this.http = http;
        seedFallbacks();
        seedIndustryZh();
    }

    public String industryOf(String ticker) {
        String key = normalizeTicker(ticker);
        Profile profile = profileOf(key);
        if (!isBlank(profile.industry)) return profile.industry;

        String fallback = fallbackIndustryMap.get(key);
        return isBlank(fallback) ? UNKNOWN : fallback;
    }

    public String industryZhOf(String ticker) {
        return industryZh(industryOf(ticker));
    }

    public String companyNameOf(String ticker) {
        String key = normalizeTicker(ticker);
        Profile profile = profileOf(key);
        if (!isBlank(profile.displayName)) return profile.displayName;

        String fallback = fallbackCompanyNameMap.get(key);
        if (!isBlank(fallback)) return fallback;
        return key;
    }

    public String industryZh(String industryEn) {
        if (isBlank(industryEn)) return "未知";
        String zh = industryZhMap.get(industryEn.trim().toLowerCase(Locale.ROOT));
        if (!isBlank(zh)) return zh;
        return "未知";
    }

    public String displayNameOf(String ticker) {
        String key = normalizeTicker(ticker);
        String name = companyNameOf(key);
        String industryEn = industryOf(key);
        String industryZh = industryZh(industryEn);
        String nameAndTicker = key.equalsIgnoreCase(name) ? key : (name + " " + key);
        return nameAndTicker + " (" + industryZh + "/" + industryEn + ")";
    }

    public double scoreIndustryTrend(String ticker, String industry, List<DailyPrice> history, Double pctChange) {
        double score = scoreIndustryTrend(industry);

        Double trend20vs60 = trendFromHistory(history, 20, 60);
        if (trend20vs60 != null) score += trend20vs60 * 220.0;

        if (pctChange != null) score += pctChange * 1.2;

        return clamp(score, 0.0, 100.0);
    }

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

    private Profile profileOf(String ticker) {
        if (isBlank(ticker)) return Profile.EMPTY;
        return profileCache.computeIfAbsent(ticker, this::fetchProfileSafe);
    }

    private Profile fetchProfileSafe(String ticker) {
        try {
            return fetchProfile(ticker);
        } catch (Exception ignored) {
            return Profile.EMPTY;
        }
    }

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

    private static Profile merge(Profile base, Profile override) {
        if (base == null) base = Profile.EMPTY;
        if (override == null) override = Profile.EMPTY;

        String name = !isBlank(override.displayName) ? override.displayName : base.displayName;
        String industry = !isBlank(override.industry) ? override.industry : base.industry;
        return new Profile(name, industry);
    }

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

    private void putIndustryZh(String en, String zh) {
        industryZhMap.put(en.trim().toLowerCase(Locale.ROOT), zh);
    }

    private static Double trendFromHistory(List<DailyPrice> history, int shortWindow, int longWindow) {
        if (history == null || history.size() < longWindow) return null;
        Double maShort = movingAverage(history, shortWindow);
        Double maLong = movingAverage(history, longWindow);
        if (maShort == null || maLong == null || maLong == 0.0) return null;
        return (maShort - maLong) / maLong;
    }

    private static Double movingAverage(List<DailyPrice> history, int window) {
        if (history == null || history.size() < window || window <= 0) return null;
        double sum = 0.0;
        for (int i = history.size() - window; i < history.size(); i++) {
            sum += history.get(i).close;
        }
        return sum / window;
    }

    private static double clamp(double value, double min, double max) {
        if (value < min) return min;
        if (value > max) return max;
        return value;
    }

    private static String normalizeTicker(String ticker) {
        if (ticker == null) return "";
        return ticker.trim().toUpperCase(Locale.ROOT);
    }

    private static String clean(String s) {
        if (s == null) return null;
        String out = s.trim();
        return out.isEmpty() ? null : out;
    }

    private static boolean isBlank(String s) {
        return s == null || s.trim().isEmpty();
    }

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
