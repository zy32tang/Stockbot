package com.stockbot.data;

import com.stockbot.data.http.HttpClientEx;
import com.stockbot.model.DailyPrice;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FundamentalsService {
    private final MarketDataService market;
    private final Map<String, Double> scoreCache = new ConcurrentHashMap<>();

    public FundamentalsService() {
        this(new HttpClientEx());
    }

    public FundamentalsService(HttpClientEx http) {
        this.market = new MarketDataService(http);
    }

    public double scoreFundamentals(String ticker) {
        String key = normalizeTicker(ticker);
        if (key.isEmpty()) return 50.0;
        return scoreCache.computeIfAbsent(key, this::computeScoreSafe);
    }

    private double computeScoreSafe(String ticker) {
        try {
            return computeScore(ticker);
        } catch (Exception ignored) {
            return 50.0;
        }
    }

    private double computeScore(String ticker) {
        List<DailyPrice> history = market.fetchDailyHistory(ticker, "1y", "1d");
        if (history == null || history.size() < 40) return 50.0;

        double last = history.get(history.size() - 1).close;
        Double ma60 = movingAverage(history, 60);
        Double ma200 = movingAverage(history, 200);
        Double ret6m = returnOver(history, 126);
        Double drawdown1y = maxDrawdown(history);
        Double vol60 = annualizedVolatility(history, 60);

        double score = 50.0;

        if (ma60 != null) score += (last >= ma60 ? 8.0 : -6.0);
        if (ma200 != null) score += (last >= ma200 ? 10.0 : -8.0);
        if (ma60 != null && ma200 != null) score += (ma60 >= ma200 ? 6.0 : -6.0);

        if (ret6m != null) {
            double pct = ret6m * 100.0;
            score += clamp(pct * 0.45, -14.0, 14.0);
        }

        if (drawdown1y != null) {
            double ddPct = drawdown1y * 100.0;
            if (ddPct >= -12.0) score += 6.0;
            else if (ddPct >= -20.0) score += 2.0;
            else if (ddPct >= -30.0) score -= 4.0;
            else score -= 10.0;
        }

        if (vol60 != null) {
            if (vol60 <= 18.0) score += 6.0;
            else if (vol60 <= 28.0) score += 2.0;
            else if (vol60 <= 40.0) score -= 3.0;
            else score -= 8.0;
        }

        return clamp(score, 0.0, 100.0);
    }

    private static Double movingAverage(List<DailyPrice> history, int window) {
        if (history == null || window <= 0 || history.size() < window) return null;
        double sum = 0.0;
        for (int i = history.size() - window; i < history.size(); i++) {
            sum += history.get(i).close;
        }
        return sum / window;
    }

    private static Double returnOver(List<DailyPrice> history, int days) {
        if (history == null || days <= 0 || history.size() <= days) return null;
        double last = history.get(history.size() - 1).close;
        double prev = history.get(history.size() - 1 - days).close;
        if (!Double.isFinite(last) || !Double.isFinite(prev) || prev == 0.0) return null;
        return (last - prev) / prev;
    }

    private static Double maxDrawdown(List<DailyPrice> history) {
        if (history == null || history.isEmpty()) return null;

        double peak = history.get(0).close;
        double minDrawdown = 0.0;

        for (DailyPrice dp : history) {
            double close = dp.close;
            if (close > peak) peak = close;
            if (peak <= 0.0) continue;

            double drawdown = (close - peak) / peak;
            if (drawdown < minDrawdown) minDrawdown = drawdown;
        }
        return minDrawdown;
    }

    private static Double annualizedVolatility(List<DailyPrice> history, int lookbackDays) {
        if (history == null || history.size() < 3) return null;

        int start = Math.max(1, history.size() - lookbackDays);
        int count = 0;
        double mean = 0.0;
        double m2 = 0.0;

        for (int i = start; i < history.size(); i++) {
            double prev = history.get(i - 1).close;
            double curr = history.get(i).close;
            if (prev <= 0.0 || curr <= 0.0) continue;

            double ret = (curr - prev) / prev;
            count++;

            double delta = ret - mean;
            mean += delta / count;
            double delta2 = ret - mean;
            m2 += delta * delta2;
        }

        if (count < 2) return null;
        double variance = m2 / (count - 1);
        if (variance < 0.0) return null;

        return Math.sqrt(variance) * Math.sqrt(252.0) * 100.0;
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
}
