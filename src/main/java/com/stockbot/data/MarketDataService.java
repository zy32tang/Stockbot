package com.stockbot.data;

import com.stockbot.data.http.HttpClientEx;
import com.stockbot.model.DailyPrice;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 模块说明：MarketDataService（class）。
 * 主要职责：承载 data 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public class MarketDataService {
    private final HttpClientEx http;

/**
 * 方法说明：MarketDataService，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public MarketDataService(HttpClientEx http) {
        this.http = http;
    }

/**
 * 方法说明：fetchDailyHistoryBars，负责拉取外部数据并做基础处理。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public List<DailyBar> fetchDailyHistoryBars(String ticker, String range, String interval) {
        List<DailyBar> out = new ArrayList<>();
        try {
            String url = "https://query1.finance.yahoo.com/v8/finance/chart/" + ticker
                    + "?range=" + range + "&interval=" + interval;
            String body = http.getText(url, 30);

            JSONObject root = new JSONObject(body);
            JSONObject chart = root.optJSONObject("chart");
            if (chart == null) return out;
            JSONArray result = chart.optJSONArray("result");
            if (result == null || result.length() == 0) return out;
            JSONObject r0 = result.optJSONObject(0);
            if (r0 == null) return out;

            JSONArray timestamps = r0.optJSONArray("timestamp");
            JSONObject indicators = r0.optJSONObject("indicators");
            JSONArray quoteArr = indicators == null ? null : indicators.optJSONArray("quote");
            JSONObject quote0 = (quoteArr == null || quoteArr.length() == 0) ? null : quoteArr.optJSONObject(0);
            if (timestamps == null || quote0 == null) return out;

            JSONArray opens = quote0.optJSONArray("open");
            JSONArray highs = quote0.optJSONArray("high");
            JSONArray lows = quote0.optJSONArray("low");
            JSONArray closes = quote0.optJSONArray("close");
            JSONArray volumes = quote0.optJSONArray("volume");
            if (closes == null) return out;

            int n = Math.min(timestamps.length(), closes.length());
            for (int i = 0; i < n; i++) {
                if (closes.isNull(i)) continue;
                long epoch = timestamps.optLong(i, 0L);
                double close = closes.optDouble(i, Double.NaN);
                if (epoch <= 0 || !Double.isFinite(close) || close <= 0.0) continue;

                double open = valueOrFallback(opens, i, close);
                double high = valueOrFallback(highs, i, Math.max(open, close));
                double low = valueOrFallback(lows, i, Math.min(open, close));
                if (high < Math.max(open, close)) high = Math.max(open, close);
                if (low > Math.min(open, close)) low = Math.min(open, close);

                double volume = valueOrFallback(volumes, i, 0.0);
                if (!Double.isFinite(volume) || volume < 0.0) {
                    volume = 0.0;
                }

                LocalDate d = Instant.ofEpochSecond(epoch).atZone(ZoneOffset.UTC).toLocalDate();
                out.add(new DailyBar(d, open, high, low, close, volume));
            }
            out.sort(Comparator.comparing(dp -> dp.date));
        } catch (Exception ignored) {}
        return out;
    }

/**
 * 方法说明：fetchDailyHistory，负责拉取外部数据并做基础处理。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public List<DailyPrice> fetchDailyHistory(String ticker, String range, String interval) {
        List<DailyPrice> out = new ArrayList<>();
        List<DailyBar> bars = fetchDailyHistoryBars(ticker, range, interval);
        for (DailyBar bar : bars) {
            if (bar == null || bar.date == null || !Double.isFinite(bar.close)) {
                continue;
            }
            out.add(new DailyPrice(bar.date, bar.close));
        }
        return out;
    }

    private double valueOrFallback(JSONArray arr, int index, double fallback) {
        if (arr == null || index < 0 || index >= arr.length() || arr.isNull(index)) {
            return fallback;
        }
        double value = arr.optDouble(index, Double.NaN);
        if (!Double.isFinite(value)) {
            return fallback;
        }
        return value;
    }

/**
 * 方法说明：fetchLastTwoCloses，负责拉取外部数据并做基础处理。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public PricePair fetchLastTwoCloses(String ticker) {
        return lastTwoFromHistory(fetchDailyHistory(ticker, "5d", "1d"));
    }

/**
 * 方法说明：lastTwoFromHistory，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static PricePair lastTwoFromHistory(List<DailyPrice> history) {
        if (history == null || history.isEmpty()) return new PricePair(null, null);
        Double prev = null;
        Double last = null;
        for (DailyPrice dp : history) {
            prev = last;
            last = dp.close;
        }
        return new PricePair(last, prev);
    }

    public static class PricePair {
        public final Double last;
        public final Double prev;
        public PricePair(Double last, Double prev) { this.last = last; this.prev = prev; }
    }

    public static class DailyBar {
        public final LocalDate date;
        public final double open;
        public final double high;
        public final double low;
        public final double close;
        public final double volume;

        public DailyBar(LocalDate date, double open, double high, double low, double close, double volume) {
            this.date = date;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = volume;
        }
    }
}
