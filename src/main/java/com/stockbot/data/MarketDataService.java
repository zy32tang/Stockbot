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

public class MarketDataService {
    private final HttpClientEx http;

    public MarketDataService(HttpClientEx http) {
        this.http = http;
    }

    public List<DailyPrice> fetchDailyHistory(String ticker, String range, String interval) {
        List<DailyPrice> out = new ArrayList<>();
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
            JSONArray closes = quote0 == null ? null : quote0.optJSONArray("close");
            if (timestamps == null || closes == null) return out;

            int n = Math.min(timestamps.length(), closes.length());
            for (int i = 0; i < n; i++) {
                if (closes.isNull(i)) continue;
                long epoch = timestamps.optLong(i, 0L);
                double close = closes.optDouble(i, Double.NaN);
                if (epoch <= 0 || !Double.isFinite(close)) continue;
                LocalDate d = Instant.ofEpochSecond(epoch).atZone(ZoneOffset.UTC).toLocalDate();
                out.add(new DailyPrice(d, close));
            }
            out.sort(Comparator.comparing(dp -> dp.date));
        } catch (Exception ignored) {}
        return out;
    }

    public PricePair fetchLastTwoCloses(String ticker) {
        return lastTwoFromHistory(fetchDailyHistory(ticker, "5d", "1d"));
    }

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
}
