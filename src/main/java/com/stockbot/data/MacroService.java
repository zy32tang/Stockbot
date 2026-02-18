package com.stockbot.data;

import com.stockbot.model.DailyPrice;

import java.util.List;

public class MacroService {
    private static final long CACHE_TTL_MS = 5 * 60 * 1000L;

    private final MarketDataService market;

    private volatile long cacheAtMillis;
    private volatile double cachedScore;

    public MacroService() {
        this(null);
    }

    public MacroService(MarketDataService market) {
        this.market = market;
        this.cachedScore = 50.0;
        this.cacheAtMillis = 0L;
    }

    public synchronized double scoreMacroJP() {
        if (market == null) return 50.0;

        long now = System.currentTimeMillis();
        if (now - cacheAtMillis < CACHE_TTL_MS) return cachedScore;

        double score = computeMacroScore();
        cachedScore = score;
        cacheAtMillis = now;
        return score;
    }

    private double computeMacroScore() {
        double score = 50.0;

        Double nikkeiPct = pctChange("^N225");
        Double topixPct = pctChange("^TOPX");
        Double usdJpyPct = pctChange("JPY=X");
        Double vix = latestClose("^VIX");
        Double nikkeiTrend = momentumVsMovingAverage("^N225", 20);

        if (nikkeiPct != null) score += nikkeiPct * 4.0;
        if (topixPct != null) score += topixPct * 3.0;
        if (usdJpyPct != null) score += usdJpyPct * 1.2;
        if (nikkeiTrend != null) score += nikkeiTrend * 300.0;

        if (vix != null) {
            if (vix >= 30.0) score -= 14.0;
            else if (vix >= 24.0) score -= 9.0;
            else if (vix >= 18.0) score -= 4.0;
            else if (vix <= 14.0) score += 3.0;
        }

        return clamp(score, 0.0, 100.0);
    }

    private Double pctChange(String ticker) {
        MarketDataService.PricePair pp = market.fetchLastTwoCloses(ticker);
        if (pp.last == null || pp.prev == null || pp.prev == 0.0) return null;
        return (pp.last - pp.prev) / pp.prev * 100.0;
    }

    private Double latestClose(String ticker) {
        MarketDataService.PricePair pp = market.fetchLastTwoCloses(ticker);
        return pp.last;
    }

    private Double momentumVsMovingAverage(String ticker, int window) {
        List<DailyPrice> history = market.fetchDailyHistory(ticker, "6mo", "1d");
        if (history == null || history.size() < window) return null;

        double ma = 0.0;
        for (int i = history.size() - window; i < history.size(); i++) {
            ma += history.get(i).close;
        }
        ma /= window;

        double last = history.get(history.size() - 1).close;
        if (!Double.isFinite(last) || !Double.isFinite(ma) || ma == 0.0) return null;
        return (last - ma) / ma;
    }

    private static double clamp(double value, double min, double max) {
        if (value < min) return min;
        if (value > max) return max;
        return value;
    }
}
