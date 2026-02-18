package com.stockbot.jp.strategy;

import com.stockbot.jp.model.FilterDecision;
import com.stockbot.jp.model.IndicatorSnapshot;
import com.stockbot.jp.model.RiskDecision;
import com.stockbot.jp.model.ScoreResult;
import org.json.JSONArray;
import org.json.JSONObject;

public final class ReasonJsonBuilder {

    public String buildReasonsJson(FilterDecision filter, RiskDecision risk, ScoreResult score) {
        JSONObject root = new JSONObject();
        root.put("filter_passed", filter.passed);
        root.put("risk_passed", risk.passed);
        root.put("filter_reasons", new JSONArray(filter.reasons));
        root.put("risk_flags", new JSONArray(risk.flags));
        root.put("filter_metrics", new JSONObject(filter.metrics));
        root.put("score_breakdown", new JSONObject(score.breakdown));
        return root.toString();
    }

    public String buildIndicatorsJson(IndicatorSnapshot ind) {
        JSONObject root = new JSONObject();
        root.put("last_close", round4(ind.lastClose));
        root.put("sma20", round4(ind.sma20));
        root.put("sma60", round4(ind.sma60));
        root.put("sma60_prev5", round4(ind.sma60Prev5));
        root.put("sma60_slope", round4(ind.sma60Slope));
        root.put("sma120", round4(ind.sma120));
        root.put("rsi14", round4(ind.rsi14));
        root.put("atr14", round4(ind.atr14));
        root.put("atr_pct", round4(ind.atrPct));
        root.put("bollinger_upper", round4(ind.bollingerUpper));
        root.put("bollinger_middle", round4(ind.bollingerMiddle));
        root.put("bollinger_lower", round4(ind.bollingerLower));
        root.put("drawdown120_pct", round4(ind.drawdown120Pct));
        root.put("volatility20_pct", round4(ind.volatility20Pct));
        root.put("volatility20_ratio", round4(ind.volatility20Pct / 100.0));
        root.put("avg_volume20", round4(ind.avgVolume20));
        root.put("volume_ratio20", round4(ind.volumeRatio20));
        root.put("pct_from_sma20", round4(ind.pctFromSma20));
        root.put("pct_from_sma60", round4(ind.pctFromSma60));
        root.put("return3d_pct", round4(ind.return3dPct));
        root.put("return5d_pct", round4(ind.return5dPct));
        root.put("return10d_pct", round4(ind.return10dPct));
        root.put("low_lookback", round4(ind.lowLookback));
        root.put("high_lookback", round4(ind.highLookback));
        return root.toString();
    }

    private double round4(double value) {
        return Math.round(value * 10000.0) / 10000.0;
    }
}
