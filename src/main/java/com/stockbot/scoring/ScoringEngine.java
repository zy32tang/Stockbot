package com.stockbot.scoring;

import com.stockbot.model.DailyPrice;
import com.stockbot.model.StockContext;

import java.util.List;

/**
 * 模块说明：ScoringEngine（class）。
 * 主要职责：承载 scoring 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public class ScoringEngine {

/**
 * 方法说明：score，负责计算评分并输出分值。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public void score(StockContext sc) {
        double fund = sc.factorScores.getOrDefault("fundamental", 50.0);
        double ind = sc.factorScores.getOrDefault("industry", 50.0);
        double macro = sc.factorScores.getOrDefault("macro", 50.0);
        double news = sc.factorScores.getOrDefault("news", 50.0);

        double avg = (fund + ind + macro + news) / 4.0;
        double base = (avg - 50.0) / 5.0;

        if (sc.pctChange != null) {
            base += sc.pctChange / 2.0;
        }

        if (base > 10.0) base = 10.0;
        if (base < -10.0) base = -10.0;
        sc.totalScore = base;

        double vol20 = annualizedVolatility(sc.priceHistory, 20);
        boolean highVolatility = Double.isFinite(vol20) && vol20 >= 38.0;
        boolean sharpDrop = sc.pctChange != null && sc.pctChange <= -3.0;

        if (base <= -2.0 || sharpDrop) {
            sc.rating = "DEFEND";
        } else if (base >= 3.0 && !highVolatility) {
            sc.rating = "ATTACK";
        } else {
            sc.rating = "NEUTRAL";
        }

        if (highVolatility || sharpDrop || base <= -2.5) {
            sc.risk = "RISK";
        } else {
            sc.risk = "NONE";
        }
    }

/**
 * 方法说明：positionSuggestion，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public String positionSuggestion(StockContext sc) {
        double s = sc.totalScore == null ? 0.0 : sc.totalScore;
        if (s >= 5) return "仓位建议：70%（激进）";
        if (s >= 3) return "仓位建议：45%（加仓）";
        if (s >= 0) return "仓位建议：20%（持有）";
        if (s >= -2) return "仓位建议：8%（观察）";
        return "仓位建议：0%（减仓/回避）";
    }

/**
 * 方法说明：annualizedVolatility，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static double annualizedVolatility(List<DailyPrice> history, int lookbackDays) {
        if (history == null || history.size() < 3) return Double.NaN;

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

        if (count < 2) return Double.NaN;
        double variance = m2 / (count - 1);
        if (variance < 0.0) return Double.NaN;

        double dailyStd = Math.sqrt(variance);
        return dailyStd * Math.sqrt(252.0) * 100.0;
    }
}
