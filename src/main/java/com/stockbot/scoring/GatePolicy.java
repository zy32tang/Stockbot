package com.stockbot.scoring;

import com.stockbot.model.StockContext;

public class GatePolicy {
    private final double scoreThreshold;
    private final int newsMin;
    private final double dropPctThreshold;

    public GatePolicy(double scoreThreshold, int newsMin, double dropPctThreshold) {
        this.scoreThreshold = scoreThreshold;
        this.newsMin = newsMin;
        this.dropPctThreshold = dropPctThreshold;
    }

    // 判断某只股票是否需要触发智能分析
    public boolean shouldRunAi(StockContext sc) {
        StringBuilder reason = new StringBuilder();
        boolean hit = false;

        if (sc.totalScore != null && sc.totalScore <= scoreThreshold) {
            hit = true;
            reason.append("score<=threshold; ");
        }
        if (sc.news.size() >= newsMin) {
            hit = true;
            reason.append("news>=min; ");
        }
        if (sc.pctChange != null && sc.pctChange <= dropPctThreshold) {
            hit = true;
            reason.append("drop%<=threshold; ");
        }

        sc.gateReason = hit ? reason.toString().trim() : "not triggered";
        return hit;
    }
}
