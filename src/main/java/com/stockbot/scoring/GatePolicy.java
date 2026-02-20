package com.stockbot.scoring;

import com.stockbot.model.StockContext;

/**
 * 模块说明：GatePolicy（class）。
 * 主要职责：承载 scoring 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public class GatePolicy {
    private final double scoreThreshold;
    private final int newsMin;
    private final double dropPctThreshold;

/**
 * 方法说明：GatePolicy，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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
