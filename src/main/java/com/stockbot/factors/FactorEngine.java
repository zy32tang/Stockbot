package com.stockbot.factors;

import com.stockbot.data.FundamentalsService;
import com.stockbot.data.IndustryService;
import com.stockbot.data.MacroService;
import com.stockbot.model.StockContext;

/**
 * 模块说明：FactorEngine（class）。
 * 主要职责：承载 factors 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public class FactorEngine {
    private final FundamentalsService fundamentals;
    private final IndustryService industry;
    private final MacroService macro;

/**
 * 方法说明：FactorEngine，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public FactorEngine(FundamentalsService fundamentals, IndustryService industry, MacroService macro) {
        this.fundamentals = fundamentals;
        this.industry = industry;
        this.macro = macro;
    }

/**
 * 方法说明：computeFactors，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public void computeFactors(StockContext sc) {
        double fFund = fundamentals.scoreFundamentals(sc.ticker);
        String industryEn = industry.industryOf(sc.ticker);
        double fInd = industry.scoreIndustryTrend(sc.ticker, industryEn, sc.priceHistory, sc.pctChange);
        double fMacro = macro.scoreMacroJP();

        double fNews = Math.min(70.0, 40.0 + sc.news.size() * 5.0);
        if (sc.pctChange != null && sc.pctChange < 0) {
            fNews -= Math.min(15.0, Math.abs(sc.pctChange));
        }

        sc.factorScores.put("fundamental", clamp(fFund));
        sc.factorScores.put("industry", clamp(fInd));
        sc.factorScores.put("macro", clamp(fMacro));
        sc.factorScores.put("news", clamp(fNews));
        sc.displayName = industry.displayNameOf(sc.ticker);
    }

/**
 * 方法说明：clamp，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static double clamp(double score) {
        if (score < 0.0) return 0.0;
        if (score > 100.0) return 100.0;
        return score;
    }
}
