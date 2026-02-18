package com.stockbot.factors;

import com.stockbot.data.FundamentalsService;
import com.stockbot.data.IndustryService;
import com.stockbot.data.MacroService;
import com.stockbot.model.StockContext;

public class FactorEngine {
    private final FundamentalsService fundamentals;
    private final IndustryService industry;
    private final MacroService macro;

    public FactorEngine(FundamentalsService fundamentals, IndustryService industry, MacroService macro) {
        this.fundamentals = fundamentals;
        this.industry = industry;
        this.macro = macro;
    }

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

    private static double clamp(double score) {
        if (score < 0.0) return 0.0;
        if (score > 100.0) return 100.0;
        return score;
    }
}
