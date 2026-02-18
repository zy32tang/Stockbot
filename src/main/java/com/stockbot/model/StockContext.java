package com.stockbot.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class StockContext {
    public final String ticker;
    public String displayName;
    public Double lastClose;
    public Double prevClose;
    public Double pctChange; // 涨跌幅（%）
    public final List<DailyPrice> priceHistory = new ArrayList<>();
    public final List<NewsItem> news = new ArrayList<>();
    public final Map<String, Double> factorScores = new LinkedHashMap<>(); // 因子分映射（基础面/行业/宏观/新闻）
    public Double totalScore;
    public String rating; // 评级代码（防守/中性/进攻）
    public String risk;   // 风险代码（有风险/无风险）
    public boolean aiRan;
    public String aiSummary;
    public String gateReason;

    public StockContext(String ticker) {
        this.ticker = ticker;
        this.displayName = ticker;
    }
}
