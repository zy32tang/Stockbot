package com.stockbot.model;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 模块说明：StockContext（class）。
 * 主要职责：承载 model 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
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

/**
 * 方法说明：StockContext，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public StockContext(String ticker) {
        this.ticker = ticker;
        this.displayName = ticker;
    }
}
