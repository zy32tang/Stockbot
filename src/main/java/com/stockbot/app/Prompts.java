package com.stockbot.app;

import com.stockbot.model.NewsItem;
import com.stockbot.model.StockContext;

public class Prompts {
    public static String buildPrompt(StockContext sc) {
        StringBuilder sb = new StringBuilder();
        sb.append("你是一名财经分析师。请根据以下信息总结该股票最新动态，并指出风险与机会。\n");
        sb.append("股票代码：").append(sc.ticker).append("\n");
        sb.append("涨跌幅（%）：").append(sc.pctChange).append("\n");
        sb.append("总分：").append(sc.totalScore).append("\n");
        sb.append("触发原因：").append(sc.gateReason).append("\n");
        sb.append("新闻标题：\n");
        int n = 0;
        for (NewsItem ni : sc.news) {
            if (n++ >= 8) break;
            sb.append("- ").append(ni.title).append("\n");
        }
        sb.append("\n请使用简体中文，不要使用 Markdown，只输出纯文本，分短段落输出。");
        sb.append("公司名请使用本地市场常用名称：日股用日文，美股用英文，中国股票用中文。");
        return sb.toString();
    }
}
