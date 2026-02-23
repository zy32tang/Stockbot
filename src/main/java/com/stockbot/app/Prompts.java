package com.stockbot.app;

import com.stockbot.model.NewsItem;
import com.stockbot.model.StockContext;

public class Prompts {
    private Prompts() {}

    // Keep empty so caller can still pass a system prompt without strict constraints.
    public static String buildSystemPrompt() {
        return "";
    }

    public static String buildPrompt(StockContext sc) {
        StockContext context = sc == null ? new StockContext("") : sc;
        StringBuilder sb = new StringBuilder();
        sb.append("You are a financial analyst.\n");
        sb.append("Summarize latest stock updates and key risks/opportunities from the input below.\n");
        sb.append("Ticker: ").append(safe(context.ticker)).append("\n");
        sb.append("Pct change (%): ").append(context.pctChange == null ? "" : context.pctChange).append("\n");
        sb.append("Score: ").append(context.totalScore == null ? "" : context.totalScore).append("\n");
        sb.append("Gate reason: ").append(safe(context.gateReason)).append("\n");
        sb.append("News titles:\n");

        int n = 0;
        for (NewsItem ni : context.news) {
            if (ni == null || n++ >= 8) {
                continue;
            }
            String title = safe(ni.title);
            if (title.isEmpty()) {
                continue;
            }
            sb.append("- ").append(title).append("\n");
        }

        sb.append("\nOutput requirements:\n");
        sb.append("1) Output in Simplified Chinese.\n");
        sb.append("2) Plain text only, no Markdown.\n");
        sb.append("3) Use short paragraphs.\n");
        sb.append("4) Use common local company naming conventions.\n");
        sb.append("5) Only use facts from the given input; do not invent details.\n");
        sb.append("6) If information is missing, omit it; do not output words like unknown/unkown/N/A.\n");
        return sb.toString();
    }

    private static String safe(String text) {
        if (text == null) {
            return "";
        }
        return text.replace("\r", " ")
                .replace("\n", " ")
                .trim();
    }
}
