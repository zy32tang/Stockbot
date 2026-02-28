package com.stockbot.jp.news;

import com.stockbot.jp.config.Config;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.ollama.OllamaChatModel;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Summarizes clustered events using LangChain4j + Ollama.
 */
public final class LangChainSummaryService {
    static final String NO_EVENT_HTML = "<p>近期新闻回溯窗口内未发现显著事件聚类。</p>";
    private final ChatLanguageModel chatModel;

    public LangChainSummaryService(Config config) {
        ChatLanguageModel built;
        try {
            built = OllamaChatModel.builder()
                    .baseUrl(config.getString("watchlist.ai.base_url", config.getString("ai.base_url", "http://127.0.0.1:11434")))
                    .modelName(config.getString("watchlist.ai.model", "llama3.1:latest"))
                    .temperature(config.getDouble("ai.temperature", 0.2))
                    .timeout(Duration.ofSeconds(Math.max(10, config.getInt("watchlist.ai.timeout_sec", config.getInt("ai.timeout_sec", 180)))))
                    .build();
        } catch (Exception e) {
            System.err.println("WARN: failed to initialize LangChain4j Ollama model: " + e.getMessage());
            built = null;
        }
        this.chatModel = built;
    }

    LangChainSummaryService(ChatLanguageModel chatModel) {
        this.chatModel = chatModel;
    }

    public String summarize(String ticker, String companyName, List<ClusterInput> clusters) {
        if (clusters == null || clusters.isEmpty()) {
            return NO_EVENT_HTML;
        }
        if (chatModel == null) {
            return fallbackSummary(clusters);
        }

        String prompt = buildPrompt(ticker, companyName, clusters);
        try {
            String out = chatModel.generate(prompt);
            String cleaned = cleanModelOutput(out);
            if (cleaned.isEmpty()) {
                return fallbackSummary(clusters);
            }
            if (!isMostlyChinese(cleaned)) {
                String rewritePrompt = buildChineseRewritePrompt(ticker, companyName, cleaned);
                String rewritten = cleanModelOutput(chatModel.generate(rewritePrompt));
                if (!rewritten.isEmpty() && isMostlyChinese(rewritten)) {
                    return rewritten;
                }
                return fallbackSummary(clusters);
            }
            return cleaned;
        } catch (Exception e) {
            System.err.println("WARN: LangChain4j summarize failed ticker=" + safe(ticker) + ", err=" + e.getMessage());
            return fallbackSummary(clusters);
        }
    }

    private String buildPrompt(String ticker, String companyName, List<ClusterInput> clusters) {
        StringBuilder sb = new StringBuilder(4096);
        sb.append("你是一名中文证券研究员。\n");
        sb.append("请为股票代码 ");
        sb.append(safe(ticker));
        if (!safe(companyName).isEmpty()) {
            sb.append(" (").append(companyName.trim()).append(")");
        }
        sb.append(" 输出简洁的新闻影响总结。\n");
        sb.append("输出规则：\n");
        sb.append("1) 仅输出 HTML 片段，不要输出 markdown 或代码块。\n");
        sb.append("2) 仅允许使用 <p>、<ul>、<li>、<strong> 标签。\n");
        sb.append("3) 内容必须为简体中文。\n");
        sb.append("4) 聚焦关键事件、潜在市场影响与风险提示，保持客观简洁。\n\n");
        sb.append("事件聚类：\n");
        int rank = 1;
        for (ClusterInput cluster : clusters) {
            if (cluster == null) {
                continue;
            }
            sb.append("聚类 ").append(rank++).append("：")
                    .append(safe(cluster.label))
                    .append("\n");
            int idx = 1;
            for (String item : cluster.items) {
                sb.append("- ").append(idx++).append(") ").append(safe(item)).append("\n");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    private String buildChineseRewritePrompt(String ticker, String companyName, String htmlFragment) {
        StringBuilder sb = new StringBuilder(2048);
        sb.append("请将以下股票新闻摘要改写为简体中文 HTML 片段。\n");
        sb.append("股票代码：").append(safe(ticker));
        if (!safe(companyName).isEmpty()) {
            sb.append("，公司：").append(safe(companyName));
        }
        sb.append("\n");
        sb.append("规则：\n");
        sb.append("1) 仅输出 HTML 片段。\n");
        sb.append("2) 仅允许使用 <p>、<ul>、<li>、<strong>。\n");
        sb.append("3) 不输出 markdown。\n");
        sb.append("4) 保持原有信息完整，不编造。\n\n");
        sb.append("原文：\n");
        sb.append(htmlFragment);
        return sb.toString();
    }

    private String fallbackSummary(List<ClusterInput> clusters) {
        StringBuilder sb = new StringBuilder(512);
        sb.append("<p><strong>新闻事件聚类要点：</strong></p><ul>");
        int used = 0;
        for (ClusterInput cluster : clusters) {
            if (cluster == null || safe(cluster.label).isEmpty()) {
                continue;
            }
            sb.append("<li>").append(escapeHtml(cluster.label));
            if (cluster.items != null && !cluster.items.isEmpty()) {
                sb.append(": ").append(escapeHtml(cluster.items.get(0)));
            }
            sb.append("</li>");
            used++;
            if (used >= 4) {
                break;
            }
        }
        sb.append("</ul>");
        if (used <= 0) {
            return NO_EVENT_HTML;
        }
        return cleanModelOutput(sb.toString());
    }

    private String cleanModelOutput(String out) {
        String text = safe(out)
                .replace("```html", "")
                .replace("```", "")
                .replace("###", "")
                .replace("**", "")
                .trim();
        if (text.isEmpty()) {
            return "";
        }
        String lower = text.toLowerCase(Locale.ROOT);
        if (lower.startsWith("<html") || lower.contains("<body")) {
            int bodyStart = lower.indexOf("<body");
            if (bodyStart >= 0) {
                int close = lower.indexOf('>', bodyStart);
                int end = lower.lastIndexOf("</body>");
                if (close >= 0 && end > close) {
                    return text.substring(close + 1, end).trim();
                }
            }
        }
        return text;
    }

    static boolean isMostlyChinese(String text) {
        String raw = text == null ? "" : text;
        if (raw.isBlank()) {
            return false;
        }
        int cjk = 0;
        int latin = 0;
        int digits = 0;
        int letters = 0;
        for (int i = 0; i < raw.length(); i++) {
            char ch = raw.charAt(i);
            if (Character.isDigit(ch)) {
                digits++;
                continue;
            }
            if (Character.UnicodeScript.of(ch) == Character.UnicodeScript.HAN) {
                cjk++;
                letters++;
                continue;
            }
            if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')) {
                latin++;
                letters++;
            }
        }
        if (cjk >= 12 && cjk >= latin) {
            return true;
        }
        if (letters <= 0) {
            return cjk > 0 && digits == 0;
        }
        double ratio = cjk / (double) letters;
        return ratio >= 0.35;
    }

    private String escapeHtml(String raw) {
        String s = safe(raw);
        return s.replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;");
    }

    private String safe(String value) {
        return value == null ? "" : value;
    }

    public static final class ClusterInput {
        public final String label;
        public final List<String> items;

        public ClusterInput(String label, List<String> items) {
            this.label = label == null ? "" : label;
            this.items = items == null ? List.of() : new ArrayList<>(items);
        }
    }
}
