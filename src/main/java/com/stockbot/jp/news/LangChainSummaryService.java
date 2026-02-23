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

    public String summarize(String ticker, String companyName, List<ClusterInput> clusters) {
        if (clusters == null || clusters.isEmpty()) {
            return "<p>No material event clusters found in recent news.</p>";
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
            return cleaned;
        } catch (Exception e) {
            System.err.println("WARN: LangChain4j summarize failed ticker=" + safe(ticker) + ", err=" + e.getMessage());
            return fallbackSummary(clusters);
        }
    }

    private String buildPrompt(String ticker, String companyName, List<ClusterInput> clusters) {
        StringBuilder sb = new StringBuilder(4096);
        sb.append("You are a market analyst.\n");
        sb.append("Write a concise HTML fragment summary for ticker ");
        sb.append(safe(ticker));
        if (!safe(companyName).isEmpty()) {
            sb.append(" (").append(companyName.trim()).append(")");
        }
        sb.append(".\n");
        sb.append("Output rules:\n");
        sb.append("1) Output must be pure HTML fragment only.\n");
        sb.append("2) Do not output markdown markers like ###, **, ```.\n");
        sb.append("3) Use only <p>, <ul>, <li>, <strong> tags.\n");
        sb.append("4) Mention key events and likely market implications.\n");
        sb.append("5) Keep it short and factual.\n\n");
        sb.append("Event clusters:\n");
        int rank = 1;
        for (ClusterInput cluster : clusters) {
            if (cluster == null) {
                continue;
            }
            sb.append("Cluster ").append(rank++).append(": ")
                    .append(safe(cluster.label))
                    .append("\n");
            int idx = 1;
            for (String item : cluster.items) {
                sb.append("- ").append(idx++).append(". ").append(safe(item)).append("\n");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    private String fallbackSummary(List<ClusterInput> clusters) {
        StringBuilder sb = new StringBuilder(512);
        sb.append("<p><strong>Clustered news highlights:</strong></p><ul>");
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
        return sb.toString();
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
