package com.stockbot.jp.news;

import com.stockbot.data.OllamaClient;
import com.stockbot.data.http.HttpClientEx;
import com.stockbot.jp.config.Config;

import java.sql.SQLException;
import java.util.List;

/**
 * Generates embeddings with Ollama and writes back to news_item.embedding.
 */
public final class OllamaEmbeddingService {
    private final NewsItemDao newsItemDao;
    private final OllamaClient ollamaClient;
    private final String embedModel;
    private final int embedTextMaxChars;
    private final int vectorDim;

    public OllamaEmbeddingService(Config config, HttpClientEx httpClient, NewsItemDao newsItemDao) {
        this.newsItemDao = newsItemDao;
        this.embedModel = config.getString("vector.embed.model", "nomic-embed-text");
        this.embedTextMaxChars = Math.max(300, config.getInt("news.embedding.max_chars", 3000));
        this.vectorDim = Math.max(8, config.getInt("news.vector.dimension", 1536));
        this.ollamaClient = new OllamaClient(
                httpClient,
                config.getString("watchlist.ai.base_url", config.getString("ai.base_url", "http://127.0.0.1:11434")),
                config.getString("watchlist.ai.model", "llama3.1:latest"),
                Math.max(5, config.getInt("watchlist.ai.timeout_sec", config.getInt("ai.timeout_sec", 180))),
                Math.max(0, config.getInt("watchlist.ai.max_tokens", config.getInt("ai.max_tokens", 400)))
        );
    }

    public int embedMissing(int limit) {
        int safeLimit = Math.max(1, limit);
        List<NewsItemDao.NewsItemRecord> pending;
        try {
            pending = newsItemDao.listWithoutEmbedding(safeLimit);
        } catch (SQLException e) {
            System.err.println("WARN: listWithoutEmbedding failed: " + e.getMessage());
            return 0;
        }
        int embedded = 0;
        for (NewsItemDao.NewsItemRecord item : pending) {
            if (item == null) {
                continue;
            }
            String text = buildEmbeddingText(item.title, item.content);
            float[] vec = embedText(text);
            if (vec.length == 0) {
                continue;
            }
            try {
                newsItemDao.updateEmbedding(item.id, vec);
                embedded++;
            } catch (Exception e) {
                System.err.println("WARN: updateEmbedding failed id=" + item.id + ", err=" + e.getMessage());
            }
        }
        return embedded;
    }

    public float[] embedText(String rawText) {
        String text = normalizeText(rawText);
        if (text.isEmpty()) {
            return new float[0];
        }
        float[] raw = ollamaClient.embed(embedModel, text);
        if (raw == null || raw.length == 0) {
            return new float[0];
        }
        return normalizeDimensionAndLength(raw, vectorDim);
    }

    private String buildEmbeddingText(String title, String content) {
        StringBuilder sb = new StringBuilder();
        if (title != null && !title.trim().isEmpty()) {
            sb.append(title.trim());
        }
        if (content != null && !content.trim().isEmpty()) {
            if (sb.length() > 0) {
                sb.append("\n\n");
            }
            sb.append(content.trim());
        }
        return normalizeText(sb.toString());
    }

    private String normalizeText(String raw) {
        if (raw == null) {
            return "";
        }
        String text = raw.replace('\u3000', ' ')
                .replace("\r", " ")
                .replace("\n", " ")
                .replaceAll("\\s+", " ")
                .trim();
        if (text.length() > embedTextMaxChars) {
            return text.substring(0, embedTextMaxChars);
        }
        return text;
    }

    private float[] normalizeDimensionAndLength(float[] vector, int targetDim) {
        int size = Math.max(1, targetDim);
        float[] out = new float[size];
        int copy = Math.min(size, vector.length);
        for (int i = 0; i < copy; i++) {
            float v = vector[i];
            out[i] = Float.isFinite(v) ? v : 0.0f;
        }
        double norm = 0.0;
        for (float v : out) {
            norm += (double) v * (double) v;
        }
        if (norm <= 0.0) {
            return out;
        }
        double scale = 1.0 / Math.sqrt(norm);
        for (int i = 0; i < out.length; i++) {
            out[i] = (float) (out[i] * scale);
        }
        return out;
    }
}
