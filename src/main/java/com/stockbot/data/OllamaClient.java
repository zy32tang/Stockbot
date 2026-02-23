package com.stockbot.data;

import com.stockbot.data.http.HttpClientEx;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Minimal Ollama wrapper used for text generation and embeddings.
 */
public class OllamaClient {
    private final HttpClientEx http;
    private final String baseUrl;
    private final String model;
    private final int timeoutSeconds;
    private final int maxTokens;

    public OllamaClient(HttpClientEx http, String baseUrl, String model, int timeoutSeconds, int maxTokens) {
        this.http = http;
        this.baseUrl = baseUrl;
        this.model = model;
        this.timeoutSeconds = timeoutSeconds;
        this.maxTokens = Math.max(0, maxTokens);
    }

    public String summarize(String prompt) {
        return summarize("", prompt);
    }

    public String summarize(String systemPrompt, String prompt) {
        try {
            JSONObject req = new JSONObject();
            req.put("model", model);
            req.put("prompt", prompt == null ? "" : prompt);
            if (systemPrompt != null && !systemPrompt.trim().isEmpty()) {
                req.put("system", systemPrompt.trim());
            }
            req.put("stream", false);
            if (maxTokens > 0) {
                JSONObject options = new JSONObject();
                options.put("num_predict", maxTokens);
                req.put("options", options);
            }
            String resp = http.postJson(baseUrl + "/api/generate", req.toString(), timeoutSeconds);
            JSONObject o = new JSONObject(resp);
            return o.optString("response", "");
        } catch (Exception e) {
            return "AI unavailable: " + e.getMessage();
        }
    }

    public float[] embed(String text) {
        return embed(model, text);
    }

    public float[] embed(String embedModel, String text) {
        String input = text == null ? "" : text.trim();
        if (input.isEmpty()) {
            return new float[0];
        }

        String modelToUse = embedModel == null || embedModel.trim().isEmpty()
                ? model
                : embedModel.trim();
        if (modelToUse == null || modelToUse.isEmpty()) {
            return new float[0];
        }

        JSONObject req = new JSONObject();
        req.put("model", modelToUse);
        req.put("prompt", input);
        try {
            String resp = http.postJson(baseUrl + "/api/embeddings", req.toString(), timeoutSeconds);
            float[] vec = parseEmbedding(resp);
            if (vec.length > 0) {
                return vec;
            }
        } catch (Exception ignored) {
            // Fall through to /api/embed for newer Ollama versions.
        }

        JSONObject reqEmbed = new JSONObject();
        reqEmbed.put("model", modelToUse);
        reqEmbed.put("input", input);
        try {
            String resp = http.postJson(baseUrl + "/api/embed", reqEmbed.toString(), timeoutSeconds);
            return parseEmbedding(resp);
        } catch (Exception ignored) {
            return new float[0];
        }
    }

    private float[] parseEmbedding(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return new float[0];
        }

        JSONObject root;
        try {
            root = new JSONObject(raw);
        } catch (Exception ignored) {
            return new float[0];
        }

        JSONArray arr = root.optJSONArray("embedding");
        if (arr == null) {
            JSONArray arrs = root.optJSONArray("embeddings");
            if (arrs != null && arrs.length() > 0) {
                Object first = arrs.opt(0);
                if (first instanceof JSONArray) {
                    arr = (JSONArray) first;
                }
            }
        }
        if (arr == null || arr.length() == 0) {
            return new float[0];
        }

        float[] out = new float[arr.length()];
        for (int i = 0; i < arr.length(); i++) {
            double d = arr.optDouble(i, Double.NaN);
            out[i] = Double.isFinite(d) ? (float) d : 0.0f;
        }
        return out;
    }
}
