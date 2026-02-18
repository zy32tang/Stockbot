package com.stockbot.data;

import com.stockbot.data.http.HttpClientEx;
import org.json.JSONObject;

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
        try {
            JSONObject req = new JSONObject();
            req.put("model", model);
            req.put("prompt", prompt);
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
            return "AI当前不可用：" + e.getMessage();
        }
    }
}
