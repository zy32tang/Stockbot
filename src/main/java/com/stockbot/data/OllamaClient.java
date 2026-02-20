package com.stockbot.data;

import com.stockbot.data.http.HttpClientEx;
import org.json.JSONObject;

/**
 * 模块说明：OllamaClient（class）。
 * 主要职责：承载 data 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public class OllamaClient {
    private final HttpClientEx http;
    private final String baseUrl;
    private final String model;
    private final int timeoutSeconds;
    private final int maxTokens;

/**
 * 方法说明：OllamaClient，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public OllamaClient(HttpClientEx http, String baseUrl, String model, int timeoutSeconds, int maxTokens) {
        this.http = http;
        this.baseUrl = baseUrl;
        this.model = model;
        this.timeoutSeconds = timeoutSeconds;
        this.maxTokens = Math.max(0, maxTokens);
    }

/**
 * 方法说明：summarize，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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
