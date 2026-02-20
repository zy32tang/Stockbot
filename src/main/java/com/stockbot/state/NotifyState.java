package com.stockbot.state;

import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

/**
 * 模块说明：NotifyState（class）。
 * 主要职责：承载 state 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public class NotifyState {
    private final Map<String, String> lastSentDate = new HashMap<>();

/**
 * 方法说明：load，负责加载配置或数据。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static NotifyState load(Path path) {
        NotifyState s = new NotifyState();
        try {
            if (Files.exists(path)) {
                String txt = Files.readString(path, StandardCharsets.UTF_8);
                JSONObject o = new JSONObject(txt);
                for (String k : o.keySet()) s.lastSentDate.put(k, o.optString(k, ""));
            }
        } catch (Exception ignored) {}
        return s;
    }

/**
 * 方法说明：shouldNotify，负责判断条件是否满足。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public boolean shouldNotify(String ticker, String risk) {
        if (!"RISK".equalsIgnoreCase(risk)) return false;
        String today = LocalDate.now().toString();
        String last = lastSentDate.get(ticker);
        return last == null || !last.equals(today);
    }

/**
 * 方法说明：markNotified，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public void markNotified(String ticker) {
        lastSentDate.put(ticker, LocalDate.now().toString());
    }

/**
 * 方法说明：save，负责保存数据到目标存储。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public void save(Path path) throws IOException {
        JSONObject o = new JSONObject(lastSentDate);
        Files.createDirectories(path.getParent());
        Files.writeString(path, o.toString(2), StandardCharsets.UTF_8);
    }
}
