package com.stockbot.utils;

import java.util.LinkedHashMap;
import java.util.Map;

public class StepTimer {
    private final Map<String, Long> start = new LinkedHashMap<>();
    private final Map<String, Long> durMs = new LinkedHashMap<>();

    public void start(String step) {
        start.put(step, System.currentTimeMillis());
    }
    public void end(String step) {
        Long s = start.get(step);
        if (s != null) {
            durMs.put(step, System.currentTimeMillis() - s);
        }
    }
    public Map<String, Long> snapshot() { return new LinkedHashMap<>(durMs); }

    public String summaryText() {
        StringBuilder sb = new StringBuilder();
        sb.append("耗时统计\n");
        for (Map.Entry<String, Long> e : durMs.entrySet()) {
            sb.append(" - ").append(stepZh(e.getKey())).append(" = ").append(e.getValue()).append(" 毫秒\n");
        }
        return sb.toString();
    }

    private static String stepZh(String step) {
        if (step == null) return "";
        switch (step) {
            case "TOTAL":
                return "总耗时";
            case "DB_INIT":
                return "数据库初始化";
            case "FETCH_ALL":
                return "全量抓取";
            case "AI_SUMMARIZE":
                return "AI摘要";
            case "STATE":
                return "通知状态";
            case "DB_WRITE":
                return "数据库写入";
            case "OUTPUT":
                return "报告输出";
            case "EMAIL":
                return "邮件发送";
            default:
                return step;
        }
    }
}
