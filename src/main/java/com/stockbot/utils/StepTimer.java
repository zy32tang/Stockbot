package com.stockbot.utils;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 模块说明：StepTimer（class）。
 * 主要职责：承载 utils 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public class StepTimer {
    private final Map<String, Long> start = new LinkedHashMap<>();
    private final Map<String, Long> durMs = new LinkedHashMap<>();

/**
 * 方法说明：start，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public void start(String step) {
        start.put(step, System.currentTimeMillis());
    }
/**
 * 方法说明：end，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public void end(String step) {
        Long s = start.get(step);
        if (s != null) {
            durMs.put(step, System.currentTimeMillis() - s);
        }
    }
/**
 * 方法说明：snapshot，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public Map<String, Long> snapshot() { return new LinkedHashMap<>(durMs); }

/**
 * 方法说明：summaryText，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public String summaryText() {
        StringBuilder sb = new StringBuilder();
        sb.append("耗时统计\n");
        for (Map.Entry<String, Long> e : durMs.entrySet()) {
            sb.append(" - ").append(stepZh(e.getKey())).append(" = ").append(e.getValue()).append(" 毫秒\n");
        }
        return sb.toString();
    }

/**
 * 方法说明：stepZh，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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
