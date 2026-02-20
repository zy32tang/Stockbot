package com.stockbot.model;

import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 模块说明：RunContext（class）。
 * 主要职责：承载 model 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public class RunContext {
    public final String runMode; // 运行模式：手动/定时
    public final String label;   // 标签：11:30 / 15:00 / 自定义
    public final ZonedDateTime startedAt;
    public final Path workingDir;
    public final Path outputsDir;
    public final Map<String, Object> meta = new LinkedHashMap<>();

/**
 * 方法说明：RunContext，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public RunContext(String runMode, String label, ZonedDateTime startedAt, Path workingDir, Path outputsDir) {
        this.runMode = runMode;
        this.label = label;
        this.startedAt = startedAt;
        this.workingDir = workingDir;
        this.outputsDir = outputsDir;
    }
}
