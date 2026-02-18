package com.stockbot.model;

import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

public class RunContext {
    public final String runMode; // 运行模式：手动/定时
    public final String label;   // 标签：11:30 / 15:00 / 自定义
    public final ZonedDateTime startedAt;
    public final Path workingDir;
    public final Path outputsDir;
    public final Map<String, Object> meta = new LinkedHashMap<>();

    public RunContext(String runMode, String label, ZonedDateTime startedAt, Path workingDir, Path outputsDir) {
        this.runMode = runMode;
        this.label = label;
        this.startedAt = startedAt;
        this.workingDir = workingDir;
        this.outputsDir = outputsDir;
    }
}
