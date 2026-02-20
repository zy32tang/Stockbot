package com.stockbot.jp.model;

import java.time.Instant;

/**
 * 模块说明：RunRow（class）。
 * 主要职责：承载 model 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class RunRow {
    public final long id;
    public final String mode;
    public final Instant startedAt;
    public final Instant finishedAt;
    public final String status;
    public final int universeSize;
    public final int scannedSize;
    public final int candidateSize;
    public final int topN;
    public final String reportPath;
    public final String notes;

/**
 * 方法说明：RunRow，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public RunRow(
            long id,
            String mode,
            Instant startedAt,
            Instant finishedAt,
            String status,
            int universeSize,
            int scannedSize,
            int candidateSize,
            int topN,
            String reportPath,
            String notes
    ) {
        this.id = id;
        this.mode = mode;
        this.startedAt = startedAt;
        this.finishedAt = finishedAt;
        this.status = status;
        this.universeSize = universeSize;
        this.scannedSize = scannedSize;
        this.candidateSize = candidateSize;
        this.topN = topN;
        this.reportPath = reportPath;
        this.notes = notes;
    }
}
