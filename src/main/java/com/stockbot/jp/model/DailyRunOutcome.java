package com.stockbot.jp.model;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

/**
 * 模块说明：DailyRunOutcome（class）。
 * 主要职责：承载 model 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class DailyRunOutcome {
    public final long runId;
    public final Instant startedAt;
    public final UniverseUpdateResult universeUpdate;
    public final int universeSize;
    public final int scannedSize;
    public final int failedSize;
    public final int candidateSize;
    public final int topN;
    public final Path reportPath;
    public final List<ScoredCandidate> topCandidates;
    public final List<WatchlistAnalysis> watchlistCandidates;
    public final List<ScoredCandidate> marketReferenceCandidates;
    public final int totalSegments;
    public final int processedSegments;
    public final int nextSegmentIndex;
    public final boolean partialRun;

/**
 * 方法说明：DailyRunOutcome，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public DailyRunOutcome(
            long runId,
            Instant startedAt,
            UniverseUpdateResult universeUpdate,
            int universeSize,
            int scannedSize,
            int failedSize,
            int candidateSize,
            int topN,
            Path reportPath,
            List<ScoredCandidate> topCandidates,
            List<WatchlistAnalysis> watchlistCandidates,
            List<ScoredCandidate> marketReferenceCandidates,
            int totalSegments,
            int processedSegments,
            int nextSegmentIndex,
            boolean partialRun
    ) {
        this.runId = runId;
        this.startedAt = startedAt;
        this.universeUpdate = universeUpdate;
        this.universeSize = universeSize;
        this.scannedSize = scannedSize;
        this.failedSize = failedSize;
        this.candidateSize = candidateSize;
        this.topN = topN;
        this.reportPath = reportPath;
        this.topCandidates = topCandidates;
        this.watchlistCandidates = watchlistCandidates;
        this.marketReferenceCandidates = marketReferenceCandidates;
        this.totalSegments = totalSegments;
        this.processedSegments = processedSegments;
        this.nextSegmentIndex = nextSegmentIndex;
        this.partialRun = partialRun;
    }
}
