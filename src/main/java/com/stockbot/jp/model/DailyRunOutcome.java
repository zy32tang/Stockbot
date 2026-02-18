package com.stockbot.jp.model;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

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
