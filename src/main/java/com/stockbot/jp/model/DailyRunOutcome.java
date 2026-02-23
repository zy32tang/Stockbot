package com.stockbot.jp.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

@Value
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder(toBuilder = true)
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
}


