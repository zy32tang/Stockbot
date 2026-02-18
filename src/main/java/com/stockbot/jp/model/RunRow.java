package com.stockbot.jp.model;

import java.time.Instant;

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
