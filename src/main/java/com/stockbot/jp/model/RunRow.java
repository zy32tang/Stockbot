package com.stockbot.jp.model;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;

@Value
@AllArgsConstructor(access = AccessLevel.PUBLIC)
@Builder(toBuilder = true)
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
}


