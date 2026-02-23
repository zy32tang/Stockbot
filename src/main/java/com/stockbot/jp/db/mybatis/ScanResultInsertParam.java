package com.stockbot.jp.db.mybatis;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.OffsetDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ScanResultInsertParam {
    private long runId;
    private String ticker;
    private String code;
    private String market;
    private String dataSource;
    private LocalDate priceTimestamp;
    private int barsCount;
    private Double lastClose;
    private boolean cacheHit;
    private long fetchLatencyMs;
    private boolean fetchSuccess;
    private boolean indicatorReady;
    private boolean candidateReady;
    private String dataInsufficientReason;
    private String failureReason;
    private String requestFailureCategory;
    private String error;
    private OffsetDateTime createdAt;
}
