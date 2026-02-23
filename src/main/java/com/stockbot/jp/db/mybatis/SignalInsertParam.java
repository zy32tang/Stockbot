package com.stockbot.jp.db.mybatis;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SignalInsertParam {
    private String runId;
    private String ticker;
    private OffsetDateTime asOf;
    private double score;
    private String riskLevel;
    private String signalState;
    private Double positionPct;
    private String reason;
}
