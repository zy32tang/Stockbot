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
public class CandidateInsertParam {
    private long runId;
    private int rankNo;
    private String ticker;
    private String code;
    private String name;
    private String market;
    private double score;
    private Double close;
    private String reasonsJson;
    private String indicatorsJson;
    private OffsetDateTime createdAt;
}
