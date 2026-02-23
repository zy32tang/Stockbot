package com.stockbot.jp.db.mybatis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CandidateRowRecord {
    private long runId;
    private int rankNo;
    private String ticker;
    private String code;
    private String name;
    private double score;
    private Double close;
}
