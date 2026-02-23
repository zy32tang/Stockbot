package com.stockbot.jp.db.mybatis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ScoredCandidateRowRecord {
    private String ticker;
    private String code;
    private String name;
    private String market;
    private Double score;
    private Double close;
    private String reasonsJson;
    private String indicatorsJson;
}
