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
public class RunFinishParam {
    private OffsetDateTime finishedAt;
    private String status;
    private int universeSize;
    private int scannedSize;
    private int candidateSize;
    private int topN;
    private String reportPath;
    private String notes;
    private long runId;
}
