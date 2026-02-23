package com.stockbot.jp.db.mybatis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RunRowRecord {
    private long id;
    private String mode;
    private OffsetDateTime startedAt;
    private OffsetDateTime finishedAt;
    private String status;
    private int universeSize;
    private int scannedSize;
    private int candidateSize;
    private int topN;
    private String reportPath;
    private String notes;
}
