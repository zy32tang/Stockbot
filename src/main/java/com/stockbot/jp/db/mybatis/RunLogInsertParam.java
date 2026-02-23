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
public class RunLogInsertParam {
    private String runId;
    private String mode;
    private OffsetDateTime startedAt;
    private OffsetDateTime endedAt;
    private String step;
    private Long elapsedMs;
    private String status;
    private String message;
}
