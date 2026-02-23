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
public class RunInsertParam {
    private Long id;
    private String mode;
    private OffsetDateTime startedAt;
    private String status;
    private String notes;
}
