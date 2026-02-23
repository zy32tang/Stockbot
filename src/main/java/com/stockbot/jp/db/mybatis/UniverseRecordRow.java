package com.stockbot.jp.db.mybatis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UniverseRecordRow {
    private String ticker;
    private String code;
    private String name;
    private String market;
}
