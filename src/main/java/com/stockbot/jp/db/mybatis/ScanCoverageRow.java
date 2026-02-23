package com.stockbot.jp.db.mybatis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ScanCoverageRow {
    private int total;
    private int fetchCoverage;
    private int indicatorCoverage;
    private int exFilteredNonTradable;
    private int exHistoryShort;
    private int exStale;
    private int exNoData;
    private int tradableDenominator;
    private int tradableIndicatorCoverage;
}
