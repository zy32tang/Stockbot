package com.stockbot.jp.db.mybatis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BarDailyRow {
    private String ticker;
    private LocalDate tradeDate;
    private Double open;
    private Double high;
    private Double low;
    private Double close;
    private Double volume;
}
