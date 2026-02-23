package com.stockbot.jp.db.mybatis;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.time.LocalDate;
import java.util.List;

public interface BarDailyMapper {
    @Insert("INSERT INTO price_daily(ticker, trade_date, open, high, low, close, volume, source) " +
            "VALUES(#{ticker}, #{tradeDate}, #{open}, #{high}, #{low}, #{close}, #{volume}, #{source}) " +
            "ON CONFLICT(ticker, trade_date) DO UPDATE SET " +
            "open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close, " +
            "volume=excluded.volume, source=excluded.source")
    int upsertBar(
            @Param("ticker") String ticker,
            @Param("tradeDate") LocalDate tradeDate,
            @Param("open") double open,
            @Param("high") double high,
            @Param("low") double low,
            @Param("close") double close,
            @Param("volume") double volume,
            @Param("source") String source
    );

    @Select("SELECT MAX(trade_date) FROM price_daily WHERE ticker=#{ticker}")
    LocalDate selectLatestTradeDate(@Param("ticker") String ticker);

    @Select("SELECT ticker, trade_date, open, high, low, close, volume " +
            "FROM price_daily WHERE ticker=#{ticker} ORDER BY trade_date DESC LIMIT #{limit}")
    List<BarDailyRow> selectRecentBars(@Param("ticker") String ticker, @Param("limit") int limit);

    @Select("SELECT close FROM price_daily WHERE ticker=#{ticker} AND trade_date>=#{date} " +
            "ORDER BY trade_date ASC LIMIT 1 OFFSET #{offset}")
    Double selectCloseOnOrAfterWithOffset(
            @Param("ticker") String ticker,
            @Param("date") LocalDate date,
            @Param("offset") int offset
    );

    @Select({
            "<script>",
            "SELECT COUNT(*) FROM (",
            "SELECT ticker FROM price_daily WHERE ticker IN ",
            "<foreach collection='tickers' item='ticker' open='(' separator=',' close=')'>",
            "#{ticker}",
            "</foreach>",
            "GROUP BY ticker HAVING COUNT(*)&gt;=#{minBars}",
            ") t",
            "</script>"
    })
    int countTickersWithMinBars(@Param("tickers") List<String> tickers, @Param("minBars") int minBars);
}
