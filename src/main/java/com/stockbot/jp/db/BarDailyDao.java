package com.stockbot.jp.db;

import com.stockbot.jp.db.mybatis.BarDailyMapper;
import com.stockbot.jp.db.mybatis.BarDailyRow;
import com.stockbot.jp.db.mybatis.MyBatisSupport;
import com.stockbot.jp.model.BarDaily;
import org.apache.ibatis.session.SqlSession;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;

/**
 * DAO for daily OHLCV prices.
 */
public final class BarDailyDao {
    private final Database database;

    public BarDailyDao(Database database) {
        this.database = database;
    }

    public void upsertBars(String ticker, List<BarDaily> bars, String source) throws SQLException {
        upsertBarsInternal(ticker, bars, source);
    }

    public int upsertBarsIncremental(String ticker, List<BarDaily> bars, String source, int overlapDays) throws SQLException {
        int initialDays = Math.max(60, overlapDays);
        return upsertBarsIncremental(ticker, bars, source, initialDays, 10);
    }

    public int upsertBarsIncremental(
            String ticker,
            List<BarDaily> bars,
            String source,
            int initialDays,
            int recentDays
    ) throws SQLException {
        if (bars == null || bars.isEmpty()) {
            return 0;
        }
        LocalDate latest = latestTradeDate(ticker);
        List<BarDaily> target = bars;
        if (latest == null) {
            int keep = Math.max(60, initialDays);
            if (bars.size() > keep) {
                target = new ArrayList<>(bars.subList(bars.size() - keep, bars.size()));
            }
        } else {
            LocalDate anchor = latest;
            if (!bars.isEmpty() && bars.get(bars.size() - 1) != null && bars.get(bars.size() - 1).tradeDate != null) {
                anchor = bars.get(bars.size() - 1).tradeDate;
            }
            LocalDate startByLastDate = latest.plusDays(1);
            LocalDate startByRecentDays = anchor.minusDays(Math.max(1, recentDays));
            LocalDate cutoff = startByLastDate.isBefore(startByRecentDays) ? startByLastDate : startByRecentDays;
            List<BarDaily> filtered = new ArrayList<>(bars.size());
            for (BarDaily bar : bars) {
                if (bar == null || bar.tradeDate == null) {
                    continue;
                }
                if (!bar.tradeDate.isBefore(cutoff)) {
                    filtered.add(bar);
                }
            }
            target = filtered;
        }
        if (target.isEmpty()) {
            return 0;
        }
        upsertBarsInternal(ticker, target, source);
        return target.size();
    }

    private void upsertBarsInternal(String ticker, List<BarDaily> bars, String source) throws SQLException {
        if (bars == null || bars.isEmpty()) {
            return;
        }
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            conn.setAutoCommit(false);
            BarDailyMapper mapper = session.getMapper(BarDailyMapper.class);
            for (BarDaily bar : bars) {
                if (bar == null || bar.tradeDate == null) {
                    continue;
                }
                mapper.upsertBar(
                        ticker,
                        bar.tradeDate,
                        bar.open,
                        bar.high,
                        bar.low,
                        bar.close,
                        bar.volume,
                        source
                );
            }
            conn.commit();
        }
    }

    private LocalDate latestTradeDate(String ticker) throws SQLException {
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            BarDailyMapper mapper = session.getMapper(BarDailyMapper.class);
            return mapper.selectLatestTradeDate(ticker);
        }
    }

    public List<BarDaily> loadRecentBars(String ticker, int limit) throws SQLException {
        List<BarDaily> desc = new ArrayList<>();
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            BarDailyMapper mapper = session.getMapper(BarDailyMapper.class);
            List<BarDailyRow> rows = mapper.selectRecentBars(ticker, Math.max(1, limit));
            for (BarDailyRow row : rows) {
                if (row == null || row.getTradeDate() == null) {
                    continue;
                }
                desc.add(new BarDaily(
                        row.getTicker(),
                        row.getTradeDate(),
                        n(row.getOpen()),
                        n(row.getHigh()),
                        n(row.getLow()),
                        n(row.getClose()),
                        n(row.getVolume())
                ));
            }
        }
        List<BarDaily> asc = new ArrayList<>(desc.size());
        for (int i = desc.size() - 1; i >= 0; i--) {
            asc.add(desc.get(i));
        }
        return asc;
    }

    public OptionalDouble closeOnOrAfterWithOffset(String ticker, LocalDate date, int offset) throws SQLException {
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            BarDailyMapper mapper = session.getMapper(BarDailyMapper.class);
            Double close = mapper.selectCloseOnOrAfterWithOffset(ticker, date, Math.max(0, offset));
            if (close == null) {
                return OptionalDouble.empty();
            }
            return OptionalDouble.of(close);
        }
    }

    public int countTickersWithMinBars(List<String> tickers, int minBars) throws SQLException {
        if (tickers == null || tickers.isEmpty()) {
            return 0;
        }
        List<String> normalized = new ArrayList<>();
        for (String ticker : tickers) {
            if (ticker == null || ticker.trim().isEmpty()) {
                continue;
            }
            normalized.add(ticker.trim());
        }
        if (normalized.isEmpty()) {
            return 0;
        }
        int threshold = Math.max(1, minBars);
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            BarDailyMapper mapper = session.getMapper(BarDailyMapper.class);
            return mapper.countTickersWithMinBars(normalized, threshold);
        }
    }

    private double n(Double value) {
        return value == null ? 0.0 : value;
    }
}
