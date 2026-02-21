package com.stockbot.jp.db;

import com.stockbot.jp.model.BarDaily;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;
import java.util.StringJoiner;

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
        String sql = "INSERT INTO price_daily(ticker, trade_date, open, high, low, close, volume, source) " +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT(ticker, trade_date) DO UPDATE SET " +
                "open=excluded.open, high=excluded.high, low=excluded.low, close=excluded.close, " +
                "volume=excluded.volume, source=excluded.source";

        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            for (BarDaily bar : bars) {
                if (bar == null || bar.tradeDate == null) {
                    continue;
                }
                ps.setString(1, ticker);
                ps.setObject(2, bar.tradeDate);
                ps.setDouble(3, bar.open);
                ps.setDouble(4, bar.high);
                ps.setDouble(5, bar.low);
                ps.setDouble(6, bar.close);
                ps.setDouble(7, bar.volume);
                ps.setString(8, source);
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
        }
    }

    private LocalDate latestTradeDate(String ticker) throws SQLException {
        String sql = "SELECT MAX(trade_date) AS max_date FROM price_daily WHERE ticker=?";
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, ticker);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    LocalDate maxDate = rs.getObject("max_date", LocalDate.class);
                    if (maxDate != null) {
                        return maxDate;
                    }
                    String text = rs.getString("max_date");
                    if (text != null && !text.trim().isEmpty()) {
                        return LocalDate.parse(text.trim());
                    }
                }
            }
        }
        return null;
    }

    public List<BarDaily> loadRecentBars(String ticker, int limit) throws SQLException {
        String sql = "SELECT ticker, trade_date, open, high, low, close, volume " +
                "FROM price_daily WHERE ticker=? ORDER BY trade_date DESC LIMIT ?";
        List<BarDaily> desc = new ArrayList<>();
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, ticker);
            ps.setInt(2, Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    LocalDate tradeDate = rs.getObject("trade_date", LocalDate.class);
                    if (tradeDate == null) {
                        String raw = rs.getString("trade_date");
                        if (raw != null && !raw.trim().isEmpty()) {
                            tradeDate = LocalDate.parse(raw.trim());
                        }
                    }
                    if (tradeDate == null) {
                        continue;
                    }
                    desc.add(new BarDaily(
                            rs.getString("ticker"),
                            tradeDate,
                            rs.getDouble("open"),
                            rs.getDouble("high"),
                            rs.getDouble("low"),
                            rs.getDouble("close"),
                            rs.getDouble("volume")
                    ));
                }
            }
        }
        List<BarDaily> asc = new ArrayList<>(desc.size());
        for (int i = desc.size() - 1; i >= 0; i--) {
            asc.add(desc.get(i));
        }
        return asc;
    }

    public OptionalDouble closeOnOrAfterWithOffset(String ticker, LocalDate date, int offset) throws SQLException {
        String sql = "SELECT close FROM price_daily WHERE ticker=? AND trade_date>=? " +
                "ORDER BY trade_date ASC LIMIT 1 OFFSET ?";
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, ticker);
            ps.setObject(2, date);
            ps.setInt(3, Math.max(0, offset));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return OptionalDouble.of(rs.getDouble("close"));
                }
            }
        }
        return OptionalDouble.empty();
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
        StringJoiner in = new StringJoiner(",");
        for (int i = 0; i < normalized.size(); i++) {
            in.add("?");
        }
        String sql = "SELECT COUNT(*) AS c FROM (" +
                "SELECT ticker FROM price_daily WHERE ticker IN (" + in + ") GROUP BY ticker HAVING COUNT(*)>=?" +
                ") t";
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (String ticker : normalized) {
                ps.setString(idx++, ticker);
            }
            ps.setInt(idx, threshold);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt("c");
                }
            }
        }
        return 0;
    }
}
