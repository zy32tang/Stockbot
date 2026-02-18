package com.stockbot.jp.db;

import com.stockbot.jp.model.UniverseRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public final class UniverseDao {
    private final Database database;

    public UniverseDao(Database database) {
        this.database = database;
    }

    public int replaceFromSource(String source, List<UniverseRecord> records) throws SQLException {
        if (records == null || records.isEmpty()) {
            return 0;
        }

        String deactivateSql = "UPDATE universe SET active=0, updated_at=? WHERE source=?";
        String upsertSql = "INSERT INTO universe(ticker, code, name, market, active, source, updated_at) " +
                "VALUES(?, ?, ?, ?, 1, ?, ?) " +
                "ON CONFLICT(ticker) DO UPDATE SET " +
                "code=excluded.code, name=excluded.name, market=excluded.market, active=1, " +
                "source=excluded.source, updated_at=excluded.updated_at";
        Instant now = Instant.now();

        try (Connection conn = database.connect()) {
            conn.setAutoCommit(false);
            try (PreparedStatement deactivate = conn.prepareStatement(deactivateSql);
                 PreparedStatement upsert = conn.prepareStatement(upsertSql)) {
                deactivate.setString(1, now.toString());
                deactivate.setString(2, source);
                deactivate.executeUpdate();

                for (UniverseRecord record : records) {
                    upsert.setString(1, record.ticker);
                    upsert.setString(2, record.code);
                    upsert.setString(3, record.name);
                    upsert.setString(4, record.market);
                    upsert.setString(5, source);
                    upsert.setString(6, now.toString());
                    upsert.addBatch();
                }
                upsert.executeBatch();
            }
            conn.commit();
            return records.size();
        }
    }

    public List<UniverseRecord> listActive(int limit) throws SQLException {
        String sql = "SELECT ticker, code, name, market FROM universe WHERE active=1 ORDER BY code ASC";
        if (limit > 0) {
            sql += " LIMIT " + limit;
        }
        List<UniverseRecord> out = new ArrayList<>();
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(new UniverseRecord(
                        rs.getString("ticker"),
                        rs.getString("code"),
                        rs.getString("name"),
                        rs.getString("market")
                ));
            }
        }
        return out;
    }

    public int countActive() throws SQLException {
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) AS c FROM universe WHERE active=1");
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
                return rs.getInt("c");
            }
        }
        return 0;
    }
}
