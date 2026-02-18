package com.stockbot.jp.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;

public final class MetadataDao {
    private final Database database;

    public MetadataDao(Database database) {
        this.database = database;
    }

    public Optional<String> get(String key) throws SQLException {
        String sql = "SELECT meta_value FROM metadata WHERE meta_key = ?";
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return Optional.ofNullable(rs.getString(1));
                }
            }
        }
        return Optional.empty();
    }

    public void put(String key, String value) throws SQLException {
        String sql = "INSERT INTO metadata(meta_key, meta_value, updated_at) VALUES(?, ?, ?) " +
                "ON CONFLICT(meta_key) DO UPDATE SET meta_value=excluded.meta_value, updated_at=excluded.updated_at";
        Instant now = Instant.now();
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, key);
            ps.setString(2, value);
            ps.setString(3, now.toString());
            ps.executeUpdate();
        }
    }

    public void delete(String key) throws SQLException {
        String sql = "DELETE FROM metadata WHERE meta_key = ?";
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, key);
            ps.executeUpdate();
        }
    }
}
