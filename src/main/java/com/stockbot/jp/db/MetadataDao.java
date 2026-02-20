package com.stockbot.jp.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;

/**
 * 模块说明：MetadataDao（class）。
 * 主要职责：承载 db 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class MetadataDao {
    private final Database database;

/**
 * 方法说明：MetadataDao，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public MetadataDao(Database database) {
        this.database = database;
    }

/**
 * 方法说明：get，负责获取数据并返回结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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

/**
 * 方法说明：put，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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

/**
 * 方法说明：delete，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public void delete(String key) throws SQLException {
        String sql = "DELETE FROM metadata WHERE meta_key = ?";
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, key);
            ps.executeUpdate();
        }
    }
}
