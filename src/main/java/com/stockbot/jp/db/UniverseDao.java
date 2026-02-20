package com.stockbot.jp.db;

import com.stockbot.jp.model.UniverseRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 模块说明：UniverseDao（class）。
 * 主要职责：承载 db 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class UniverseDao {
    private final Database database;

/**
 * 方法说明：UniverseDao，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public UniverseDao(Database database) {
        this.database = database;
    }

/**
 * 方法说明：replaceFromSource，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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

/**
 * 方法说明：listActive，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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

/**
 * 方法说明：countActive，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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
