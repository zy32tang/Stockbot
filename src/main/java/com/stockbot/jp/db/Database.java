package com.stockbot.jp.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 模块说明：Database（class）。
 * 主要职责：承载 db 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class Database {
    private static final Logger SQL_LOG = LogManager.getLogger("SQL");

    private final String jdbcUrl;
    private final boolean sqlLogEnabled;

/**
 * 方法说明：Database，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public Database(Path dbPath) {
        this(dbPath, false);
    }

/**
 * 方法说明：Database，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public Database(Path dbPath, boolean sqlLogEnabled) {
        try {
            Path parent = dbPath.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
        } catch (Exception e) {
            throw new IllegalStateException("failed to prepare DB directory: " + e.getMessage(), e);
        }
        this.jdbcUrl = "jdbc:sqlite:" + dbPath.toAbsolutePath();
        this.sqlLogEnabled = sqlLogEnabled;
    }

/**
 * 方法说明：connect，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public Connection connect() throws SQLException {
        Connection raw = DriverManager.getConnection(jdbcUrl);
        Connection connection = sqlLogEnabled ? SqlLogProxy.wrapConnection(raw, SQL_LOG) : raw;
        try (Statement st = connection.createStatement()) {
            st.execute("PRAGMA foreign_keys = ON");
            st.execute("PRAGMA busy_timeout = 5000");
            st.execute("PRAGMA journal_mode = WAL");
            st.execute("PRAGMA synchronous = NORMAL");
        }
        return connection;
    }
}
