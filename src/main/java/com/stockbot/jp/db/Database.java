package com.stockbot.jp.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public final class Database {
    private static final Logger SQL_LOG = LogManager.getLogger("SQL");

    private final String jdbcUrl;
    private final boolean sqlLogEnabled;

    public Database(Path dbPath) {
        this(dbPath, false);
    }

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
