package com.stockbot.jp.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Database connection manager backed by PostgreSQL.
 */
public final class Database {
    private static final Logger SQL_LOG = LogManager.getLogger("SQL");

    private final DataSource dataSource;
    private final String jdbcUrl;
    private final String schema;
    private final boolean sqlLogEnabled;

    public Database(String jdbcUrl, String user, String pass, String schema, boolean sqlLogEnabled) {
        if (isBlank(jdbcUrl)) {
            throw new IllegalArgumentException("db.url must not be empty");
        }
        this.jdbcUrl = jdbcUrl.trim();
        this.schema = normalizeSchema(schema);
        this.sqlLogEnabled = sqlLogEnabled;

        PGSimpleDataSource pg = new PGSimpleDataSource();
        pg.setUrl(this.jdbcUrl);
        if (!isBlank(user)) {
            pg.setUser(user.trim());
        }
        if (pass != null) {
            pg.setPassword(pass);
        }
        pg.setCurrentSchema(this.schema);
        pg.setApplicationName("stockbot");
        this.dataSource = pg;
    }

    public Connection connect() throws SQLException {
        Connection raw = dataSource.getConnection();
        try (Statement st = raw.createStatement()) {
            st.execute("SET search_path TO " + schema + ", public");
        }
        return sqlLogEnabled ? SqlLogProxy.wrapConnection(raw, SQL_LOG) : raw;
    }

    public String dbType() {
        return "POSTGRES";
    }

    public String schema() {
        return schema;
    }

    public String maskedJdbcUrl() {
        String out = jdbcUrl;
        out = out.replaceAll("(?i)(password=)[^&]+", "$1***");
        out = out.replaceAll("(://[^:/@]+:)[^@]+(@)", "$1***$2");
        return out;
    }

    private String normalizeSchema(String raw) {
        String value = isBlank(raw) ? "stockbot" : raw.trim();
        if (!value.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("invalid db.schema, allowed pattern: [A-Za-z_][A-Za-z0-9_]*");
        }
        return value;
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
