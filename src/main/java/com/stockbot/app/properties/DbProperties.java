package com.stockbot.app.properties;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "db")
public class DbProperties {
    private String path = "outputs/stockbot.db";
    private String url = "jdbc:postgresql://localhost:5432/stockbot";
    private String user = "stockbot";
    private String pass = "stockbot";
    private String schema = "stockbot";
    private SqlLog sqlLog = new SqlLog();

    @Getter
    @Setter
    public static class SqlLog {
        private boolean enabled = true;
    }
}

