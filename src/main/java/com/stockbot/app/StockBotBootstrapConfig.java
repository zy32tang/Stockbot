package com.stockbot.app;

import com.stockbot.app.properties.DbProperties;
import com.stockbot.app.properties.EmailProperties;
import com.stockbot.app.properties.MailProperties;
import com.stockbot.app.properties.ScanProperties;
import com.stockbot.jp.config.Config;
import com.stockbot.jp.db.BarDailyDao;
import com.stockbot.jp.db.Database;
import com.stockbot.jp.db.MetadataDao;
import com.stockbot.jp.db.MigrationRunner;
import com.stockbot.jp.db.RunDao;
import com.stockbot.jp.db.ScanResultDao;
import com.stockbot.jp.db.UniverseDao;
import com.stockbot.jp.vector.EventMemoryService;
import com.stockbot.jp.vector.VectorSearchService;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;

import java.nio.file.Path;
import java.util.Map;

@Configuration
@EnableConfigurationProperties({DbProperties.class, ScanProperties.class, EmailProperties.class, MailProperties.class})
public class StockBotBootstrapConfig {
    @Bean
    public Config stockBotConfig(Environment environment) {
        Map<String, Object> stockBotRawProperties = Binder.get(environment)
                .bind("", Bindable.mapOf(String.class, Object.class))
                .orElseGet(Map::of);
        Path workingDir = Path.of(".").toAbsolutePath().normalize();
        return Config.fromConfigurationProperties(workingDir, stockBotRawProperties);
    }

    @Bean
    @Lazy
    public Database database(DbProperties dbProperties) {
        Database database = new Database(
                readDbUrl(dbProperties),
                readDbUser(dbProperties),
                readDbPass(dbProperties),
                readDbSchema(dbProperties),
                isSqlLogEnabled(dbProperties)
        );
        try {
            new MigrationRunner().run(database);
        } catch (Exception e) {
            throw new IllegalStateException("Database migration failed: " + e.getMessage(), e);
        }
        return database;
    }

    @Bean
    @Lazy
    public UniverseDao universeDao(Database database) {
        return new UniverseDao(database);
    }

    @Bean
    @Lazy
    public MetadataDao metadataDao(Database database) {
        return new MetadataDao(database);
    }

    @Bean
    @Lazy
    public BarDailyDao barDailyDao(Database database) {
        return new BarDailyDao(database);
    }

    @Bean
    @Lazy
    public RunDao runDao(Database database) {
        return new RunDao(database);
    }

    @Bean
    @Lazy
    public ScanResultDao scanResultDao(Database database) {
        return new ScanResultDao(database);
    }

    @Bean
    @Lazy
    public EventMemoryService eventMemoryService(Config config, Database database, BarDailyDao barDailyDao) {
        return new EventMemoryService(config, new VectorSearchService(database), barDailyDao);
    }

    private String readDbUrl(DbProperties dbProperties) {
        return firstNonBlank(
                System.getenv("STOCKBOT_DB_URL"),
                dbProperties == null ? null : dbProperties.getUrl(),
                "jdbc:postgresql://localhost:5432/stockbot"
        );
    }

    private String readDbUser(DbProperties dbProperties) {
        return firstNonBlank(
                System.getenv("STOCKBOT_DB_USER"),
                dbProperties == null ? null : dbProperties.getUser(),
                "stockbot"
        );
    }

    private String readDbPass(DbProperties dbProperties) {
        return firstNonBlank(
                System.getenv("STOCKBOT_DB_PASS"),
                dbProperties == null ? null : dbProperties.getPass(),
                "stockbot"
        );
    }

    private String readDbSchema(DbProperties dbProperties) {
        return firstNonBlank(
                dbProperties == null ? null : dbProperties.getSchema(),
                "stockbot"
        );
    }

    private boolean isSqlLogEnabled(DbProperties dbProperties) {
        if (dbProperties == null || dbProperties.getSqlLog() == null) {
            return true;
        }
        return dbProperties.getSqlLog().isEnabled();
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return "";
        }
        for (String value : values) {
            if (value == null) {
                continue;
            }
            String trimmed = value.trim();
            if (!trimmed.isEmpty()) {
                return trimmed;
            }
        }
        return "";
    }
}
