package com.stockbot.jp.db.mybatis;

import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.sql.Connection;

/**
 * Centralized MyBatis bootstrap for JP database mappers.
 */
public final class MyBatisSupport {
    private static final SqlSessionFactory FACTORY = buildFactory();

    private MyBatisSupport() {
    }

    public static SqlSession openSession(Connection connection) {
        return FACTORY.openSession(connection);
    }

    private static SqlSessionFactory buildFactory() {
        Configuration config = new Configuration();
        config.setMapUnderscoreToCamelCase(true);

        config.addMapper(MetadataMapper.class);
        config.addMapper(UniverseMapper.class);
        config.addMapper(BarDailyMapper.class);
        config.addMapper(ScanResultMapper.class);
        config.addMapper(RunMapper.class);
        config.addMapper(NewsItemMapper.class);

        return new SqlSessionFactoryBuilder().build(config);
    }
}
