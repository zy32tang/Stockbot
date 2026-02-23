package com.stockbot.jp.db;

import com.stockbot.jp.db.mybatis.MetadataMapper;
import com.stockbot.jp.db.mybatis.MyBatisSupport;
import org.apache.ibatis.session.SqlSession;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

/**
 * DAO for metadata key/value rows.
 */
public final class MetadataDao {
    private final Database database;

    public MetadataDao(Database database) {
        this.database = database;
    }

    public Optional<String> get(String key) throws SQLException {
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            MetadataMapper mapper = session.getMapper(MetadataMapper.class);
            return Optional.ofNullable(mapper.selectMetaValue(key));
        }
    }

    public void put(String key, String value) throws SQLException {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            MetadataMapper mapper = session.getMapper(MetadataMapper.class);
            mapper.upsertMeta(key, value, now);
        }
    }

    public void delete(String key) throws SQLException {
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            MetadataMapper mapper = session.getMapper(MetadataMapper.class);
            mapper.deleteMeta(key);
        }
    }
}
