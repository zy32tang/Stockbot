package com.stockbot.jp.db;

import com.stockbot.jp.db.mybatis.MyBatisSupport;
import com.stockbot.jp.db.mybatis.UniverseMapper;
import com.stockbot.jp.db.mybatis.UniverseRecordRow;
import com.stockbot.jp.model.UniverseRecord;
import org.apache.ibatis.session.SqlSession;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * DAO for universe table.
 */
public final class UniverseDao {
    private final Database database;

    public UniverseDao(Database database) {
        this.database = database;
    }

    public int replaceFromSource(String source, List<UniverseRecord> records) throws SQLException {
        if (records == null || records.isEmpty()) {
            return 0;
        }

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            conn.setAutoCommit(false);
            UniverseMapper mapper = session.getMapper(UniverseMapper.class);
            mapper.deactivateBySource(source, now);
            for (UniverseRecord record : records) {
                if (record == null) {
                    continue;
                }
                mapper.upsert(record.ticker, record.code, record.name, record.market, source, now);
            }
            conn.commit();
            return records.size();
        }
    }

    public List<UniverseRecord> listActive(int limit) throws SQLException {
        List<UniverseRecord> out = new ArrayList<>();
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            UniverseMapper mapper = session.getMapper(UniverseMapper.class);
            List<UniverseRecordRow> rows = mapper.listActive(limit);
            for (UniverseRecordRow row : rows) {
                if (row == null) {
                    continue;
                }
                out.add(new UniverseRecord(
                        row.getTicker(),
                        row.getCode(),
                        row.getName(),
                        row.getMarket()
                ));
            }
        }
        return out;
    }

    public int countActive() throws SQLException {
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            UniverseMapper mapper = session.getMapper(UniverseMapper.class);
            return mapper.countActive();
        }
    }
}
