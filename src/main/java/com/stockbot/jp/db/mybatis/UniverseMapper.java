package com.stockbot.jp.db.mybatis;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.OffsetDateTime;
import java.util.List;

public interface UniverseMapper {
    @Update("UPDATE universe SET active=FALSE, updated_at=#{updatedAt} WHERE source=#{source}")
    int deactivateBySource(@Param("source") String source, @Param("updatedAt") OffsetDateTime updatedAt);

    @Insert("INSERT INTO universe(ticker, code, name, market, active, source, updated_at) " +
            "VALUES(#{ticker}, #{code}, #{name}, #{market}, TRUE, #{source}, #{updatedAt}) " +
            "ON CONFLICT(ticker) DO UPDATE SET " +
            "code=excluded.code, name=excluded.name, market=excluded.market, active=TRUE, " +
            "source=excluded.source, updated_at=excluded.updated_at")
    int upsert(
            @Param("ticker") String ticker,
            @Param("code") String code,
            @Param("name") String name,
            @Param("market") String market,
            @Param("source") String source,
            @Param("updatedAt") OffsetDateTime updatedAt
    );

    @Select({
            "<script>",
            "SELECT ticker, code, name, market FROM universe WHERE active=TRUE ORDER BY code ASC",
            "<if test='limit &gt; 0'> LIMIT #{limit}</if>",
            "</script>"
    })
    List<UniverseRecordRow> listActive(@Param("limit") int limit);

    @Select("SELECT COUNT(*) FROM universe WHERE active=TRUE")
    int countActive();
}
