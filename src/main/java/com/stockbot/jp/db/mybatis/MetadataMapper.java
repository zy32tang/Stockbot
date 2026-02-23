package com.stockbot.jp.db.mybatis;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.time.OffsetDateTime;

public interface MetadataMapper {
    @Select("SELECT meta_value FROM metadata WHERE meta_key = #{key}")
    String selectMetaValue(@Param("key") String key);

    @Insert("INSERT INTO metadata(meta_key, meta_value, updated_at) VALUES(#{key}, #{value}, #{updatedAt}) " +
            "ON CONFLICT(meta_key) DO UPDATE SET meta_value=excluded.meta_value, updated_at=excluded.updated_at")
    int upsertMeta(
            @Param("key") String key,
            @Param("value") String value,
            @Param("updatedAt") OffsetDateTime updatedAt
    );

    @Delete("DELETE FROM metadata WHERE meta_key = #{key}")
    int deleteMeta(@Param("key") String key);
}
