package com.stockbot.jp.db.mybatis;

import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.OffsetDateTime;
import java.util.List;

public interface NewsItemMapper {
    @Insert("INSERT INTO news_item(url, title, content, source, lang, region, published_at, created_at, updated_at) " +
            "VALUES(#{url}, #{title}, #{content}, #{source}, #{lang}, #{region}, #{publishedAt}, now(), now()) " +
            "ON CONFLICT(url) DO UPDATE SET " +
            "title=excluded.title, " +
            "content=excluded.content, " +
            "source=excluded.source, " +
            "lang=excluded.lang, " +
            "region=excluded.region, " +
            "published_at=COALESCE(excluded.published_at, news_item.published_at), " +
            "embedding=CASE " +
            "WHEN news_item.title IS DISTINCT FROM excluded.title OR news_item.content IS DISTINCT FROM excluded.content " +
            "THEN NULL ELSE news_item.embedding END, " +
            "updated_at=now()")
    int upsertNewsItem(
            @Param("url") String url,
            @Param("title") String title,
            @Param("content") String content,
            @Param("source") String source,
            @Param("lang") String lang,
            @Param("region") String region,
            @Param("publishedAt") OffsetDateTime publishedAt
    );

    @Select("SELECT id, url, title, content, source, lang, region, published_at, " +
            "NULL::text AS embedding_text, 0.0 AS similarity " +
            "FROM news_item WHERE embedding IS NULL " +
            "ORDER BY published_at DESC NULLS LAST, id DESC LIMIT #{limit}")
    List<NewsItemSearchRow> listWithoutEmbedding(@Param("limit") int limit);

    @Update("UPDATE news_item SET embedding=CAST(#{vectorLiteral} AS vector), updated_at=now() WHERE id=#{id}")
    int updateEmbedding(@Param("id") long id, @Param("vectorLiteral") String vectorLiteral);

    @Select({
            "<script>",
            "SELECT id, url, title, content, source, lang, region, published_at,",
            "embedding::text AS embedding_text,",
            "(1 - (embedding &lt;=&gt; CAST(#{vectorLiteral} AS vector))) AS similarity",
            "FROM news_item",
            "WHERE embedding IS NOT NULL",
            "<if test='lang != null and lang != \"\"'>",
            "AND lang = #{lang}",
            "</if>",
            "<if test='region != null and region != \"\"'>",
            "AND region = #{region}",
            "</if>",
            "<if test='lookbackDays &gt; 0'>",
            "AND published_at &gt;= (now() - (#{lookbackDays} || ' days')::interval)",
            "</if>",
            "ORDER BY embedding &lt;=&gt; CAST(#{vectorLiteral} AS vector)",
            "LIMIT #{topK}",
            "</script>"
    })
    List<NewsItemSearchRow> searchSimilar(
            @Param("vectorLiteral") String vectorLiteral,
            @Param("topK") int topK,
            @Param("lookbackDays") int lookbackDays,
            @Param("lang") String lang,
            @Param("region") String region
    );
}
