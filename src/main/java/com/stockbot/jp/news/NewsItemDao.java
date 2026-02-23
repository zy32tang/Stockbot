package com.stockbot.jp.news;

import com.stockbot.jp.db.Database;
import com.stockbot.jp.db.mybatis.MyBatisSupport;
import com.stockbot.jp.db.mybatis.NewsItemMapper;
import com.stockbot.jp.db.mybatis.NewsItemSearchRow;
import lombok.Builder;
import lombok.Value;
import org.apache.ibatis.session.SqlSession;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * DAO for news_item persistence and pgvector search.
 */
public final class NewsItemDao {
    private final Database database;

    public NewsItemDao(Database database) {
        this.database = database;
    }

    public int upsertAll(List<UpsertItem> items) throws SQLException {
        if (items == null || items.isEmpty()) {
            return 0;
        }

        int affected = 0;
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            conn.setAutoCommit(false);
            NewsItemMapper mapper = session.getMapper(NewsItemMapper.class);
            for (UpsertItem item : items) {
                if (item == null || isBlank(item.getUrl()) || isBlank(item.getTitle())) {
                    continue;
                }
                mapper.upsertNewsItem(
                        item.getUrl().trim(),
                        item.getTitle().trim(),
                        safe(item.getContent()),
                        blankTo(item.getSource(), "rss"),
                        safe(item.getLang()),
                        safe(item.getRegion()),
                        item.getPublishedAt()
                );
                affected++;
            }
            conn.commit();
        }
        return affected;
    }

    public List<NewsItemRecord> listWithoutEmbedding(int limit) throws SQLException {
        int safeLimit = Math.max(1, limit);
        List<NewsItemRecord> out = new ArrayList<>();
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            NewsItemMapper mapper = session.getMapper(NewsItemMapper.class);
            for (NewsItemSearchRow row : mapper.listWithoutEmbedding(safeLimit)) {
                out.add(toRecord(row));
            }
        }
        return out;
    }

    public void updateEmbedding(long id, float[] embedding) throws SQLException {
        String literal = toVectorLiteral(embedding);
        if (literal == null) {
            return;
        }
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            NewsItemMapper mapper = session.getMapper(NewsItemMapper.class);
            mapper.updateEmbedding(id, literal);
        }
    }

    public List<NewsItemRecord> searchSimilar(float[] queryEmbedding, SearchOptions options) throws SQLException {
        String literal = toVectorLiteral(queryEmbedding);
        if (literal == null || options == null) {
            return List.of();
        }
        int topK = Math.max(1, options.getTopK());
        int lookbackDays = Math.max(0, options.getLookbackDays());
        String lang = safe(options.getLang());
        String region = safe(options.getRegion());

        List<NewsItemRecord> out = new ArrayList<>();
        try (Connection conn = database.connect();
             SqlSession session = MyBatisSupport.openSession(conn)) {
            NewsItemMapper mapper = session.getMapper(NewsItemMapper.class);
            List<NewsItemSearchRow> rows = mapper.searchSimilar(literal, topK, lookbackDays, lang, region);
            for (NewsItemSearchRow row : rows) {
                out.add(toRecord(row));
            }
        }
        return out;
    }

    private NewsItemRecord toRecord(NewsItemSearchRow row) {
        if (row == null) {
            return NewsItemRecord.builder().build();
        }
        return NewsItemRecord.builder()
                .id(row.getId())
                .url(safe(row.getUrl()))
                .title(safe(row.getTitle()))
                .content(safe(row.getContent()))
                .source(safe(row.getSource()))
                .lang(safe(row.getLang()))
                .region(safe(row.getRegion()))
                .publishedAt(row.getPublishedAt())
                .embedding(parseVectorLiteral(row.getEmbeddingText()))
                .similarity(row.getSimilarity() == null ? 0.0 : row.getSimilarity())
                .build();
    }

    public static String toVectorLiteral(float[] vector) {
        if (vector == null || vector.length == 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder(vector.length * 8 + 2);
        sb.append('[');
        int valid = 0;
        for (int i = 0; i < vector.length; i++) {
            float v = vector[i];
            if (!Float.isFinite(v)) {
                v = 0.0f;
            }
            if (valid > 0) {
                sb.append(',');
            }
            sb.append(String.format(Locale.US, "%f", v));
            valid++;
        }
        sb.append(']');
        return valid == 0 ? null : sb.toString();
    }

    public static float[] parseVectorLiteral(String text) {
        if (text == null) {
            return new float[0];
        }
        String raw = text.trim();
        if (raw.isEmpty()) {
            return new float[0];
        }
        if (raw.startsWith("[")) {
            raw = raw.substring(1);
        }
        if (raw.endsWith("]")) {
            raw = raw.substring(0, raw.length() - 1);
        }
        if (raw.trim().isEmpty()) {
            return new float[0];
        }
        String[] tokens = raw.split(",");
        float[] out = new float[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            try {
                out[i] = Float.parseFloat(tokens[i].trim());
            } catch (Exception ignored) {
                out[i] = 0.0f;
            }
        }
        return out;
    }

    private String safe(String value) {
        return value == null ? "" : value;
    }

    private String blankTo(String value, String fallback) {
        String text = safe(value).trim();
        return text.isEmpty() ? fallback : text;
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    @Value
    @Builder(toBuilder = true)
    public static class UpsertItem {
        String url;
        String title;
        String content;
        String source;
        String lang;
        String region;
        OffsetDateTime publishedAt;
    }

    @Value
    public static class SearchOptions {
        int topK;
        int lookbackDays;
        String lang;
        String region;

        @Builder(toBuilder = true)
        public SearchOptions(int topK, int lookbackDays, String lang, String region) {
            this.topK = Math.max(1, topK);
            this.lookbackDays = Math.max(0, lookbackDays);
            this.lang = lang == null ? "" : lang.trim();
            this.region = region == null ? "" : region.trim();
        }
    }

    @Value
    public static class NewsItemRecord {
        long id;
        String url;
        String title;
        String content;
        String source;
        String lang;
        String region;
        OffsetDateTime publishedAt;
        float[] embedding;
        double similarity;

        @Builder(toBuilder = true)
        public NewsItemRecord(
                long id,
                String url,
                String title,
                String content,
                String source,
                String lang,
                String region,
                OffsetDateTime publishedAt,
                float[] embedding,
                double similarity
        ) {
            this.id = id;
            this.url = url == null ? "" : url;
            this.title = title == null ? "" : title;
            this.content = content == null ? "" : content;
            this.source = source == null ? "" : source;
            this.lang = lang == null ? "" : lang;
            this.region = region == null ? "" : region;
            this.publishedAt = publishedAt;
            this.embedding = embedding == null ? new float[0] : embedding;
            this.similarity = Double.isFinite(similarity) ? similarity : 0.0;
        }
    }
}
