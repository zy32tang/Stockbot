package com.stockbot.jp.news;

import com.stockbot.jp.db.Database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
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
        String sql = "INSERT INTO news_item(url, title, content, source, lang, region, published_at, created_at, updated_at) " +
                "VALUES(?, ?, ?, ?, ?, ?, ?, now(), now()) " +
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
                "updated_at=now()";

        int affected = 0;
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            for (UpsertItem item : items) {
                if (item == null || isBlank(item.url) || isBlank(item.title)) {
                    continue;
                }
                ps.setString(1, item.url.trim());
                ps.setString(2, item.title.trim());
                ps.setString(3, safe(item.content));
                ps.setString(4, blankTo(item.source, "rss"));
                ps.setString(5, safe(item.lang));
                ps.setString(6, safe(item.region));
                ps.setObject(7, item.publishedAt);
                ps.addBatch();
                affected++;
            }
            ps.executeBatch();
            conn.commit();
        }
        return affected;
    }

    public List<NewsItemRecord> listWithoutEmbedding(int limit) throws SQLException {
        int safeLimit = Math.max(1, limit);
        String sql = "SELECT id, url, title, content, source, lang, region, published_at " +
                "FROM news_item WHERE embedding IS NULL ORDER BY published_at DESC NULLS LAST, id DESC LIMIT ?";
        List<NewsItemRecord> out = new ArrayList<>();
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, safeLimit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new NewsItemRecord(
                            rs.getLong("id"),
                            safe(rs.getString("url")),
                            safe(rs.getString("title")),
                            safe(rs.getString("content")),
                            safe(rs.getString("source")),
                            safe(rs.getString("lang")),
                            safe(rs.getString("region")),
                            readOffsetDateTime(rs, "published_at"),
                            null,
                            0.0
                    ));
                }
            }
        }
        return out;
    }

    public void updateEmbedding(long id, float[] embedding) throws SQLException {
        String literal = toVectorLiteral(embedding);
        if (literal == null) {
            return;
        }
        String sql = "UPDATE news_item SET embedding=CAST(? AS vector), updated_at=now() WHERE id=?";
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, literal);
            ps.setLong(2, id);
            ps.executeUpdate();
        }
    }

    public List<NewsItemRecord> searchSimilar(float[] queryEmbedding, SearchOptions options) throws SQLException {
        String literal = toVectorLiteral(queryEmbedding);
        if (literal == null || options == null) {
            return List.of();
        }
        int topK = Math.max(1, options.topK);
        int lookbackDays = Math.max(0, options.lookbackDays);
        String lang = safe(options.lang);
        String region = safe(options.region);

        String sql = "SELECT id, url, title, content, source, lang, region, published_at, " +
                "embedding::text AS embedding_text, " +
                "(1 - (embedding <=> CAST(? AS vector))) AS similarity " +
                "FROM news_item " +
                "WHERE embedding IS NOT NULL " +
                "AND (? = '' OR lang = ?) " +
                "AND (? = '' OR region = ?) " +
                "AND (? <= 0 OR published_at >= (now() - (? || ' days')::interval)) " +
                "ORDER BY embedding <=> CAST(? AS vector) " +
                "LIMIT ?";

        List<NewsItemRecord> out = new ArrayList<>();
        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            ps.setString(idx++, literal);
            ps.setString(idx++, lang);
            ps.setString(idx++, lang);
            ps.setString(idx++, region);
            ps.setString(idx++, region);
            ps.setInt(idx++, lookbackDays);
            ps.setInt(idx++, lookbackDays);
            ps.setString(idx++, literal);
            ps.setInt(idx, topK);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new NewsItemRecord(
                            rs.getLong("id"),
                            safe(rs.getString("url")),
                            safe(rs.getString("title")),
                            safe(rs.getString("content")),
                            safe(rs.getString("source")),
                            safe(rs.getString("lang")),
                            safe(rs.getString("region")),
                            readOffsetDateTime(rs, "published_at"),
                            parseVectorLiteral(rs.getString("embedding_text")),
                            rs.getDouble("similarity")
                    ));
                }
            }
        }
        return out;
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

    private OffsetDateTime readOffsetDateTime(ResultSet rs, String column) {
        try {
            OffsetDateTime v = rs.getObject(column, OffsetDateTime.class);
            if (v != null) {
                return v;
            }
        } catch (Exception ignored) {
            // Fall through.
        }
        try {
            java.sql.Timestamp ts = rs.getTimestamp(column);
            return ts == null ? null : ts.toInstant().atOffset(ZoneOffset.UTC);
        } catch (Exception ignored) {
            return null;
        }
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

    public static final class UpsertItem {
        public final String url;
        public final String title;
        public final String content;
        public final String source;
        public final String lang;
        public final String region;
        public final OffsetDateTime publishedAt;

        public UpsertItem(
                String url,
                String title,
                String content,
                String source,
                String lang,
                String region,
                OffsetDateTime publishedAt
        ) {
            this.url = url;
            this.title = title;
            this.content = content;
            this.source = source;
            this.lang = lang;
            this.region = region;
            this.publishedAt = publishedAt;
        }
    }

    public static final class SearchOptions {
        public final int topK;
        public final int lookbackDays;
        public final String lang;
        public final String region;

        public SearchOptions(int topK, int lookbackDays, String lang, String region) {
            this.topK = Math.max(1, topK);
            this.lookbackDays = Math.max(0, lookbackDays);
            this.lang = lang == null ? "" : lang.trim();
            this.region = region == null ? "" : region.trim();
        }
    }

    public static final class NewsItemRecord {
        public final long id;
        public final String url;
        public final String title;
        public final String content;
        public final String source;
        public final String lang;
        public final String region;
        public final OffsetDateTime publishedAt;
        public final float[] embedding;
        public final double similarity;

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
