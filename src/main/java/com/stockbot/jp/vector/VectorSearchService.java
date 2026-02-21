package com.stockbot.jp.vector;

import com.stockbot.jp.db.Database;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Minimal pgvector-backed document search service.
 */
public final class VectorSearchService {
    private final Database database;

    public VectorSearchService(Database database) {
        this.database = database;
    }

    public String upsertDoc(Doc doc) throws SQLException {
        if (doc == null || isBlank(doc.docType) || isBlank(doc.content)) {
            throw new IllegalArgumentException("doc_type and content are required");
        }

        String contentHash = isBlank(doc.contentHash) ? sha256(doc.content) : doc.contentHash.trim();
        String embeddingLiteral = toVectorLiteral(doc.embedding);

        String sql = "INSERT INTO docs(doc_type, ticker, title, content, lang, source, published_at, content_hash, embedding) " +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, CASE WHEN ? IS NULL THEN NULL ELSE CAST(? AS vector) END) " +
                "ON CONFLICT(content_hash) DO UPDATE SET " +
                "doc_type=excluded.doc_type, ticker=excluded.ticker, title=excluded.title, content=excluded.content, " +
                "lang=excluded.lang, source=excluded.source, published_at=excluded.published_at, " +
                "embedding=COALESCE(excluded.embedding, docs.embedding)";

        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, doc.docType);
            ps.setString(2, trimToNull(doc.ticker));
            ps.setString(3, trimToNull(doc.title));
            ps.setString(4, doc.content);
            ps.setString(5, trimToNull(doc.lang));
            ps.setString(6, trimToNull(doc.source));
            ps.setObject(7, toOffsetDateTime(doc.publishedAt));
            ps.setString(8, contentHash);
            ps.setString(9, embeddingLiteral);
            ps.setString(10, embeddingLiteral);
            ps.executeUpdate();
        }

        return contentHash;
    }

    public List<DocMatch> searchSimilar(String text, int topK, SearchFilters filters) throws SQLException {
        return searchSimilar(text, null, topK, filters);
    }

    public List<DocMatch> searchSimilar(String text, float[] queryEmbedding, int topK, SearchFilters filters) throws SQLException {
        int limit = Math.max(1, topK);
        if (queryEmbedding != null && queryEmbedding.length > 0) {
            return searchByVector(queryEmbedding, limit, filters);
        }
        return searchByText(text, limit, filters);
    }

    private List<DocMatch> searchByVector(float[] queryEmbedding, int topK, SearchFilters filters) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT id, doc_type, ticker, title, content, lang, source, published_at, content_hash, ")
                .append("embedding <=> CAST(? AS vector) AS distance ")
                .append("FROM docs WHERE embedding IS NOT NULL");

        List<Object> params = new ArrayList<>();
        params.add(toVectorLiteral(queryEmbedding));
        appendFilters(sql, params, filters);
        sql.append(" ORDER BY embedding <=> CAST(? AS vector) LIMIT ?");
        params.add(toVectorLiteral(queryEmbedding));
        params.add(topK);

        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            bind(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                return toMatches(rs);
            }
        }
    }

    private List<DocMatch> searchByText(String text, int topK, SearchFilters filters) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT id, doc_type, ticker, title, content, lang, source, published_at, content_hash, NULL::double precision AS distance ")
                .append("FROM docs WHERE 1=1");

        List<Object> params = new ArrayList<>();
        appendFilters(sql, params, filters);

        String keyword = trimToNull(text);
        if (keyword != null) {
            sql.append(" AND (title ILIKE ? OR content ILIKE ?)");
            String like = "%" + keyword + "%";
            params.add(like);
            params.add(like);
        }

        sql.append(" ORDER BY published_at DESC NULLS LAST, created_at DESC LIMIT ?");
        params.add(topK);

        try (Connection conn = database.connect();
             PreparedStatement ps = conn.prepareStatement(sql.toString())) {
            bind(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                return toMatches(rs);
            }
        }
    }

    private void appendFilters(StringBuilder sql, List<Object> params, SearchFilters filters) {
        if (filters == null) {
            return;
        }
        if (!isBlank(filters.docType)) {
            sql.append(" AND doc_type = ?");
            params.add(filters.docType.trim().toUpperCase(Locale.ROOT));
        }
        if (!isBlank(filters.ticker)) {
            sql.append(" AND ticker = ?");
            params.add(filters.ticker.trim());
        }
        if (!isBlank(filters.lang)) {
            sql.append(" AND lang = ?");
            params.add(filters.lang.trim());
        }
    }

    private List<DocMatch> toMatches(ResultSet rs) throws SQLException {
        List<DocMatch> out = new ArrayList<>();
        while (rs.next()) {
            Timestamp ts = rs.getTimestamp("published_at");
            out.add(new DocMatch(
                    rs.getLong("id"),
                    rs.getString("doc_type"),
                    rs.getString("ticker"),
                    rs.getString("title"),
                    rs.getString("content"),
                    rs.getString("lang"),
                    rs.getString("source"),
                    ts == null ? null : ts.toInstant(),
                    rs.getString("content_hash"),
                    rs.getObject("distance") == null ? null : rs.getDouble("distance")
            ));
        }
        return out;
    }

    private void bind(PreparedStatement ps, List<Object> params) throws SQLException {
        for (int i = 0; i < params.size(); i++) {
            ps.setObject(i + 1, params.get(i));
        }
    }

    private OffsetDateTime toOffsetDateTime(Instant instant) {
        return instant == null ? null : instant.atOffset(ZoneOffset.UTC);
    }

    private String toVectorLiteral(float[] embedding) {
        if (embedding == null || embedding.length == 0) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < embedding.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(Float.toString(embedding[i]));
        }
        sb.append(']');
        return sb.toString();
    }

    private String sha256(String content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] out = digest.digest(content.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(out.length * 2);
            for (byte b : out) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException("failed to compute content hash", e);
        }
    }

    private String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String out = value.trim();
        return out.isEmpty() ? null : out;
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    public static final class Doc {
        public final String docType;
        public final String ticker;
        public final String title;
        public final String content;
        public final String lang;
        public final String source;
        public final Instant publishedAt;
        public final String contentHash;
        public final float[] embedding;

        public Doc(
                String docType,
                String ticker,
                String title,
                String content,
                String lang,
                String source,
                Instant publishedAt,
                String contentHash,
                float[] embedding
        ) {
            this.docType = docType;
            this.ticker = ticker;
            this.title = title;
            this.content = content;
            this.lang = lang;
            this.source = source;
            this.publishedAt = publishedAt;
            this.contentHash = contentHash;
            this.embedding = embedding;
        }
    }

    public static final class SearchFilters {
        public final String docType;
        public final String ticker;
        public final String lang;

        public SearchFilters(String docType, String ticker, String lang) {
            this.docType = docType;
            this.ticker = ticker;
            this.lang = lang;
        }
    }

    public static final class DocMatch {
        public final long id;
        public final String docType;
        public final String ticker;
        public final String title;
        public final String content;
        public final String lang;
        public final String source;
        public final Instant publishedAt;
        public final String contentHash;
        public final Double distance;

        public DocMatch(
                long id,
                String docType,
                String ticker,
                String title,
                String content,
                String lang,
                String source,
                Instant publishedAt,
                String contentHash,
                Double distance
        ) {
            this.id = id;
            this.docType = docType;
            this.ticker = ticker;
            this.title = title;
            this.content = content;
            this.lang = lang;
            this.source = source;
            this.publishedAt = publishedAt;
            this.contentHash = contentHash;
            this.distance = distance;
        }
    }
}
