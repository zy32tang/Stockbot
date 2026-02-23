package com.stockbot.jp.db.mybatis;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NewsItemSearchRow {
    private long id;
    private String url;
    private String title;
    private String content;
    private String source;
    private String lang;
    private String region;
    private OffsetDateTime publishedAt;
    private String embeddingText;
    private Double similarity;
}
