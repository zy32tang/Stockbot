package com.stockbot.model;

import java.time.ZonedDateTime;

public class NewsItem {
    public final String title;
    public final String link;
    public final String source;
    public final ZonedDateTime publishedAt;

    public NewsItem(String title, String link, String source, ZonedDateTime publishedAt) {
        this.title = title;
        this.link = link;
        this.source = source;
        this.publishedAt = publishedAt;
    }
}
