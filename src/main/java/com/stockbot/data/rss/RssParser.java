package com.stockbot.data.rss;

import com.stockbot.model.NewsItem;
import org.w3c.dom.*;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class RssParser {

    public static List<NewsItem> parse(String xml, int maxItems) {
        List<NewsItem> out = new ArrayList<>();
        try {
            Document doc = DocumentBuilderFactory.newInstance()
                    .newDocumentBuilder()
                    .parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
            NodeList items = doc.getElementsByTagName("item");
            for (int i = 0; i < items.getLength() && out.size() < maxItems; i++) {
                Element item = (Element) items.item(i);
                String title = text(item, "title");
                String link = text(item, "link");
                String pubDate = text(item, "pubDate");
                ZonedDateTime zdt = null;
                try {
                    zdt = ZonedDateTime.parse(pubDate, DateTimeFormatter.RFC_1123_DATE_TIME);
                } catch (Exception ignored) {}
                String source = sourceText(item, title, link);
                out.add(new NewsItem(title == null ? "" : title.trim(),
                        link == null ? "" : link.trim(),
                        source,
                        zdt));
            }
        } catch (Exception e) {
            // 忽略解析异常并返回空结果
        }
        return out;
    }

    private static String text(Element parent, String tag) {
        NodeList nl = parent.getElementsByTagName(tag);
        if (nl.getLength() == 0) return null;
        Node n = nl.item(0);
        return n == null ? null : n.getTextContent();
    }

    private static String sourceText(Element item, String title, String link) {
        String source = text(item, "source");
        if (source != null && !source.trim().isEmpty()) return source.trim();

        // 聚合来源常将媒体名拼在标题里，格式通常是“标题 - 来源”
        if (title != null && title.contains(" - ")) {
            String[] parts = title.split(" - ");
            if (parts.length >= 2) {
                String guessed = parts[parts.length - 1].trim();
                if (!guessed.isEmpty()) return guessed;
            }
        }

        // 最后兜底：使用链接域名
        try {
            if (link != null && !link.trim().isEmpty()) {
                URI u = URI.create(link.trim());
                String host = u.getHost();
                if (host != null && !host.trim().isEmpty()) return host.trim();
            }
        } catch (Exception ignored) {}
        return "";
    }
}
