package com.stockbot.jp.output;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Final HTML cleanup and extraction helpers based on Jsoup.
 */
public final class HtmlPostProcessor {

    public String cleanDocument(String html) {
        return cleanDocument(html, false);
    }

    public String cleanDocument(String html, boolean removeTrendImages) {
        if (html == null || html.isBlank()) {
            return "";
        }
        Document doc = Jsoup.parse(html);
        doc.outputSettings().charset(StandardCharsets.UTF_8);
        doc.outputSettings().prettyPrint(false);
        doc.outputSettings().syntax(Document.OutputSettings.Syntax.html);
        doc.select("script,noscript,iframe,object,embed").remove();
        doc.select("meta[http-equiv=refresh]").remove();

        if (removeTrendImages) {
            doc.select("img[src^=trends/]").remove();
            removeTrendLabels(doc);
        }
        return doc.outerHtml();
    }

    public List<String> localImageSources(String html) {
        if (html == null || html.isBlank()) {
            return List.of();
        }
        Document doc = Jsoup.parse(html);
        Set<String> out = new LinkedHashSet<>();
        for (Element img : doc.select("img[src]")) {
            String src = img.attr("src") == null ? "" : img.attr("src").trim();
            if (src.isEmpty()) {
                continue;
            }
            String lower = src.toLowerCase(Locale.ROOT);
            if (lower.startsWith("data:")
                    || lower.startsWith("cid:")
                    || lower.startsWith("http://")
                    || lower.startsWith("https://")) {
                continue;
            }
            out.add(src);
        }
        return new ArrayList<>(out);
    }

    private void removeTrendLabels(Document doc) {
        for (Element div : doc.select("div:has(> b)")) {
            if (isTrendLabel(div)) {
                div.remove();
            }
        }
    }

    private boolean isTrendLabel(Element element) {
        if (element == null) {
            return false;
        }
        String text = element.text();
        if (text == null) {
            return false;
        }
        String normalized = text.replace('：', ':').trim().toLowerCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            return false;
        }
        return normalized.contains("trend") && normalized.endsWith(":");
    }
}
