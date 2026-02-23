package com.stockbot.jp.output;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.TextNode;

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
        cleanupMarkdownArtifacts(doc);
        normalizeBlockText(doc);

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

    private void cleanupMarkdownArtifacts(Document doc) {
        if (doc == null) {
            return;
        }
        for (Element el : doc.getAllElements()) {
            for (TextNode node : el.textNodes()) {
                String cleaned = stripMarkdown(node.text());
                if (!cleaned.equals(node.text())) {
                    node.text(cleaned);
                }
            }
        }
    }

    private void normalizeBlockText(Document doc) {
        if (doc == null) {
            return;
        }
        for (Element block : doc.select("p,li,div,td,th")) {
            for (TextNode node : block.textNodes()) {
                String normalized = normalizeInline(node.text());
                if (!normalized.equals(node.text())) {
                    node.text(normalized);
                }
            }
        }
    }

    private String stripMarkdown(String text) {
        if (text == null || text.isEmpty()) {
            return "";
        }
        String cleaned = text.replace("```", " ")
                .replace("###", " ")
                .replace("**", " ")
                .replace("__", " ")
                .replaceAll("(?m)^\\s{0,3}[-*]\\s+", "");
        return normalizeInline(cleaned);
    }

    private String normalizeInline(String text) {
        if (text == null || text.isEmpty()) {
            return "";
        }
        return text.replace("\r", " ")
                .replace("\n", " ")
                .replace('\u3000', ' ')
                .replaceAll("\\s+", " ")
                .trim();
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
