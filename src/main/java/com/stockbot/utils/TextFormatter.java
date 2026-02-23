package com.stockbot.utils;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.util.regex.Pattern;

/**
 * 模块说明：TextFormatter（class）。
 * 主要职责：承载 utils 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public class TextFormatter {
    private static final Pattern MD_MARKS = Pattern.compile("(?m)^\\s*(#{1,6}\\s+|\\*\\*|\\*|`{1,3}|-\\s+|>\\s+)");
    private static final Pattern MD_STRONG_MARKS = Pattern.compile("(\\*\\*|__)");
    private static final Pattern MULTI_BLANK = Pattern.compile("\n{3,}");
    private static final Pattern LINEBREAK_IN_PARA = Pattern.compile("(?<!\n)\n(?!\n)");
    private static final Pattern CONTROL_CHARS = Pattern.compile("[\\p{Cntrl}&&[^\n\t]]");

    // 规则：去掉标记语法符号，保留段落分隔，但将段内换行合并为空格
    public static String cleanForEmail(String s) {
        if (s == null) return "";
        String t = s.replace("\r\n", "\n").replace("\r", "\n");
        t = MD_MARKS.matcher(t).replaceAll("");
        // 删除行内可能出现的强调标记，例如 "**text**" 或末尾的 "**"
        t = MD_STRONG_MARKS.matcher(t).replaceAll("");
        // 将段落内的单个换行合并为空格
        t = LINEBREAK_IN_PARA.matcher(t).replaceAll(" ");
        // 规范空行：段落之间最多保留 1 个空行
        t = MULTI_BLANK.matcher(t).replaceAll("\n\n");
        // 去除行尾空白
        t = t.replaceAll("[ \t]+\n", "\n").trim();
        return t;
    }

    public static String toPlainText(String s) {
        if (s == null) {
            return "";
        }
        String t = s.replace("\r\n", "\n").replace("\r", "\n").replace("\\n", "\n");
        Document doc = Jsoup.parse(t);
        doc.outputSettings().prettyPrint(false);
        doc.select("br").append("\\n");
        doc.select("p,div,li,ul,ol,h1,h2,h3,h4,h5,h6").prepend("\\n");
        t = doc.text().replace("\\n", "\n");
        t = cleanForEmail(t);
        t = CONTROL_CHARS.matcher(t).replaceAll("");
        return t.trim();
    }
}
