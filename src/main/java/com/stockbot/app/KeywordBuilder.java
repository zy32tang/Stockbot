package com.stockbot.app;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 模块说明：KeywordBuilder（class）。
 * 主要职责：承载 app 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public class KeywordBuilder {

    private static final Map<String, List<String>> ALIAS = new LinkedHashMap<>();
    private static final List<String> THEME_FINANCIAL = List.of(
            "earnings",
            "revenue",
            "guidance",
            "outlook",
            "forecast",
            "quarter results"
    );
    private static final List<String> THEME_EVENT = List.of(
            "dividend",
            "buyback",
            "merger",
            "acquisition",
            "partnership",
            "spinoff"
    );
    private static final List<String> THEME_RISK = List.of(
            "lawsuit",
            "regulation",
            "investigation",
            "downgrade",
            "supply chain risk"
    );
    private static final List<String> THEME_INDUSTRY = List.of(
            "demand",
            "orders",
            "capacity",
            "production",
            "capex"
    );
    private static final List<String> THEME_JP_LOCAL = List.of(
            "決算",
            "業績",
            "株価",
            "見通し",
            "受注"
    );
    private static final List<String> THEME_CN_LOCAL = List.of(
            "财报",
            "业绩",
            "公告",
            "股价",
            "并购",
            "分红"
    );

    static {
        ALIAS.put("7974.T", List.of("任天堂", "Nintendo"));
        ALIAS.put("8035.T", List.of("東京エレクトロン", "Tokyo Electron"));
    }

/**
 * 方法说明：buildQuery，负责构建目标对象或输出内容。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static String buildQuery(String ticker) {
        List<String> q = buildQueries(ticker, 3);
        if (q.isEmpty()) return ticker == null ? "" : ticker;
        if (q.size() == 1) return q.get(0);
        return String.join(" OR ", q);
    }

/**
 * 方法说明：buildQueries，负责构建目标对象或输出内容。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static List<String> buildQueries(String ticker, int maxQueries) {
        return buildQueries(ticker, maxQueries, "");
    }

/**
 * 方法说明：buildQueries，负责构建目标对象或输出内容。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static List<String> buildQueries(String ticker, int maxQueries, String extraTermsCsv) {
        String t = normalizeTicker(ticker);
        if (t.isEmpty()) return List.of();

        String market = marketOf(t);
        String code = numericCode(t);

        List<String> aliases = ALIAS.getOrDefault(t, List.of());
        List<String> anchors = buildAnchors(t, code, aliases);
        List<String> themes = buildThemes(market, extraTermsCsv);

        Set<String> out = new LinkedHashSet<>();
        for (String anchor : anchors) {
            out.add(anchor);
        }

        for (String theme : themes) {
            for (String anchor : anchors) {
                out.add(anchor + " " + theme);
            }
        }

        int limit = Math.max(1, maxQueries);
        List<String> list = new ArrayList<>();
        for (String q : out) {
            if (q == null) continue;
            String s = q.trim();
            if (s.isEmpty()) continue;
            list.add(s);
            if (list.size() >= limit) break;
        }
        return list;
    }

/**
 * 方法说明：buildAnchors，负责构建目标对象或输出内容。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static List<String> buildAnchors(String ticker, String code, List<String> aliases) {
        Set<String> out = new LinkedHashSet<>();
        addPhrase(out, ticker);
        if (!code.isEmpty()) addPhrase(out, code);
        if (!code.isEmpty()) addPhrase(out, code + " " + ticker);

        for (String aliasRaw : aliases) {
            String alias = normalizePhrase(aliasRaw);
            if (alias.isEmpty()) continue;
            addPhrase(out, alias);
            addPhrase(out, alias + " " + ticker);
            if (!code.isEmpty()) addPhrase(out, alias + " " + code);
        }
        return new ArrayList<>(out);
    }

/**
 * 方法说明：buildThemes，负责构建目标对象或输出内容。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static List<String> buildThemes(String market, String extraTermsCsv) {
        Set<String> out = new LinkedHashSet<>();
        out.addAll(THEME_FINANCIAL);
        out.addAll(THEME_EVENT);
        out.addAll(THEME_RISK);
        out.addAll(THEME_INDUSTRY);

        if ("JP".equals(market)) {
            out.addAll(THEME_JP_LOCAL);
        } else if ("CN".equals(market)) {
            out.addAll(THEME_CN_LOCAL);
        }

        if (extraTermsCsv != null && !extraTermsCsv.trim().isEmpty()) {
            for (String raw : extraTermsCsv.split("[,;\\n\\r]")) {
                addPhrase(out, raw);
            }
        }

        return new ArrayList<>(out);
    }

/**
 * 方法说明：addPhrase，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static void addPhrase(Set<String> out, String raw) {
        String s = normalizePhrase(raw);
        if (!s.isEmpty()) out.add(s);
    }

/**
 * 方法说明：normalizePhrase，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static String normalizePhrase(String raw) {
        if (raw == null) return "";
        String trimmed = raw.trim();
        if (trimmed.isEmpty()) return "";
        return trimmed.replaceAll("\\s+", " ");
    }

/**
 * 方法说明：normalizeTicker，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static String normalizeTicker(String ticker) {
        if (ticker == null) return "";
        return ticker.trim().toUpperCase(Locale.ROOT);
    }

/**
 * 方法说明：numericCode，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static String numericCode(String ticker) {
        if (ticker == null) return "";
        int dot = ticker.indexOf('.');
        if (dot <= 0) return "";
        String head = ticker.substring(0, dot);
        if (head.matches("\\d{4,6}")) return head;
        return "";
    }

/**
 * 方法说明：marketOf，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static String marketOf(String ticker) {
        if (ticker == null) return "US";
        if (ticker.endsWith(".T")) return "JP";
        if (ticker.endsWith(".SS") || ticker.endsWith(".SZ") || ticker.endsWith(".HK")) return "CN";
        return "US";
    }
}
