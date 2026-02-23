package com.stockbot.jp.output;

import com.stockbot.core.diagnostics.Diagnostics;
import com.stockbot.jp.config.Config;
import com.stockbot.jp.model.ScanFailureReason;
import com.stockbot.jp.model.ScanResultSummary;
import com.stockbot.jp.model.ScoredCandidate;
import com.stockbot.jp.model.WatchlistAnalysis;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class ReportBuilder {
    private static final DateTimeFormatter FILE_TS = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    private static final DateTimeFormatter DISPLAY_TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final LocalTime CLOSE_TIME = LocalTime.of(15, 0);

    private final Config config;
    private final ThymeleafReportRenderer renderer;
    private final HtmlPostProcessor htmlPostProcessor;
    private final I18n i18n;

    public ReportBuilder(Config config) {
        this.config = config;
        this.renderer = new ThymeleafReportRenderer();
        this.htmlPostProcessor = new HtmlPostProcessor();
        this.i18n = new I18n(config);
    }

    public enum RunType {
        INTRADAY,
        CLOSE
    }

    public static RunType detectRunType(Instant startedAt, ZoneId zoneId) {
        if (startedAt == null || zoneId == null) {
            return RunType.CLOSE;
        }
        return startedAt.atZone(zoneId).toLocalTime().isBefore(CLOSE_TIME) ? RunType.INTRADAY : RunType.CLOSE;
    }

    public Path writeDailyReport(
            Path reportDir,
            Instant startedAt,
            ZoneId zoneId,
            int universeSize,
            int scannedSize,
            int candidateSize,
            int topN,
            List<String> watchlist,
            List<WatchlistAnalysis> watchlistCandidates,
            List<ScoredCandidate> marketReferenceCandidates,
            double minScore,
            RunType runType,
            Map<String, Double> previousScoreByTicker,
            ScanResultSummary scanSummary,
            boolean marketScanPartial,
            String marketScanStatus,
            Diagnostics diagnostics
    ) throws Exception {
        Files.createDirectories(reportDir);
        Path report = reportDir.resolve("jp_daily_" + FILE_TS.format(startedAt.atZone(zoneId)) + ".html");
        String html = buildHtml(
                startedAt,
                zoneId,
                universeSize,
                scannedSize,
                candidateSize,
                topN,
                watchlistCandidates,
                marketReferenceCandidates,
                runType,
                scanSummary,
                marketScanPartial,
                marketScanStatus,
                diagnostics
        );
        Files.writeString(report, html, StandardCharsets.UTF_8);
        Files.writeString(reportDir.resolve("email_main.html"), html, StandardCharsets.UTF_8);

        JSONObject debug = new JSONObject();
        debug.put("generated_at", DISPLAY_TS.format(startedAt.atZone(zoneId)));
        debug.put("watchlist_count", watchlistCandidates == null ? 0 : watchlistCandidates.size());
        debug.put("top_cards_count", marketReferenceCandidates == null ? 0 : marketReferenceCandidates.size());
        Files.writeString(reportDir.resolve("report_debug.json"), debug.toString(2), StandardCharsets.UTF_8);
        return report;
    }

    public String buildNoviceActionSummary(double indicatorCoveragePct, RunType runType, List<WatchlistAnalysis> watchRows) {
        List<WatchlistAnalysis> rows = sortWatch(watchRows);
        StringBuilder sb = new StringBuilder();
        sb.append(i18n.t("report.novice.conclusion", "结论：")).append(" ");
        sb.append(indicatorCoveragePct < 50.0
                ? i18n.t("report.novice.coverage_low", "覆盖率偏低，建议降低风险。")
                : i18n.t("report.novice.coverage_ok", "覆盖率可接受。"));
        sb.append("\n")
                .append(i18n.t("report.novice.run_type", "运行类型："))
                .append(runTypeLabel(runType));
        int used = 0;
        for (WatchlistAnalysis row : rows) {
            if (row == null) {
                continue;
            }
            String action = watchAction(row);
            if ("WAIT".equals(action)) {
                continue;
            }
            sb.append("\n[").append(watchActionLabel(action)).append("] ")
                    .append(blankTo(row.displayName, row.ticker))
                    .append(" score=").append(fmt1(row.technicalScore));
            used++;
            if (used >= 5) {
                break;
            }
        }
        if (used == 0) {
            sb.append("\n").append(i18n.t("report.novice.no_action", "暂无操作建议。"));
        }
        return sb.toString();
    }

    public String buildHtml(
            Instant startedAt,
            ZoneId zoneId,
            int universeSize,
            int scannedSize,
            int candidateSize,
            int topN,
            List<WatchlistAnalysis> watchlistCandidates,
            List<ScoredCandidate> marketReferenceCandidates,
            RunType runType,
            ScanResultSummary scanSummary,
            boolean marketScanPartial,
            String marketScanStatus,
            Diagnostics diagnostics
    ) {
        ScanResultSummary summary = scanSummary == null
                ? new ScanResultSummary(0, 0, 0, Map.of(), Map.of(), Map.of())
                : scanSummary;
        List<WatchlistAnalysis> watchRows = sortWatch(watchlistCandidates);
        List<ScoredCandidate> topCards = sortMarket(marketReferenceCandidates);
        if (topCards.size() > 5) {
            topCards = new ArrayList<>(topCards.subList(0, 5));
        }

        Map<String, Object> view = new HashMap<>();
        view.put("pageTitle", i18n.t("report.page.title", "StockBot 日股每日报告"));
        view.put("reportTitle", i18n.t("report.title", "StockBot 日报"));
        view.put("topTiles", List.of(
                kv("key", i18n.t("report.tile.run_time", "运行时间（JST）"), "value", DISPLAY_TS.format(startedAt.atZone(zoneId))),
                kv("key", i18n.t("report.tile.fetch_coverage", "抓取覆盖率"), "value", scannedSize + " / " + Math.max(1, universeSize)),
                kv("key", i18n.t("report.tile.candidates", "候选数"), "value", Integer.toString(candidateSize)),
                kv("key", i18n.t("report.tile.top_n", "TopN"), "value", Integer.toString(topN)),
                kv("key", i18n.t("report.tile.run_type", "运行类型"), "value", runTypeLabel(runType))
        ));
        view.put("failureStats", List.of(
                i18n.t("report.failure.timeout", "超时") + ": " + summary.requestFailureCount(ScanFailureReason.TIMEOUT),
                i18n.t("report.failure.parse_error", "解析错误") + ": " + summary.requestFailureCount(ScanFailureReason.PARSE_ERROR),
                i18n.t("report.failure.stale", "过期数据") + ": " + summary.failureCount(ScanFailureReason.STALE),
                i18n.t("report.failure.other", "其他") + ": " + summary.failureCount(ScanFailureReason.OTHER)
        ));
        view.put("hasSuspectPrice", watchRows.stream().anyMatch(r -> r != null && r.priceSuspect));
        view.put("suspectTickers", suspectTickers(watchRows));
        String unknownText = i18n.t("report.coverage.unknown", "未知");
        view.put("coverageSource", diagnostics == null ? unknownText : blankTo(diagnostics.coverageSource, unknownText));
        view.put("partialMessage", (marketScanPartial || "PARTIAL".equalsIgnoreCase(blankTo(marketScanStatus, "")))
                ? i18n.t("report.partial.market_scan", "市场扫描结果为部分完成。")
                : "");
        view.put("systemStatusLines", List.of(
                i18n.t("report.system.indicator_core", "指标核心") + "=" + config.getString("indicator.core", "sma20,sma60,rsi14,atr14"),
                i18n.t("report.system.min_score", "扫描最低分") + "=" + config.getString("scan.min_score", "55"),
                i18n.t("report.system.min_signals", "最少信号数") + "=" + config.getString("filter.min_signals", "3")
        ));
        view.put("noviceLines", List.of(
                i18n.t("report.novice.line1", "优先关注最强标的。"),
                i18n.t("report.novice.line2", "覆盖率偏低时请控制仓位。")
        ));
        view.put("actionAdvice", kv(
                "css", "info",
                "level", i18n.t("report.action.level.check", "检查"),
                "reason", i18n.t("report.action.reason", "请结合自选与 Top5 结果综合判断。"),
                "singleMaxPct", fmt1(config.getDouble("report.position.max_single_pct", 5.0)),
                "totalMaxPct", fmt1(config.getDouble("report.position.max_total_pct", 50.0))
        ));

        List<Map<String, Object>> watchTable = new ArrayList<>();
        List<Map<String, Object>> aiItems = new ArrayList<>();
        for (WatchlistAnalysis row : watchRows) {
            if (row == null) {
                continue;
            }
            String action = watchAction(row);
            watchTable.add(kv(
                    "priceSuspect", row.priceSuspect,
                    "name", blankTo(row.displayName, row.ticker),
                    "traceSummary", i18n.t("report.trace.source", "来源") + "=" + blankTo(row.dataSource, "-")
                            + " | " + i18n.t("report.trace.ts", "时间") + "=" + blankTo(row.priceTimestamp, "-"),
                    "lastClose", fmt2(row.lastClose),
                    "action", watchActionLabel(action),
                    "actionCss", watchActionCss(action),
                    "reasons", List.of(blankTo(row.gateReason, i18n.t("report.common.na", "无"))),
                    "entryRange", "-",
                    "stopLoss", "-",
                    "takeProfit", "-"
            ));
            aiItems.add(kv(
                    "name", blankTo(row.displayName, row.ticker),
                    "lines", splitLines(blankTo(row.aiSummary, "")),
                    "newsDigests", row.newsDigests == null ? List.of() : row.newsDigests
            ));
        }
        view.put("watchRows", watchTable);
        view.put("watchAiItems", aiItems);

        List<Map<String, Object>> cardRows = new ArrayList<>();
        int rank = 1;
        for (ScoredCandidate candidate : topCards) {
            if (candidate == null) {
                continue;
            }
            cardRows.add(kv(
                    "rank", rank++,
                    "name", blankTo(candidate.code, candidate.ticker) + " " + blankTo(candidate.name, ""),
                    "latestPrice", fmt2(candidate.close),
                    "score", fmt1(candidate.score),
                    "riskCss", candidate.score >= 70 ? "good" : "warn",
                    "riskText", candidate.score >= 70
                            ? i18n.t("report.card.risk.low", "低")
                            : i18n.t("report.card.risk.mid", "中"),
                    "planLine", i18n.t("report.card.plan", "执行前请先复核图形与风险。"),
                    "reasons", List.of("score=" + fmt1(candidate.score))
            ));
        }
        view.put("topCards", kv(
                "funnelStats", List.of(i18n.t("report.topcards.funnel_from_market", "市场扫描候选数")
                        + "=" + (marketReferenceCandidates == null ? 0 : marketReferenceCandidates.size())),
                "mainReasons", List.of(),
                "skipReason", "",
                "gateLines", List.of(),
                "noviceWarn", false,
                "emptyMessage", cardRows.isEmpty() ? i18n.t("report.topcards.empty", "暂无 Top5 候选。") : "",
                "cards", cardRows,
                "excludedDerivatives", ""
        ));

        view.put("disclaimerLines", List.of(
                i18n.t("report.disclaimer.1", "本报告仅用于决策辅助，不构成投资建议。"),
                i18n.t("report.disclaimer.2", "下单前请再次核验价格与风险。")
        ));
        view.put("hasConfigSnapshot", false);
        view.put("configRows", List.of());
        view.put("diagnostics", kv("enabled", diagnostics != null, "runId", diagnostics == null ? 0L : diagnostics.runId, "runMode", diagnostics == null ? "" : diagnostics.runMode, "coverageScope", diagnostics == null ? "" : diagnostics.coverageScope, "coverageSource", diagnostics == null ? "" : diagnostics.coverageSource, "coverageOwner", diagnostics == null ? "" : diagnostics.coverageOwner, "coverageMetrics", List.of(), "dataSources", List.of(), "featureStatuses", List.of(), "configSnapshot", List.of(), "notes", diagnostics == null ? List.of() : diagnostics.notes));

        String rendered = renderer.renderDailyReport(Map.of("view", view));
        return htmlPostProcessor.cleanDocument(rendered);
    }

    private List<WatchlistAnalysis> sortWatch(List<WatchlistAnalysis> rows) {
        List<WatchlistAnalysis> out = new ArrayList<>();
        if (rows != null) {
            out.addAll(rows);
        }
        out.sort(Comparator.comparingDouble((WatchlistAnalysis r) -> r == null ? Double.NEGATIVE_INFINITY : r.totalScore).reversed());
        return out;
    }

    private List<ScoredCandidate> sortMarket(List<ScoredCandidate> rows) {
        List<ScoredCandidate> out = new ArrayList<>();
        if (rows != null) {
            out.addAll(rows);
        }
        out.sort(Comparator.comparingDouble((ScoredCandidate r) -> r == null ? Double.NEGATIVE_INFINITY : r.score).reversed());
        return out;
    }

    private String watchAction(WatchlistAnalysis row) {
        if (row == null) {
            return "WAIT";
        }
        String status = blankTo(row.technicalStatus, "").toUpperCase(Locale.ROOT);
        if ("CANDIDATE".equals(status) && row.technicalScore >= 80.0) return "BUY";
        if ("CANDIDATE".equals(status)) return "WATCH";
        if ("RISK".equals(status)) return "AVOID";
        return "WAIT";
    }

    private String watchActionLabel(String action) {
        if ("BUY".equals(action)) return i18n.t("report.action.buy", "买入");
        if ("WATCH".equals(action)) return i18n.t("report.action.watch", "关注");
        if ("AVOID".equals(action)) return i18n.t("report.action.avoid", "回避");
        return i18n.t("report.action.wait", "观望");
    }

    private String runTypeLabel(RunType runType) {
        if (runType == RunType.CLOSE) {
            return i18n.t("report.run_type.close", "收盘后");
        }
        return i18n.t("report.run_type.intraday", "盘中");
    }

    private String watchActionCss(String action) {
        if ("BUY".equals(action)) return "good";
        if ("WATCH".equals(action)) return "warn";
        if ("AVOID".equals(action)) return "bad";
        return "gray";
    }

    private String suspectTickers(List<WatchlistAnalysis> rows) {
        List<String> out = new ArrayList<>();
        for (WatchlistAnalysis row : rows) {
            if (row != null && row.priceSuspect) out.add(blankTo(row.ticker, row.code));
        }
        return String.join(",", out);
    }

    private List<String> splitLines(String text) {
        if (text == null || text.trim().isEmpty()) return List.of(i18n.t("report.ai.none", "暂无 AI 摘要。"));
        List<String> out = new ArrayList<>();
        for (String line : text.split("\\r?\\n")) {
            String t = line.trim();
            if (!t.isEmpty()) out.add(t);
        }
        return out.isEmpty() ? List.of(text.trim()) : out;
    }

    private String fmt1(double value) {
        return String.format(Locale.US, "%.1f", value);
    }

    private String fmt2(double value) {
        return Double.isFinite(value) ? String.format(Locale.US, "%.2f", value) : "-";
    }

    private String blankTo(String value, String fallback) {
        String text = value == null ? "" : value.trim();
        return text.isEmpty() ? fallback : text;
    }

    private Map<String, Object> kv(Object... values) {
        Map<String, Object> out = new HashMap<>();
        for (int i = 0; i + 1 < values.length; i += 2) out.put(String.valueOf(values[i]), values[i + 1]);
        return out;
    }
}
