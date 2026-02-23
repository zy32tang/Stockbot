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

    public ReportBuilder(Config config) {
        this.config = config;
        this.renderer = new ThymeleafReportRenderer();
        this.htmlPostProcessor = new HtmlPostProcessor();
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
        sb.append("Conclusion: ");
        sb.append(indicatorCoveragePct < 50.0 ? "coverage low, reduce risk." : "coverage acceptable.");
        sb.append("\nRun type: ").append(runType == RunType.CLOSE ? "CLOSE" : "INTRADAY");
        int used = 0;
        for (WatchlistAnalysis row : rows) {
            if (row == null) {
                continue;
            }
            String action = watchAction(row);
            if ("WAIT".equals(action)) {
                continue;
            }
            sb.append("\n[").append(action).append("] ")
                    .append(blankTo(row.displayName, row.ticker))
                    .append(" score=").append(fmt1(row.technicalScore));
            used++;
            if (used >= 5) {
                break;
            }
        }
        if (used == 0) {
            sb.append("\nNo action.");
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
        view.put("pageTitle", "StockBot JP Daily Report");
        view.put("reportTitle", "StockBot Daily Report");
        view.put("topTiles", List.of(
                kv("key", "Run Time (JST)", "value", DISPLAY_TS.format(startedAt.atZone(zoneId))),
                kv("key", "Fetch Coverage", "value", scannedSize + " / " + Math.max(1, universeSize)),
                kv("key", "Candidates", "value", Integer.toString(candidateSize)),
                kv("key", "TopN", "value", Integer.toString(topN)),
                kv("key", "Run Type", "value", runType == RunType.CLOSE ? "CLOSE" : "INTRADAY")
        ));
        view.put("failureStats", List.of(
                "timeout: " + summary.requestFailureCount(ScanFailureReason.TIMEOUT),
                "parse_error: " + summary.requestFailureCount(ScanFailureReason.PARSE_ERROR),
                "stale: " + summary.failureCount(ScanFailureReason.STALE),
                "other: " + summary.failureCount(ScanFailureReason.OTHER)
        ));
        view.put("hasSuspectPrice", watchRows.stream().anyMatch(r -> r != null && r.priceSuspect));
        view.put("suspectTickers", suspectTickers(watchRows));
        view.put("coverageSource", diagnostics == null ? "UNKNOWN" : blankTo(diagnostics.coverageSource, "UNKNOWN"));
        view.put("partialMessage", (marketScanPartial || "PARTIAL".equalsIgnoreCase(blankTo(marketScanStatus, ""))) ? "Market scan is partial." : "");
        view.put("systemStatusLines", List.of(
                "indicator.core=" + config.getString("indicator.core", "sma20,sma60,rsi14,atr14"),
                "scan.min_score=" + config.getString("scan.min_score", "55"),
                "filter.min_signals=" + config.getString("filter.min_signals", "3")
        ));
        view.put("noviceLines", List.of(
                "Use only strongest names.",
                "Keep positions small when coverage is low."
        ));
        view.put("actionAdvice", kv(
                "css", "info",
                "level", "CHECK",
                "reason", "Review watchlist and top cards together.",
                "singleMaxPct", fmt1(config.getDouble("report.position.max_single_pct", 5.0)),
                "totalMaxPct", fmt1(config.getDouble("report.position.max_total_pct", 50.0))
        ));

        List<Map<String, Object>> watchTable = new ArrayList<>();
        List<Map<String, Object>> aiItems = new ArrayList<>();
        for (WatchlistAnalysis row : watchRows) {
            if (row == null) {
                continue;
            }
            watchTable.add(kv(
                    "priceSuspect", row.priceSuspect,
                    "name", blankTo(row.displayName, row.ticker),
                    "traceSummary", "source=" + blankTo(row.dataSource, "-") + " | ts=" + blankTo(row.priceTimestamp, "-"),
                    "lastClose", fmt2(row.lastClose),
                    "action", watchAction(row),
                    "actionCss", watchActionCss(watchAction(row)),
                    "reasons", List.of(blankTo(row.gateReason, "n/a")),
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
                    "riskText", candidate.score >= 70 ? "LOW" : "MID",
                    "planLine", "Check chart before execution.",
                    "reasons", List.of("score=" + fmt1(candidate.score))
            ));
        }
        view.put("topCards", kv(
                "funnelStats", List.of("candidates_from_market_scan=" + (marketReferenceCandidates == null ? 0 : marketReferenceCandidates.size())),
                "mainReasons", List.of(),
                "skipReason", "",
                "gateLines", List.of(),
                "noviceWarn", false,
                "emptyMessage", cardRows.isEmpty() ? "No Top5 candidates." : "",
                "cards", cardRows,
                "excludedDerivatives", ""
        ));

        view.put("disclaimerLines", List.of(
                "Decision support only, not investment advice.",
                "Validate prices and risk before placing orders."
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
        if (text == null || text.trim().isEmpty()) return List.of("No AI summary.");
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
