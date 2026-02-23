package com.stockbot.jp.output;

import com.stockbot.core.ModuleResult;
import com.stockbot.core.ModuleStatus;
import com.stockbot.core.RunTelemetry;
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
import java.util.LinkedHashMap;
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
        return writeDailyReport(
                reportDir,
                startedAt,
                zoneId,
                universeSize,
                scannedSize,
                candidateSize,
                topN,
                watchlist,
                watchlistCandidates,
                marketReferenceCandidates,
                minScore,
                runType,
                previousScoreByTicker,
                scanSummary,
                marketScanPartial,
                marketScanStatus,
                diagnostics,
                Map.of(),
                null
        );
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
            Diagnostics diagnostics,
            Map<String, ModuleResult> moduleResults,
            RunTelemetry runTelemetry
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
                diagnostics,
                moduleResults,
                runTelemetry
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
        sb.append(i18n.t("report.novice.conclusion", "Conclusion")).append(" ");
        sb.append(indicatorCoveragePct < 50.0
                ? i18n.t("report.novice.coverage_low", "Coverage is low. Keep position size small.")
                : i18n.t("report.novice.coverage_ok", "Coverage is acceptable."));
        sb.append("\n")
                .append(i18n.t("report.novice.run_type", "Run type: "))
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
            sb.append("\n").append(i18n.t("report.novice.no_action", "No immediate action."));
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
        return buildHtml(
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
                diagnostics,
                Map.of(),
                null
        );
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
            Diagnostics diagnostics,
            Map<String, ModuleResult> moduleResults,
            RunTelemetry runTelemetry
    ) {
        ScanResultSummary summary = scanSummary == null
                ? new ScanResultSummary(0, 0, 0, Map.of(), Map.of(), Map.of())
                : scanSummary;
        List<WatchlistAnalysis> watchRows = sortWatch(watchlistCandidates);
        List<ScoredCandidate> topCards = sortMarket(marketReferenceCandidates);
        if (topCards.size() > 5) {
            topCards = new ArrayList<>(topCards.subList(0, 5));
        }
        Map<String, ModuleResult> modules = moduleResults == null ? Map.of() : moduleResults;
        boolean indicatorsOk = isModuleOk(modules, "indicators");
        String notReadyHint = indicatorsOk ? "-" : "- (\u539F\u56E0\u89C1\u6A21\u5757\u72B6\u6001)";

        Map<String, Object> view = new HashMap<>();
        view.put("pageTitle", i18n.t("report.page.title", "StockBot JP Daily Report"));
        view.put("reportTitle", i18n.t("report.title", "StockBot Daily Report"));
        view.put("topTiles", List.of(
                kv("key", i18n.t("report.tile.run_time", "Run Time"), "value", DISPLAY_TS.format(startedAt.atZone(zoneId))),
                kv("key", i18n.t("report.tile.fetch_coverage", "Fetch Coverage"), "value", scannedSize + " / " + Math.max(1, universeSize)),
                kv("key", i18n.t("report.tile.candidates", "Candidates"), "value", Integer.toString(candidateSize)),
                kv("key", i18n.t("report.tile.top_n", "TopN"), "value", Integer.toString(topN)),
                kv("key", i18n.t("report.tile.run_type", "Run Type"), "value", runTypeLabel(runType))
        ));
        view.put("failureStats", List.of(
                i18n.t("report.failure.timeout", "Timeout") + ": " + summary.requestFailureCount(ScanFailureReason.TIMEOUT),
                i18n.t("report.failure.parse_error", "Parse Error") + ": " + summary.requestFailureCount(ScanFailureReason.PARSE_ERROR),
                i18n.t("report.failure.stale", "Stale") + ": " + summary.failureCount(ScanFailureReason.STALE),
                i18n.t("report.failure.other", "Other") + ": " + summary.failureCount(ScanFailureReason.OTHER)
        ));
        view.put("hasSuspectPrice", watchRows.stream().anyMatch(r -> r != null && r.priceSuspect));
        view.put("suspectTickers", suspectTickers(watchRows));
        String unknownText = i18n.t("report.coverage.unknown", "UNKNOWN");
        view.put("coverageSource", diagnostics == null ? unknownText : blankTo(diagnostics.coverageSource, unknownText));
        view.put("partialMessage", (marketScanPartial || "PARTIAL".equalsIgnoreCase(blankTo(marketScanStatus, "")))
                ? i18n.t("report.partial.market_scan", "Market scan is partial.")
                : "");
        view.put("systemStatusLines", List.of(
                i18n.t("report.system.indicator_core", "indicator.core") + "=" + config.getString("indicator.core", "sma20,sma60,rsi14,atr14"),
                i18n.t("report.system.min_score", "scan.min_score") + "=" + config.getString("scan.min_score", "55"),
                i18n.t("report.system.min_signals", "filter.min_signals") + "=" + config.getString("filter.min_signals", "3")
        ));
        view.put("noviceLines", List.of(
                i18n.t("report.novice.line1", "Prefer observing high score names first."),
                i18n.t("report.novice.line2", "If coverage is low, keep position size small.")
        ));
        view.put("moduleStatuses", moduleStatusRows(modules));
        view.put("actionAdvice", kv(
                "css", "info",
                "level", i18n.t("report.action.level.check", "CHECK"),
                "reason", i18n.t("report.action.reason", "Use watchlist and Top5 together; keep risk controls."),
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
                    "traceSummary", i18n.t("report.trace.source", "source") + "=" + blankTo(row.dataSource, "-")
                            + " | " + i18n.t("report.trace.ts", "ts") + "=" + blankTo(row.priceTimestamp, "-"),
                    "lastClose", fmt2(row.lastClose),
                    "action", watchActionLabel(action),
                    "actionCss", watchActionCss(action),
                    "reasons", List.of(blankTo(row.gateReason, i18n.t("report.common.na", "N/A"))),
                    "entryRange", notReadyHint,
                    "stopLoss", notReadyHint,
                    "takeProfit", notReadyHint
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
                            ? i18n.t("report.card.risk.low", "LOW")
                            : i18n.t("report.card.risk.mid", "MID"),
                    "planLine", i18n.t("report.card.plan", "Use staged entry and strict stop."),
                    "reasons", List.of("score=" + fmt1(candidate.score))
            ));
        }
        view.put("topCards", kv(
                "funnelStats", List.of(i18n.t("report.topcards.funnel_from_market", "market_candidates")
                        + "=" + (marketReferenceCandidates == null ? 0 : marketReferenceCandidates.size())),
                "mainReasons", List.of(),
                "skipReason", "",
                "gateLines", List.of(),
                "noviceWarn", false,
                "emptyMessage", cardRows.isEmpty() ? i18n.t("report.topcards.empty", "No Top5 candidates.") : "",
                "cards", cardRows,
                "excludedDerivatives", ""
        ));

        view.put("disclaimerLines", List.of(
                i18n.t("report.disclaimer.1", "This report is for research only."),
                i18n.t("report.disclaimer.2", "Please make decisions with your own risk control.")
        ));
        view.put("hasConfigSnapshot", false);
        view.put("configRows", List.of());
        view.put("runSummaryText", runTelemetry == null ? "" : runTelemetry.getSummary());
        view.put("diagnostics", kv(
                "enabled", diagnostics != null,
                "runId", diagnostics == null ? 0L : diagnostics.runId,
                "runMode", diagnostics == null ? "" : diagnostics.runMode,
                "coverageScope", diagnostics == null ? "" : diagnostics.coverageScope,
                "coverageSource", diagnostics == null ? "" : diagnostics.coverageSource,
                "coverageOwner", diagnostics == null ? "" : diagnostics.coverageOwner,
                "coverageMetrics", List.of(),
                "dataSources", List.of(),
                "featureStatuses", List.of(),
                "configSnapshot", List.of(),
                "notes", diagnostics == null ? List.of() : diagnostics.notes
        ));

        String rendered = renderer.renderDailyReport(Map.of("view", view));
        return htmlPostProcessor.cleanDocument(rendered);
    }

    private List<Map<String, Object>> moduleStatusRows(Map<String, ModuleResult> moduleResults) {
        if (moduleResults == null || moduleResults.isEmpty()) {
            return List.of();
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Map.Entry<String, ModuleResult> entry : moduleResults.entrySet()) {
            String moduleName = entry.getKey() == null ? "" : entry.getKey().trim();
            if (moduleName.isEmpty()) {
                continue;
            }
            ModuleResult result = entry.getValue();
            ModuleStatus status = result == null ? ModuleStatus.ERROR : result.status();
            String reason = result == null ? "unknown" : blankTo(result.reason(), "unknown");
            Map<String, Object> evidence = result == null ? Map.of() : result.evidence();
            rows.add(kv(
                    "module", moduleName,
                    "status", status.name(),
                    "reason", reason,
                    "evidence", evidence.isEmpty() ? "{}" : new JSONObject(new LinkedHashMap<>(evidence)).toString()
            ));
        }
        return rows;
    }

    private boolean isModuleOk(Map<String, ModuleResult> moduleResults, String key) {
        if (moduleResults == null || key == null) {
            return false;
        }
        ModuleResult result = moduleResults.get(key);
        return result != null && result.status() == ModuleStatus.OK;
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
        if ("CANDIDATE".equals(status) && row.technicalScore >= 80.0) {
            return "BUY";
        }
        if ("CANDIDATE".equals(status)) {
            return "WATCH";
        }
        if ("RISK".equals(status)) {
            return "AVOID";
        }
        return "WAIT";
    }

    private String watchActionLabel(String action) {
        if ("BUY".equals(action)) return i18n.t("report.action.buy", "BUY");
        if ("WATCH".equals(action)) return i18n.t("report.action.watch", "WATCH");
        if ("AVOID".equals(action)) return i18n.t("report.action.avoid", "AVOID");
        return i18n.t("report.action.wait", "WAIT");
    }

    private String runTypeLabel(RunType runType) {
        if (runType == RunType.CLOSE) {
            return i18n.t("report.run_type.close", "CLOSE");
        }
        return i18n.t("report.run_type.intraday", "INTRADAY");
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
            if (row != null && row.priceSuspect) {
                out.add(blankTo(row.ticker, row.code));
            }
        }
        return String.join(",", out);
    }

    private List<String> splitLines(String text) {
        if (text == null || text.trim().isEmpty()) {
            return List.of(i18n.t("report.ai.none", "No AI summary."));
        }
        List<String> out = new ArrayList<>();
        for (String line : text.split("\\r?\\n")) {
            String t = line.trim();
            if (!t.isEmpty()) {
                out.add(t);
            }
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
        for (int i = 0; i + 1 < values.length; i += 2) {
            out.put(String.valueOf(values[i]), values[i + 1]);
        }
        return out;
    }
}
