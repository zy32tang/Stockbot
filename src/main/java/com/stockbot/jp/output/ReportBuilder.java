package com.stockbot.jp.output;

import com.stockbot.core.ModuleResult;
import com.stockbot.core.ModuleStatus;
import com.stockbot.core.RunTelemetry;
import com.stockbot.core.diagnostics.Outcome;
import com.stockbot.core.diagnostics.Diagnostics;
import com.stockbot.jp.config.Config;
import com.stockbot.jp.model.ScanFailureReason;
import com.stockbot.jp.model.ScanResultSummary;
import com.stockbot.jp.model.ScoredCandidate;
import com.stockbot.jp.model.WatchlistAnalysis;
import com.stockbot.jp.plan.TradePlan;
import com.stockbot.jp.plan.TradePlanBuilder;
import org.json.JSONArray;
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
    private static final LocalTime DEFAULT_CLOSE_TIME = LocalTime.of(15, 0);

    private final Config config;
    private final ThymeleafReportRenderer renderer;
    private final HtmlPostProcessor htmlPostProcessor;
    private final I18n i18n;
    private final TradePlanBuilder tradePlanBuilder;

    public ReportBuilder(Config config) {
        this.config = config;
        this.renderer = new ThymeleafReportRenderer();
        this.htmlPostProcessor = new HtmlPostProcessor();
        this.i18n = new I18n(config);
        this.tradePlanBuilder = new TradePlanBuilder(config);
    }

    public enum RunType {
        INTRADAY,
        CLOSE
    }

    public static RunType detectRunType(Instant startedAt, ZoneId zoneId) {
        return detectRunType(startedAt, zoneId, DEFAULT_CLOSE_TIME);
    }

    public static RunType detectRunType(Instant startedAt, ZoneId zoneId, LocalTime closeTime) {
        if (startedAt == null || zoneId == null) {
            return RunType.CLOSE;
        }
        LocalTime effectiveClose = closeTime == null ? DEFAULT_CLOSE_TIME : closeTime;
        return startedAt.atZone(zoneId).toLocalTime().isBefore(effectiveClose) ? RunType.INTRADAY : RunType.CLOSE;
    }

    public RunType detectRunTypeByConfig(Instant startedAt, ZoneId zoneId) {
        return detectRunType(startedAt, zoneId, configuredCloseTime());
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
        int topCardsLimit = resolveTopCardsLimit();
        if (topCards.size() > topCardsLimit) {
            topCards = new ArrayList<>(topCards.subList(0, topCardsLimit));
        }
        Map<String, ModuleResult> modules = moduleResults == null ? Map.of() : moduleResults;
        int effectiveTotal = summary.total > 0 ? summary.total : Math.max(0, universeSize);
        int effectiveFetchCoverage = summary.total > 0
                ? Math.max(0, summary.fetchCoverage)
                : Math.max(0, scannedSize);
        int fetchMissing = Math.max(0, effectiveTotal - effectiveFetchCoverage);
        String marketInterval = blankTo(config.getString("fetch.interval.market", "1d"), "1d");

        Map<String, Object> view = new HashMap<>();
        view.put("pageTitle", i18n.t("report.page.title", "StockBot JP Daily Report"));
        view.put("reportTitle", i18n.t("report.title", "StockBot Daily Report"));
        view.put("topTiles", List.of(
                kv("key", i18n.t("report.tile.run_time", "Run Time"), "value", DISPLAY_TS.format(startedAt.atZone(zoneId))),
                kv(
                        "key",
                        i18n.t("report.tile.fetch_coverage", "Fetch Coverage"),
                        "value",
                        effectiveFetchCoverage + " / " + Math.max(1, effectiveTotal)
                ),
                kv("key", i18n.t("report.tile.candidates", "Candidates"), "value", Integer.toString(candidateSize)),
                kv("key", i18n.t("report.tile.top_n", "TopN"), "value", Integer.toString(topN)),
                kv("key", i18n.t("report.tile.run_type", "Run Type"), "value", runTypeLabel(runType))
        ));
        view.put("failureStats", List.of(
                i18n.t("report.failure.timeout", "Timeout") + ": " + summary.requestFailureCount(ScanFailureReason.TIMEOUT),
                i18n.t("report.failure.no_data", "No Data") + ": " + summary.requestFailureCount(ScanFailureReason.HTTP_404_NO_DATA),
                i18n.t("report.failure.parse_error", "Parse Error") + ": " + summary.requestFailureCount(ScanFailureReason.PARSE_ERROR),
                i18n.t("report.failure.rate_limit", "Rate Limit") + ": " + summary.requestFailureCount(ScanFailureReason.RATE_LIMIT),
                i18n.t("report.failure.stale", "Stale") + ": " + summary.failureCount(ScanFailureReason.STALE),
                i18n.t("report.failure.other", "Other") + ": " + summary.failureCount(ScanFailureReason.OTHER)
        ));
        view.put("coverageMeta", List.of(
                i18n.t("report.coverage.data_granularity", "data_granularity") + "=" + marketInterval,
                i18n.t("report.coverage.fetch_missing", "fetch_missing") + "=" + fetchMissing
        ));
        view.put("hasSuspectPrice", watchRows.stream().anyMatch(r -> r != null && r.priceSuspect));
        view.put("suspectTickers", suspectTickers(watchRows));
        String unknownText = i18n.t("report.coverage.unknown", "UNKNOWN");
        view.put("coverageSource", diagnostics == null ? unknownText : blankTo(diagnostics.coverageSource, unknownText));
        view.put("partialMessage", (marketScanPartial || "PARTIAL".equalsIgnoreCase(blankTo(marketScanStatus, "")))
                ? i18n.t("report.partial.market_scan", "Market scan is partial.")
                : "");
        view.put("systemStatusLines", List.of(
                "tech.ma="
                        + config.getString("tech.ma.short", "5")
                        + "/"
                        + config.getString("tech.ma.mid", "10")
                        + "/"
                        + config.getString("tech.ma.long", "20"),
                "tech.bias.safe/risk="
                        + config.getString("tech.bias.safe", "0.05")
                        + "/"
                        + config.getString("tech.bias.risk", "0.08"),
                "top5.filter_risk=" + config.getString("top5.filter_risk", "true")
                        + ", top5.count=" + config.getString("top5.count", "5")
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

        List<Map<String, Object>> watchCards = new ArrayList<>();
        for (WatchlistAnalysis row : watchRows) {
            if (row == null) {
                continue;
            }
            watchCards.add(buildWatchCard(row));
        }
        view.put("watchCards", watchCards);

        List<Map<String, Object>> marketEntries = new ArrayList<>();
        for (ScoredCandidate candidate : topCards) {
            if (candidate == null) {
                continue;
            }
            marketEntries.add(buildMarketEntry(candidate));
        }
        marketEntries.sort(top5Comparator());

        boolean filterRisk = config.getBoolean("top5.filter_risk", true);
        int topCount = Math.max(1, config.getInt("top5.count", 5));
        List<Map<String, Object>> qualified = new ArrayList<>();
        for (Map<String, Object> row : marketEntries) {
            if (row == null) {
                continue;
            }
            String riskLevel = blankTo(asText(row.get("riskLevel")), "RISK").toUpperCase(Locale.ROOT);
            if (filterRisk && "RISK".equals(riskLevel)) {
                continue;
            }
            qualified.add(row);
        }

        List<Map<String, Object>> cardRows = qualified.size() <= topCount
                ? qualified
                : new ArrayList<>(qualified.subList(0, topCount));
        for (int i = 0; i < cardRows.size(); i++) {
            cardRows.get(i).put("rank", i + 1);
        }

        String top5EmptyMessage = "";
        List<Map<String, Object>> riskTop3Rows = List.of();
        if (cardRows.isEmpty()) {
            top5EmptyMessage = "No qualified opportunities today (all are RISK or data degraded).";
            riskTop3Rows = selectRiskTop3(marketEntries, watchCards);
        }
        view.put("topCards", kv(
                "funnelStats", List.of(i18n.t("report.topcards.funnel_from_market", "market_candidates")
                        + "=" + (marketReferenceCandidates == null ? 0 : marketReferenceCandidates.size())),
                "mainReasons", List.of(),
                "skipReason", "",
                "gateLines", List.of(),
                "noviceWarn", false,
                "emptyMessage", top5EmptyMessage,
                "cards", cardRows,
                "riskTop3", riskTop3Rows,
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

    private Map<String, Object> buildWatchCard(WatchlistAnalysis row) {
        JSONObject indicators = safeJsonObject(row == null ? "" : row.technicalIndicatorsJson);
        JSONObject reasons = safeJsonObject(row == null ? "" : row.technicalReasonsJson);
        JSONObject subscores = indicators.optJSONObject("subscores");

        int trendStrength = (int) Math.round(indicators.optDouble("trend_strength",
                row == null ? 0.0 : row.technicalScore));
        int executionQuality = subscores == null ? 0 : subscores.optInt("execution_quality", 0);
        int volumeConfirm = subscores == null ? 0 : subscores.optInt("volume_confirm", 0);
        String signal = blankTo(indicators.optString("signal_status", row == null ? "" : row.technicalStatus), "NEUTRAL");
        String riskLevel = blankTo(indicators.optString("risk_level", row == null ? "" : row.technicalStatus), "RISK");
        String dataStatus = blankTo(indicators.optString("data_status", ""), "MISSING");

        double price = finiteOr(indicators.optDouble("last_close", Double.NaN), row == null ? Double.NaN : row.lastClose);
        double ma5 = indicators.optDouble("ma5", Double.NaN);
        double ma10 = indicators.optDouble("ma10", Double.NaN);
        double ma20 = indicators.optDouble("ma20", Double.NaN);
        double bias = indicators.optDouble("bias", Double.NaN);
        double volRatio = indicators.optDouble("vol_ratio", Double.NaN);
        double stopLine = indicators.optDouble("stop_line", Double.NaN);
        double stopPct = indicators.optDouble("stop_pct", Double.NaN);

        List<Map<String, Object>> checklist = extractChecklistRows(reasons, 6);
        if (checklist.isEmpty()) {
            checklist = List.of(kv(
                    "status", "WATCH",
                    "label", "Data status",
                    "value", dataStatus,
                    "rule", "Checklist unavailable, fallback to data status"
            ));
        }
        List<String> checklistReasons = checklistReasonLines(checklist, 6);

        return kv(
                "name", blankTo(row == null ? "" : row.displayName, row == null ? "" : row.ticker),
                "ticker", blankTo(row == null ? "" : row.ticker, row == null ? "" : row.code),
                "trendStrength", trendStrength,
                "signal", signal,
                "riskLevel", riskLevel,
                "dataStatus", dataStatus,
                "price", fmt2(price),
                "ma5", fmt2(ma5),
                "ma10", fmt2(ma10),
                "ma20", fmt2(ma20),
                "bias", fmtPct(bias),
                "volRatio", fmt2(volRatio),
                "stopLine", fmt2(stopLine),
                "stopPct", fmtPct(stopPct),
                "aiStatus", (row != null && row.aiTriggered) ? "AI: ready" : "AI: skipped",
                "newsStatus", "News: " + Math.max(0, row == null ? 0 : row.newsCount) + " items",
                "traceSummary", i18n.t("report.trace.source", "source") + "=" + blankTo(row == null ? "" : row.dataSource, "-")
                        + " | " + i18n.t("report.trace.ts", "ts") + "=" + blankTo(row == null ? "" : row.priceTimestamp, "-"),
                "checklist", checklist,
                "checklistReasons", checklistReasons,
                "riskRank", riskRank(riskLevel),
                "dataRank", dataRank(dataStatus),
                "stopPctRaw", finiteOr(stopPct, 0.0),
                "biasAbsRaw", Math.abs(finiteOr(bias, 0.0)),
                "executionQuality", executionQuality,
                "volumeConfirm", volumeConfirm
        );
    }

    private Map<String, Object> buildMarketEntry(ScoredCandidate candidate) {
        JSONObject indicators = safeJsonObject(candidate == null ? "" : candidate.indicatorsJson);
        JSONObject reasons = safeJsonObject(candidate == null ? "" : candidate.reasonsJson);
        JSONObject subscores = indicators.optJSONObject("subscores");

        int trendStrength = indicators.optInt("trend_strength", candidate == null ? 0 : (int) Math.round(candidate.score));
        int executionQuality = subscores == null ? 0 : subscores.optInt("execution_quality", 0);
        int volumeConfirm = subscores == null ? 0 : subscores.optInt("volume_confirm", 0);
        String signal = blankTo(indicators.optString("signal_status", ""), "NEUTRAL");
        String riskLevel = blankTo(indicators.optString("risk_level", ""), "RISK");
        String dataStatus = blankTo(indicators.optString("data_status", ""), "MISSING");

        double price = finiteOr(indicators.optDouble("last_close", Double.NaN), candidate == null ? Double.NaN : candidate.close);
        double ma5 = indicators.optDouble("ma5", Double.NaN);
        double ma10 = indicators.optDouble("ma10", Double.NaN);
        double ma20 = indicators.optDouble("ma20", Double.NaN);
        double bias = indicators.optDouble("bias", Double.NaN);
        double volRatio = indicators.optDouble("vol_ratio", Double.NaN);
        double stopLine = indicators.optDouble("stop_line", Double.NaN);
        double stopPct = indicators.optDouble("stop_pct", Double.NaN);

        List<Map<String, Object>> checklist = extractChecklistRows(reasons, 6);
        if (checklist.isEmpty()) {
            checklist = List.of(kv(
                    "status", "WATCH",
                    "label", "Data status",
                    "value", dataStatus,
                    "rule", "Checklist unavailable, fallback to data status"
            ));
        }
        List<String> checklistReasons = checklistReasonLines(checklist, 6);

        return kv(
                "name", blankTo(candidate == null ? "" : candidate.code, candidate == null ? "" : candidate.ticker)
                        + " " + blankTo(candidate == null ? "" : candidate.name, ""),
                "ticker", blankTo(candidate == null ? "" : candidate.ticker, ""),
                "trendStrength", trendStrength,
                "signal", signal,
                "riskLevel", riskLevel,
                "dataStatus", dataStatus,
                "price", fmt2(price),
                "ma5", fmt2(ma5),
                "ma10", fmt2(ma10),
                "ma20", fmt2(ma20),
                "bias", fmtPct(bias),
                "volRatio", fmt2(volRatio),
                "stopLine", fmt2(stopLine),
                "stopPct", fmtPct(stopPct),
                "checklist", checklist,
                "checklistReasons", checklistReasons,
                "riskRank", riskRank(riskLevel),
                "dataRank", dataRank(dataStatus),
                "stopPctRaw", finiteOr(stopPct, 0.0),
                "biasAbsRaw", Math.abs(finiteOr(bias, 0.0)),
                "executionQuality", executionQuality,
                "volumeConfirm", volumeConfirm
        );
    }

    private Comparator<Map<String, Object>> top5Comparator() {
        return (a, b) -> {
            int c1 = Integer.compare(asInt(b.get("trendStrength")), asInt(a.get("trendStrength")));
            if (c1 != 0) return c1;
            int c2 = Integer.compare(asInt(b.get("executionQuality")), asInt(a.get("executionQuality")));
            if (c2 != 0) return c2;
            return Integer.compare(asInt(b.get("volumeConfirm")), asInt(a.get("volumeConfirm")));
        };
    }

    private List<Map<String, Object>> selectRiskTop3(
            List<Map<String, Object>> marketEntries,
            List<Map<String, Object>> watchCards
    ) {
        List<Map<String, Object>> source = (marketEntries != null && !marketEntries.isEmpty())
                ? new ArrayList<>(marketEntries)
                : (watchCards == null ? new ArrayList<>() : new ArrayList<>(watchCards));
        source.sort((a, b) -> {
            int r = Integer.compare(asInt(b.get("riskRank")), asInt(a.get("riskRank")));
            if (r != 0) return r;
            int d = Integer.compare(asInt(b.get("dataRank")), asInt(a.get("dataRank")));
            if (d != 0) return d;
            int s = Double.compare(asDouble(b.get("stopPctRaw")), asDouble(a.get("stopPctRaw")));
            if (s != 0) return s;
            return Double.compare(asDouble(b.get("biasAbsRaw")), asDouble(a.get("biasAbsRaw")));
        });
        List<Map<String, Object>> out = new ArrayList<>();
        int limit = Math.min(3, source.size());
        for (int i = 0; i < limit; i++) {
            Map<String, Object> row = source.get(i);
            @SuppressWarnings("unchecked")
            List<String> reasons = (List<String>) row.get("checklistReasons");
            List<String> topReasons = reasons == null ? List.of() : reasons.subList(0, Math.min(3, reasons.size()));
            out.add(kv(
                    "rank", i + 1,
                    "name", blankTo(asText(row.get("name")), asText(row.get("ticker"))),
                    "riskLevel", blankTo(asText(row.get("riskLevel")), "RISK"),
                    "dataStatus", blankTo(asText(row.get("dataStatus")), "MISSING"),
                    "reasons", topReasons
            ));
        }
        return out;
    }

    private List<Map<String, Object>> extractChecklistRows(JSONObject reasonRoot, int maxItems) {
        if (reasonRoot == null) {
            return List.of();
        }
        List<Map<String, Object>> out = new ArrayList<>();
        JSONObject tech = reasonRoot.optJSONObject("tech");
        JSONArray checklist = tech == null ? null : tech.optJSONArray("checklist");
        if (checklist != null) {
            for (int i = 0; i < checklist.length(); i++) {
                JSONObject item = checklist.optJSONObject(i);
                if (item == null) {
                    continue;
                }
                out.add(kv(
                        "status", blankTo(item.optString("status", ""), "WATCH"),
                        "label", blankTo(item.optString("label", ""), "Rule"),
                        "value", blankTo(item.optString("value", ""), "-"),
                        "rule", blankTo(item.optString("rule", ""), "-")
                ));
            }
        }
        if (out.isEmpty()) {
            appendReasonFallback(out, reasonRoot.optJSONArray("risk_reasons"), "FAIL");
            appendReasonFallback(out, reasonRoot.optJSONArray("filter_reasons"), "WATCH");
            appendReasonFallback(out, reasonRoot.optJSONArray("score_reasons"), "PASS");
        }
        out.sort((a, b) -> Integer.compare(statusPriority(asText(a.get("status"))), statusPriority(asText(b.get("status")))));
        if (out.size() > maxItems) {
            return new ArrayList<>(out.subList(0, maxItems));
        }
        return out;
    }

    private void appendReasonFallback(List<Map<String, Object>> out, JSONArray array, String status) {
        if (out == null || array == null) {
            return;
        }
        for (int i = 0; i < array.length(); i++) {
            String text = blankTo(array.optString(i, ""), "");
            if (text.isEmpty()) {
                continue;
            }
            out.add(kv(
                    "status", status,
                    "label", "Reason",
                    "value", text,
                    "rule", "-"
            ));
        }
    }

    private List<String> checklistReasonLines(List<Map<String, Object>> checklist, int maxItems) {
        if (checklist == null || checklist.isEmpty()) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        for (Map<String, Object> item : checklist) {
            if (item == null) {
                continue;
            }
            String label = blankTo(asText(item.get("label")), "Rule");
            String value = blankTo(asText(item.get("value")), "-");
            out.add(label + ": " + value);
            if (out.size() >= maxItems) {
                break;
            }
        }
        return out;
    }

    private int statusPriority(String status) {
        String s = blankTo(status, "WATCH").toUpperCase(Locale.ROOT);
        if ("FAIL".equals(s)) return 0;
        if ("WATCH".equals(s)) return 1;
        return 2;
    }

    private int riskRank(String riskLevel) {
        String risk = blankTo(riskLevel, "").toUpperCase(Locale.ROOT);
        if ("RISK".equals(risk)) return 3;
        if ("NEAR".equals(risk)) return 2;
        if ("IN".equals(risk)) return 1;
        return 0;
    }

    private int dataRank(String dataStatus) {
        String status = blankTo(dataStatus, "").toUpperCase(Locale.ROOT);
        if ("MISSING".equals(status)) return 3;
        if ("DEGRADED".equals(status)) return 2;
        if ("OK".equals(status)) return 1;
        return 0;
    }

    private List<WatchlistAnalysis> sortWatch(List<WatchlistAnalysis> rows) {
        List<WatchlistAnalysis> out = new ArrayList<>();
        if (rows != null) {
            out.addAll(rows);
        }
        out.sort(Comparator.comparingDouble((WatchlistAnalysis r) -> r == null ? Double.NEGATIVE_INFINITY : r.technicalScore).reversed());
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

    private int resolveTopCardsLimit() {
        int configured = config.getInt("report.topcards.max_items", 0);
        if (configured <= 0) {
            configured = config.getInt("scan.market_reference_top_n", 5);
        }
        return Math.max(1, configured);
    }

    private LocalTime configuredCloseTime() {
        String raw = blankTo(config.getString("report.run_type.close_time", "15:00"), "15:00");
        try {
            return LocalTime.parse(raw);
        } catch (Exception ignored) {
            return DEFAULT_CLOSE_TIME;
        }
    }

    private String watchAction(WatchlistAnalysis row) {
        if (row == null) {
            return "WAIT";
        }
        String status = blankTo(row.technicalStatus, "").toUpperCase(Locale.ROOT);
        double buyThreshold = config.getDouble("report.action.buy_score_threshold", 80.0);
        if ("IN".equals(status) && row.technicalScore >= buyThreshold) {
            return "BUY";
        }
        if ("IN".equals(status) || "NEAR".equals(status)) {
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

    private List<String> watchReasons(WatchlistAnalysis row, Outcome<TradePlan> planOutcome) {
        List<String> reasons = new ArrayList<>();
        JSONObject root = safeJsonObject(row == null ? "" : row.technicalReasonsJson);
        appendReasonsFromJsonArray(reasons, root.optJSONArray("filter_reasons"));
        appendReasonsFromJsonArray(reasons, root.optJSONArray("risk_reasons"));
        appendReasonsFromJsonArray(reasons, root.optJSONArray("score_reasons"));

        String causeCode = blankTo(root.optString("cause_code", ""), "");
        if (!causeCode.isEmpty() && !"NONE".equalsIgnoreCase(causeCode)) {
            appendReason(reasons, "cause_code=" + causeCode);
        }

        JSONObject details = root.optJSONObject("details");
        if (details != null) {
            appendReason(reasons, details.optString("user_message", ""));
            String detailReason = blankTo(details.optString("reason", ""), "");
            if (!detailReason.isEmpty()) {
                appendReason(reasons, "reason=" + detailReason);
            }
        }

        if (reasons.isEmpty()) {
            appendReason(reasons, row == null ? "" : row.gateReason);
            appendReason(reasons, row == null ? "" : row.error);
        }

        if (planOutcome != null && !planOutcome.success) {
            Object reason = planOutcome.details == null ? null : planOutcome.details.get("reason");
            String reasonText = reason == null ? "" : String.valueOf(reason).trim();
            if (!reasonText.isEmpty()) {
                appendReason(reasons, "plan=" + reasonText);
            }
        }

        List<String> out = new ArrayList<>();
        for (String reason : reasons) {
            if (reason == null) {
                continue;
            }
            String trimmed = reason.trim();
            if (trimmed.isEmpty() || isPlaceholderReason(trimmed) || out.contains(trimmed)) {
                continue;
            }
            out.add(trimmed);
            if (out.size() >= 6) {
                break;
            }
        }
        if (out.isEmpty()) {
            return List.of("No checklist available");
        }
        return out;
    }

    private String resolvePlanFailureReason(Outcome<TradePlan> planOutcome, WatchlistAnalysis row) {
        if (planOutcome == null) {
            return row != null && !row.indicatorReady ? "indicator_not_ready" : "plan_unavailable";
        }
        if (!planOutcome.success) {
            Object reason = planOutcome.details == null ? null : planOutcome.details.get("reason");
            String reasonText = reason == null ? "" : String.valueOf(reason).trim();
            if (!reasonText.isEmpty()) {
                return reasonText;
            }
            if (planOutcome.causeCode != null) {
                return planOutcome.causeCode.name().toLowerCase(Locale.ROOT);
            }
            return "plan_invalid";
        }
        if (planOutcome.value == null || !planOutcome.value.valid) {
            return "plan_invalid";
        }
        if (row != null && !row.indicatorReady) {
            return "indicator_not_ready";
        }
        return "";
    }

    private void appendReasonsFromJsonArray(List<String> out, JSONArray array) {
        if (array == null || out == null) {
            return;
        }
        for (int i = 0; i < array.length(); i++) {
            appendReason(out, array.optString(i, ""));
        }
    }

    private void appendReason(List<String> out, String reason) {
        if (out == null || reason == null) {
            return;
        }
        String text = reason.trim();
        if (!text.isEmpty()) {
            out.add(text);
        }
    }

    private JSONObject safeJsonObject(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return new JSONObject();
        }
        try {
            return new JSONObject(raw);
        } catch (Exception ignored) {
            return new JSONObject();
        }
    }

    private boolean isPlaceholderReason(String value) {
        String text = blankTo(value, "").trim().toLowerCase(Locale.ROOT);
        return text.isEmpty()
                || "-".equals(text)
                || "na".equals(text)
                || "n/a".equals(text)
                || "none".equals(text)
                || "\u65e0".equals(text);
    }

    private String fmt1(double value) {
        return String.format(Locale.US, "%.1f", value);
    }

    private String fmt2(double value) {
        return Double.isFinite(value) ? String.format(Locale.US, "%.2f", value) : "-";
    }

    private String fmtPct(double value) {
        return Double.isFinite(value) ? String.format(Locale.US, "%.2f%%", value * 100.0) : "-";
    }

    private double finiteOr(double value, double fallback) {
        return Double.isFinite(value) ? value : fallback;
    }

    private int asInt(Object value) {
        if (value instanceof Number number) {
            return number.intValue();
        }
        String text = asText(value);
        if (text.isEmpty()) {
            return 0;
        }
        try {
            return (int) Math.round(Double.parseDouble(text));
        } catch (Exception ignored) {
            return 0;
        }
    }

    private double asDouble(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        String text = asText(value);
        if (text.isEmpty()) {
            return 0.0;
        }
        try {
            return Double.parseDouble(text);
        } catch (Exception ignored) {
            return 0.0;
        }
    }

    private String asText(Object value) {
        return value == null ? "" : String.valueOf(value).trim();
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



