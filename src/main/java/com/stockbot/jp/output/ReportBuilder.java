package com.stockbot.jp.output;

import com.stockbot.core.diagnostics.CauseCode;
import com.stockbot.core.diagnostics.Diagnostics;
import com.stockbot.core.diagnostics.FeatureResolution;
import com.stockbot.core.diagnostics.FeatureStatus;
import com.stockbot.core.diagnostics.Outcome;
import com.stockbot.jp.config.Config;
import com.stockbot.jp.indicator.IndicatorEngine;
import com.stockbot.jp.model.DataInsufficientReason;
import com.stockbot.jp.model.ScanFailureReason;
import com.stockbot.jp.model.ScanResultSummary;
import com.stockbot.jp.model.ScoredCandidate;
import com.stockbot.jp.model.WatchlistAnalysis;
import com.stockbot.jp.plan.TradePlan;
import com.stockbot.jp.plan.TradePlanBuilder;
import com.stockbot.jp.polymarket.PolymarketSignalReport;
import com.stockbot.jp.polymarket.PolymarketTopicSignal;
import com.stockbot.jp.polymarket.PolymarketWatchImpact;
import com.stockbot.jp.strategy.CandidateFilter;
import com.stockbot.jp.strategy.RiskFilter;
import com.stockbot.jp.strategy.ScoringEngine;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 模块说明：ReportBuilder（class）。
 * 主要职责：承载 output 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class ReportBuilder {
    private static final DateTimeFormatter FILE_TS = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");
    private static final DateTimeFormatter DISPLAY_TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final LocalTime CLOSE_TIME = LocalTime.of(15, 0);
    private static final List<String> DERIVATIVE_KEYWORDS = List.of(
            "ETF", "ETN", "REIT", "INV", "INVERSE", "LEVERAGE", "LEVERAGED",
            "BEAR", "BULL", "DOUBLE", "TRIPLE", "INDEX LINKED", "HEDGE"
    );
    private static final String OWNER_TOP5 = "com.stockbot.jp.output.ReportBuilder#buildTopSelection(...)";

    private final Config config;
    private final TradePlanBuilder tradePlanBuilder;

/**
 * 方法说明：ReportBuilder，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public ReportBuilder(Config config) {
        this.config = config;
        this.tradePlanBuilder = new TradePlanBuilder(config);
    }

    public enum RunType {
        INTRADAY,
        CLOSE
    }

    private enum RiskGrade {
        LOW,
        MID,
        HIGH
    }

/**
 * 方法说明：detectRunType，负责检测条件并输出判断结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static RunType detectRunType(Instant startedAt, ZoneId zoneId) {
        if (startedAt == null || zoneId == null) {
            return RunType.CLOSE;
        }
        return startedAt.atZone(zoneId).toLocalTime().isBefore(CLOSE_TIME) ? RunType.INTRADAY : RunType.CLOSE;
    }

/**
 * 方法说明：writeDailyReport，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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
            PolymarketSignalReport polymarketReport,
            Diagnostics diagnostics
    ) throws Exception {
        Files.createDirectories(reportDir);
        String ts = FILE_TS.format(startedAt.atZone(zoneId));
        Path report = reportDir.resolve("jp_daily_" + ts + ".html");
        String html = buildHtml(
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
                polymarketReport,
                diagnostics
        );
        Files.writeString(report, html, StandardCharsets.UTF_8);
        Files.writeString(reportDir.resolve("email_main.html"), html, StandardCharsets.UTF_8);
        String debugJson = buildDebugJson(
                startedAt,
                zoneId,
                watchlistCandidates,
                scanSummary,
                marketScanPartial,
                marketScanStatus,
                diagnostics
        );
        Files.writeString(reportDir.resolve("report_debug.json"), debugJson, StandardCharsets.UTF_8);
        return report;
    }

/**
 * 方法说明：buildHtml，负责构建目标对象或输出内容。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public String buildHtml(
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
            PolymarketSignalReport polymarketReport,
            Diagnostics diagnostics
    ) {
        List<WatchlistAnalysis> watchRows = sortedWatch(watchlistCandidates);
        Diagnostics diag = diagnostics == null ? new Diagnostics(0L, "") : diagnostics;
        ScanResultSummary summary = scanSummary == null
                ? new ScanResultSummary(0, 0, 0, Map.of(), Map.of(), Map.of())
                : scanSummary;
        Diagnostics.CoverageMetric fetchCoverageMetric = diag.selectedFetchCoverage();
        Diagnostics.CoverageMetric indicatorCoverageMetric = diag.selectedIndicatorCoverage();
        Diagnostics.CoverageMetric marketRunFetchMetric = diag.coverages.get("market_scan_fetch_coverage");
        Diagnostics.CoverageMetric watchlistFetchMetric = diag.coverages.get("watchlist_fetch_coverage");
        int fetchCoverageDenominator = fetchCoverageMetric == null ? universeSize : fetchCoverageMetric.denominator;
        int indicatorCoverageDenominator = indicatorCoverageMetric == null ? universeSize : indicatorCoverageMetric.denominator;
        int fetchCoverageCount = fetchCoverageMetric == null ? Math.max(0, summary.fetchCoverage) : fetchCoverageMetric.numerator;
        int indicatorCoverageCount = indicatorCoverageMetric == null ? Math.max(0, summary.indicatorCoverage) : indicatorCoverageMetric.numerator;
        double fetchCoveragePct = fetchCoverageMetric == null ? coveragePct(universeSize, fetchCoverageCount) : fetchCoverageMetric.pct;
        double indicatorCoveragePct = indicatorCoverageMetric == null ? coveragePct(universeSize, indicatorCoverageCount) : indicatorCoverageMetric.pct;
        String coverageScope = blankTo(diag.coverageScope, "MARKET");
        String coverageSource = blankTo(diag.coverageSource, "UNKNOWN");
        String marketRunCoverage = formatCoverage(marketRunFetchMetric);
        String watchlistCoverage = formatCoverage(watchlistFetchMetric);
        int marketRunDenominator = marketRunFetchMetric == null ? fetchCoverageDenominator : marketRunFetchMetric.denominator;
        boolean showCoverageScope = config.getBoolean("report.coverage.show_scope", true);
        String coverageScopeLabel = showCoverageScope
                ? coverageScope + "(" + ("WATCHLIST".equalsIgnoreCase(coverageScope)
                ? String.valueOf(fetchCoverageDenominator)
                : ("JP scan " + marketRunDenominator)) + ")"
                : coverageScope;

        CandidateSelection topSelection = buildTopSelection(
                sortedMarket(marketReferenceCandidates),
                runType,
                previousScoreByTicker,
                minScore,
                5,
                candidateSize,
                marketScanPartial,
                fetchCoveragePct,
                safe(marketScanStatus),
                diag
        );

        ActionAdvice advice = actionAdvice(fetchCoveragePct, indicatorCoveragePct, candidateSize, topSelection.cards);
        int horizonDays = Math.max(1, config.getInt("backtest.hold_days", 10));
        double singleMaxPct = clamp(config.getDouble("report.position.max_single_pct", config.getDouble("position.single.maxPct", 0.05) * 100.0), 0.0, 100.0) / 100.0;
        double totalMaxPct = clamp(config.getDouble("report.position.max_total_pct", config.getDouble("position.total.maxPct", 0.50) * 100.0), 0.0, 100.0) / 100.0;
        boolean isClose = runType == RunType.CLOSE;
        boolean hideEntryInIntraday = config.getBoolean("report.mode.intraday.hideEntry", true);

        StringBuilder sb = new StringBuilder(64_000);
        appendPageHeader(sb);
        appendTopHeader(
                sb,
                startedAt,
                zoneId,
                horizonDays,
                universeSize,
                candidateSize,
                topN,
                runType,
                fetchCoverageCount,
                fetchCoverageDenominator,
                fetchCoveragePct,
                indicatorCoverageCount,
                indicatorCoverageDenominator,
                indicatorCoveragePct,
                coverageScopeLabel,
                coverageSource,
                summary,
                watchRows,
                marketRunCoverage,
                watchlistCoverage,
                marketScanPartial,
                marketScanStatus,
                marketRunDenominator
        );
        appendSystemStatus(sb, summary, diag);
        appendActionAdvice(sb, advice, singleMaxPct, totalMaxPct);
        appendWatchTable(sb, watchRows, isClose);
        appendWatchAiSummary(sb, watchRows);
        appendTopCards(sb, topSelection, isClose, hideEntryInIntraday, diag);
        appendPolymarketSignals(sb, polymarketReport, diag);
        appendDisclaimer(sb);
        appendDiagnostics(sb, diag);
        sb.append("</div></body></html>");
        return sb.toString();
    }

/**
 * 方法说明：buildMailText，负责构建目标对象或输出内容。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public String buildMailText(
            Instant startedAt,
            ZoneId zoneId,
            int universeSize,
            int scannedSize,
            int candidateSize,
            int topN,
            List<WatchlistAnalysis> watchlistCandidates,
            List<ScoredCandidate> marketReferenceCandidates,
            List<String> watchlist,
            double minScore
    ) {
        double coverage = coveragePct(universeSize, scannedSize);
        return String.format(Locale.US, "StockBot JP report | run=%s | coverage=%.1f%% | candidates=%d | topN=%d",
                DISPLAY_TS.format(startedAt.atZone(zoneId)), coverage, candidateSize, topN);
    }

/**
 * 方法说明：appendPageHeader，负责追加组装输出片段。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void appendPageHeader(StringBuilder sb) {
        sb.append("<!doctype html><html><head><meta charset='UTF-8'><style>");
        sb.append("body{margin:0;background:#eef4f8;color:#12263a;font-family:'Segoe UI','PingFang SC','Microsoft YaHei',sans-serif;}");
        sb.append(".wrap{max-width:1120px;margin:20px auto;background:#fff;border:1px solid #d7e2eb;border-radius:14px;padding:18px 20px 24px;}");
        sb.append("h1{margin:0 0 10px;font-size:26px;}h2{margin:18px 0 10px;font-size:20px;}h3{margin:8px 0;font-size:16px;}");
        sb.append(".grid{display:grid;gap:10px;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));}.tile{border:1px solid #dbe6ef;border-radius:10px;background:#fbfdff;padding:10px;}");
        sb.append(".k{font-size:12px;color:#4b5d73;margin-bottom:4px;}.v{font-size:17px;font-weight:700;}.small{font-size:12px;color:#5b6c80;line-height:1.6;}");
        sb.append(".chip{display:inline-block;padding:2px 10px;border-radius:999px;font-size:12px;font-weight:700;}");
        sb.append(".good{background:#dcfce7;color:#166534;}.warn{background:#fef3c7;color:#92400e;}.bad{background:#fee2e2;color:#991b1b;}.info{background:#dbeafe;color:#1d4ed8;}.gray{background:#e5e7eb;color:#374151;}");
        sb.append("table{width:100%;border-collapse:collapse;font-size:13px;}th,td{border:1px solid #d9e4ee;padding:8px;text-align:left;vertical-align:top;}th{background:#f3f7fb;font-weight:700;}");
        sb.append(".card{border:1px solid #dbe6ef;border-radius:12px;background:#fcfeff;padding:12px;margin-top:10px;}.warnbox{border:1px solid #f3d7a1;background:#fff7ea;color:#8b5e00;border-radius:10px;padding:10px;margin-top:8px;}");
        sb.append(".dangerbox{border:1px solid #fecaca;background:#fff1f2;color:#991b1b;border-radius:10px;padding:10px;margin-top:8px;}");
        sb.append(".suspect{background:#fff8f8;} details{margin-top:6px;} summary{cursor:pointer;}");
        sb.append(".bul{margin:6px 0 0 16px;padding:0;} .bul li{margin:3px 0;}.disclaimer{font-size:12px;line-height:1.7;color:#5b6c80;border-top:1px solid #e1eaf2;margin-top:14px;padding-top:10px;}");
        sb.append("@media (max-width:720px){.wrap{margin:8px;border-radius:8px;padding:12px;}h1{font-size:20px;}th,td{font-size:12px;padding:6px;}}");
        sb.append("</style></head><body><div class='wrap'>");
    }

/**
 * 方法说明：appendTopHeader，负责追加组装输出片段。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void appendTopHeader(
            StringBuilder sb,
            Instant startedAt,
            ZoneId zoneId,
            int horizonDays,
            int universeSize,
            int candidateSize,
            int topN,
            RunType runType,
            int fetchCoverageCount,
            int fetchCoverageDenominator,
            double fetchCoveragePct,
            int indicatorCoverageCount,
            int indicatorCoverageDenominator,
            double indicatorCoveragePct,
            String coverageScope,
            String coverageSource,
            ScanResultSummary summary,
            List<WatchlistAnalysis> watchRows,
            String marketRunCoverage,
            String watchlistCoverage,
            boolean marketScanPartial,
            String marketScanStatus,
            int marketRunDenominator
    ) {
        sb.append("<h1>StockBot 决策辅助报告</h1>");
        sb.append("<h2>A. 顶部 Header</h2>");
        sb.append("<div class='grid'>");
        sb.append(tile("运行时间 (JST)", escape(DISPLAY_TS.format(startedAt.atZone(zoneId)))));
        sb.append(tile("预测周期", horizonDays + " 天"));
        sb.append(tile("FETCH_COVERAGE", fetchCoverageCount + " / " + fetchCoverageDenominator + " (" + fmt1(fetchCoveragePct) + "%)"));
        sb.append(tile("INDICATOR_COVERAGE", indicatorCoverageCount + " / " + indicatorCoverageDenominator + " (" + fmt1(indicatorCoveragePct) + "%)"));
        sb.append(tile("market_run_coverage", escape(blankTo(marketRunCoverage, "-"))));
        sb.append(tile("watchlist_coverage", escape(blankTo(watchlistCoverage, "-"))));
        sb.append(tile("今日候选数量", String.valueOf(candidateSize)));
        sb.append(tile("今日 TopN 数量", String.valueOf(topN)));
        sb.append(tile("邮件类型", runType == RunType.CLOSE ? "CLOSE (15:00)" : "INTRADAY (11:30)"));
        sb.append(tile("coverage_scope", escape(blankTo(coverageScope, "MARKET"))));
        sb.append("</div>");

        int suspect = countPriceSuspects(watchRows);
        if (suspect > 0) {
            sb.append("<div class='dangerbox'>Possible price mapping issue: same lastClose repeated across tickers.</div>");
            sb.append("<div class='small'>suspect tickers: ").append(escape(joinSuspectTickers(watchRows))).append("</div>");
        }

        sb.append("<div class='small' style='margin-top:8px;'>失败原因统计：");
        sb.append("timeout: ").append(summary.requestFailureCount(ScanFailureReason.TIMEOUT)).append(" | ");
        sb.append("http_404/no_data: ").append(summary.requestFailureCount(ScanFailureReason.HTTP_404_NO_DATA) + summary.failureCount(ScanFailureReason.HTTP_404_NO_DATA)).append(" | ");
        sb.append("parse_error: ").append(summary.requestFailureCount(ScanFailureReason.PARSE_ERROR)).append(" | ");
        sb.append("stale: ").append(summary.failureCount(ScanFailureReason.STALE)).append(" | ");
        sb.append("history_short: ").append(summary.failureCount(ScanFailureReason.HISTORY_SHORT)).append(" | ");
        sb.append("filtered_non_tradable: ").append(summary.failureCount(ScanFailureReason.FILTERED_NON_TRADABLE)).append(" | ");
        sb.append("rate_limit: ").append(summary.requestFailureCount(ScanFailureReason.RATE_LIMIT)).append(" | ");
        sb.append("other: ").append(summary.failureCount(ScanFailureReason.OTHER) + summary.requestFailureCount(ScanFailureReason.OTHER));
        sb.append("</div>");
        sb.append("<div class='small'>coverage_source=").append(escape(blankTo(coverageSource, "UNKNOWN"))).append("</div>");
        if (marketScanPartial || "PARTIAL".equalsIgnoreCase(blankTo(marketScanStatus, ""))) {
            sb.append("<div class='small'>覆盖率低：market scan=PARTIAL（仅处理 ")
                    .append(Math.max(0, marketRunDenominator))
                    .append(" 标的）</div>");
        }
    }

/**
 * 方法说明：appendSystemStatus，负责追加组装输出片段。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void appendSystemStatus(StringBuilder sb, ScanResultSummary summary, Diagnostics diagnostics) {
        sb.append("<h2>B. 系统状态说明</h2>");
        sb.append("<div class='small'>");
        sb.append("1) 综合分量纲统一为 <b>0~100</b>（自选股与全市场一致）。<br>");
        sb.append("2) 指标引擎：").append(escape(String.join(", ", IndicatorEngine.computedIndicators()))).append("<br>");
        sb.append("3) 筛选模块(CandidateFilter)：hard 条件 + signals，hard=")
                .append(escape(String.join(", ", CandidateFilter.hardRuleNames())))
                .append("，signals=")
                .append(escape(String.join(", ", CandidateFilter.signalRuleNames())))
                .append("<br>");
        sb.append("4) 风控模块(RiskFilter)：")
                .append(escape(String.join(", ", RiskFilter.riskFlagNames())))
                .append("<br>");
        sb.append("5) 评分模块(ScoringEngine)：")
                .append(escape(String.join(", ", ScoringEngine.factorNames())))
                .append("（risk penalty clamp 0..100）<br>");
        sb.append("6) 当前关键阈值：")
                .append("scan.min_score=").append(escape(config.getString("scan.min_score"))).append("，")
                .append("filter.min_signals=").append(escape(config.getString("filter.min_signals"))).append("，")
                .append("filter.hard.max_drop_3d_pct=").append(escape(config.getString("filter.hard.max_drop_3d_pct"))).append("，")
                .append("rr.min=").append(escape(config.getString("rr.min"))).append("，")
                .append("plan.entry.buffer_pct=").append(escape(config.getString("plan.entry.buffer_pct"))).append("，")
                .append("plan.stop.atr_mult=").append(escape(config.getString("plan.stop.atr_mult")))
                .append("<br>");

        FeatureResolution perf = diagnostics == null ? null : diagnostics.feature("report.metrics.top5_perf");
        if (perf == null) {
            perf = new FeatureResolution(
                    "report.metrics.top5_perf.enabled",
                    config.getBoolean("report.metrics.top5_perf.enabled", false),
                    false,
                    FeatureStatus.DISABLED_NOT_IMPLEMENTED,
                    CauseCode.FEATURE_NOT_IMPLEMENTED,
                    OWNER_TOP5,
                    "feature not implemented",
                    ""
            );
        }
        sb.append("7) 近30日 Top5 胜率/最大回撤：")
                .append(renderFeatureStatus("report.metrics.top5_perf.enabled", perf, summary))
                .append("<br>");
        sb.append("</div>");
    }

/**
 * 方法说明：appendActionAdvice，负责追加组装输出片段。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void appendActionAdvice(StringBuilder sb, ActionAdvice advice, double singleMaxPct, double totalMaxPct) {
        sb.append("<h2>C. 今日行动建议（总览）</h2>");
        sb.append("<div class='card'>");
        sb.append("<div>建议等级：<span class='chip ").append(advice.css).append("'>").append(escape(advice.level)).append("</span></div>");
        sb.append("<div class='small' style='margin-top:6px;'>").append(escape(advice.reason)).append("</div>");
        sb.append("<div class='small' style='margin-top:6px;'>新手仓位上限：单笔 ≤ ").append(fmt1(singleMaxPct * 100.0))
                .append("%，总仓 ≤ ").append(fmt1(totalMaxPct * 100.0)).append("%</div>");
        sb.append("</div>");
    }

/**
 * 方法说明：appendWatchTable，负责追加组装输出片段。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void appendWatchTable(StringBuilder sb, List<WatchlistAnalysis> watchRows, boolean isClose) {
        sb.append("<h2>D. 自选股跟踪（精简表格）</h2>");
        sb.append("<table><tr><th>代码+名称</th><th>最新价</th><th>综合分(0-100)</th><th>风险等级</th><th>信号状态</th><th>")
                .append(isClose ? "止损位" : "风险线(止损意义)").append("</th></tr>");
        if (watchRows.isEmpty()) {
            sb.append("<tr><td colspan='6'>无自选股数据。</td></tr></table>");
            return;
        }
        for (WatchlistAnalysis row : watchRows) {
            IndicatorData ind = parseIndicators(row.technicalIndicatorsJson, row.lastClose);
            RiskAssessment risk = assessRisk(ind);
            Outcome<TradePlan> planOutcome = tradePlanBuilder.build(toTradePlanInput(ind));
            TradePlan plan = planOutcome.value == null ? TradePlan.invalid() : planOutcome.value;
            String signal = "CANDIDATE".equalsIgnoreCase(blankTo(row.technicalStatus, "")) ? "触发" : "未触发";
            String riskCss = risk.grade == RiskGrade.HIGH ? "bad" : (risk.grade == RiskGrade.MID ? "warn" : "good");
            JSONObject reasonRoot = safeJson(row.technicalReasonsJson);
            DisplayReason displayReason = resolveDisplayReason(row, reasonRoot, planOutcome);
            JSONObject fetchTrace = reasonRoot.optJSONObject("fetch_trace");
            sb.append("<tr").append(row.priceSuspect ? " class='suspect'" : "").append("><td>");
            sb.append(escape(watchName(row)));
            if (row.priceSuspect) {
                sb.append(" <span class='chip bad'>SUSPECT</span>");
            }
            sb.append("<details><summary class='small'>trace</summary><div class='small'>");
            sb.append("data_source=").append(escape(row.dataSource)).append(" | ");
            sb.append("price_timestamp=").append(escape(row.priceTimestamp)).append(" | ");
            sb.append("bars_count=").append(row.barsCount).append(" | ");
            sb.append("cache_hit=").append(row.cacheHit).append(" | ");
            sb.append("fetch_latency_ms=").append(row.fetchLatencyMs);
            if (fetchTrace != null) {
                sb.append(" | ticker_normalized=").append(escape(fetchTrace.optString("ticker_normalized", "")));
                sb.append(" | resolved_exchange=").append(escape(fetchTrace.optString("resolved_exchange", "")));
                sb.append(" | fetcher_class=").append(escape(fetchTrace.optString("fetcher_class", "")));
                sb.append(" | fallback_path=").append(escape(fetchTrace.optString("fallback_path", "")));
            }
            sb.append("</div></details>");
            appendWatchWhy(sb, displayReason);
            sb.append("</td>");
            sb.append("<td>").append(fmt2(row.lastClose)).append("</td>");
            sb.append("<td><span class='chip info'>").append(fmt1(row.technicalScore)).append("</span> ").append(escape(scoreTier(row.technicalScore))).append("</td>");
            sb.append("<td><span class='chip ").append(riskCss).append("'>").append(escape(riskText(risk))).append("</span></td>");
            sb.append("<td>").append(escape(signal)).append("</td>");
            sb.append("<td>").append(escape(plan.valid ? fmt2(plan.stopLoss) : displayReason.category)).append("</td></tr>");
        }
        sb.append("</table>");
        sb.append("<div class='small' style='margin-top:8px;'>说明：SMA/RSI/ATR/BB% 等原始指标不在正文展示。</div>");
    }

/**
 * 方法说明：appendWatchAiSummary，负责追加组装输出片段。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void appendWatchAiSummary(StringBuilder sb, List<WatchlistAnalysis> watchRows) {
        if (watchRows.isEmpty()) return;
        sb.append("<div class='card'><h3 style='margin-top:0;'>AI 鏂伴椈鎽樿锛堟渶澶?琛岋級</h3>");
        for (WatchlistAnalysis row : watchRows) {
            sb.append("<div style='margin-bottom:8px;'><b>").append(escape(watchName(row))).append("</b>");
            for (String line : aiSummaryLines(row)) {
                sb.append("<div class='small'>").append(escape(line)).append("</div>");
            }
            sb.append("</div>");
        }
        sb.append("</div>");
    }
/**
 * 方法说明：appendTopCards，负责追加组装输出片段。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void appendTopCards(StringBuilder sb, CandidateSelection topSelection, boolean isClose, boolean hideEntryInIntraday, Diagnostics diagnostics) {
        sb.append("<h2>E. 今日 Top 5 候选</h2>");
        appendTopFunnel(sb, topSelection.funnel, topSelection.skipReason, diagnostics);

        if (topSelection.cards.isEmpty()) {
            sb.append("<div class='warnbox'>");
            if (!safe(topSelection.skipReason).isEmpty()) {
                sb.append(escape(topSelection.skipReason));
            } else {
                sb.append("当前没有满足规则的 Top 5 候选（已被 gate 或规则过滤）。");
            }
            if (!topSelection.funnel.mainReasons.isEmpty()) {
                sb.append("<br>主要淘汰原因：").append(escape(String.join("；", topSelection.funnel.mainReasons)));
            }
            sb.append("</div>");
        } else {
            for (CandidateCard card : topSelection.cards) {
                String riskCss = card.risk.grade == RiskGrade.HIGH ? "bad" : (card.risk.grade == RiskGrade.MID ? "warn" : "good");
                sb.append("<div class='card'><div><b>#").append(card.rank).append(" ").append(escape(card.name)).append("</b></div>");
                sb.append("<div class='small'>最新价 ").append(fmt2(card.latestPrice))
                        .append(" | 综合分 <span class='chip info'>").append(fmt1(card.score)).append("</span>")
                        .append(" | 风险 <span class='chip ").append(riskCss).append("'>").append(escape(riskText(card.risk))).append("</span></div>");
                if (!card.risk.tags.isEmpty()) {
                    sb.append("<div class='small'>风险标签：").append(escape(String.join(", ", card.risk.tags))).append("</div>");
                }
                if (isClose || !hideEntryInIntraday) {
                    if (card.plan.valid) {
                        sb.append("<div class='small'>入场区间：").append(fmt2(card.plan.entryLow)).append(" ~ ").append(fmt2(card.plan.entryHigh))
                                .append(" | 止损位：").append(fmt2(card.plan.stopLoss))
                                .append(" | 目标位：").append(fmt2(card.plan.takeProfit))
                                .append(" | 盈亏比：").append(fmt2(card.plan.rrRatio)).append("</div>");
                    } else {
                        sb.append("<div class='small'>价位方案不可用（PLAN_UNAVAILABLE）。</div>");
                    }
                } else {
                    String change = card.isNewCandidate ? "新增候选" : "延续候选";
                    String delta = card.scoreDelta == null ? "分数变化：无历史数据" : ("分数变化：" + signed(card.scoreDelta));
                    String stopLine = card.plan.valid ? ("风险线(止损)：" + fmt2(card.plan.stopLoss)) : "风险线(止损)：PLAN_UNAVAILABLE";
                    sb.append("<div class='small'>").append(escape(change)).append(" | ").append(escape(delta)).append(" | ").append(escape(stopLine)).append("</div>");
                }
                sb.append("<ul class='bul'>");
                for (String reason : card.reasons) {
                    sb.append("<li>").append(escape(reason)).append("</li>");
                }
                sb.append("</ul></div>");
            }
        }

        if (!topSelection.excludedDerivatives.isEmpty()) {
            sb.append("<div class='warnbox'><b>衍生/对冲类（仅研究用）</b><br>已从 Top 候选剔除：")
                    .append(escape(String.join("; ", topSelection.excludedDerivatives)))
                    .append(".</div>");
        }
    }

/**
 * 方法说明：appendTopFunnel，负责追加组装输出片段。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void appendTopFunnel(StringBuilder sb, TopFunnel funnel, String skipReason, Diagnostics diagnostics) {
        sb.append("<div class='card'><h3 style='margin-top:0;'>Top5 漏斗统计</h3><div class='small'>");
        sb.append("candidates_from_market_scan=").append(funnel.candidatesFromMarketScan).append(" | ");
        sb.append("excluded_derivatives=").append(funnel.excludedDerivatives).append(" | ");
        sb.append("missing_indicators=").append(funnel.missingIndicators).append(" | ");
        sb.append("plan_invalid=").append(funnel.planInvalid).append(" | ");
        sb.append("score_below_threshold=").append(funnel.scoreBelowThreshold).append(" | ");
        sb.append("final_top5_count=").append(funnel.finalTop5Count);
        sb.append("</div>");

        if (funnel.finalTop5Count == 0 && !funnel.mainReasons.isEmpty()) {
            sb.append("<div class='small'>最主要淘汰原因：").append(escape(String.join("；", funnel.mainReasons))).append("</div>");
        }
        if (!safe(skipReason).isEmpty()) {
            sb.append("<div class='small'>").append(escape(skipReason)).append("</div>");
        }
        if (diagnostics != null && !diagnostics.top5Gates.isEmpty()) {
            sb.append("<details><summary class='small'>Top5 gate reason tree</summary><div class='small'>");
            for (Diagnostics.GateTrace gate : diagnostics.top5Gates) {
                sb.append("gate=").append(escape(gate.gate))
                        .append(" | passed=").append(gate.passed)
                        .append(" | fail_count=").append(gate.failCount)
                        .append(" | threshold=").append(escape(blankTo(gate.threshold, "-")))
                        .append(" | cause_code=").append(gate.causeCode.name())
                        .append(" | owner=").append(escape(gate.owner))
                        .append(" | details=").append(escape(blankTo(gate.details, "-")))
                        .append("<br>");
            }
            sb.append("</div></details>");
        }
        sb.append("</div>");
    }

/**
 * 方法说明：appendPolymarketSignals，负责追加组装输出片段。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void appendPolymarketSignals(StringBuilder sb, PolymarketSignalReport report, Diagnostics diagnostics) {
        sb.append("<h2>F. Polymarket Signals</h2>");
        FeatureResolution polymarketFeature = diagnostics == null ? null : diagnostics.feature("polymarket");
        if (polymarketFeature != null && polymarketFeature.status != FeatureStatus.ENABLED) {
            sb.append("<div class='warnbox'>")
                    .append(escape(renderFeatureStatusSimple("polymarket.enabled", polymarketFeature)))
                    .append("</div>");
            return;
        }
        if (report == null || !report.enabled) {
            String message = report == null ? "未启用" : report.statusMessage;
            sb.append("<div class='warnbox'>").append(escape(blankTo(message, "未启用"))).append("</div>");
            return;
        }
        if (report.signals.isEmpty()) {
            sb.append("<div class='warnbox'>").append(escape(blankTo(report.statusMessage, "无匹配市场"))).append("</div>");
            return;
        }
        if (!safe(report.statusMessage).isEmpty()) {
            sb.append("<div class='small'>状态：").append(escape(report.statusMessage)).append("</div>");
        }

        for (PolymarketTopicSignal signal : report.signals) {
            sb.append("<div class='card'>");
            sb.append("<div class='small'><b>Topic: ").append(escape(signal.topic))
                    .append(" | Implied Prob: ").append(fmt1(signal.impliedProbabilityPct)).append("%")
                    .append(" | 24h: ").append(signed(signal.change24hPct)).append("%")
                    .append(" | OI: ").append(escape(signal.oiDirection))
                    .append("</b></div>");
            sb.append("<div class='small'>Likely affected industries: ")
                    .append(escape(signal.likelyIndustries.isEmpty() ? "Unknown" : String.join(", ", signal.likelyIndustries)))
                    .append("</div>");
            sb.append("<div class='small'>Watchlist impact: ");
            if (signal.watchImpacts == null || signal.watchImpacts.isEmpty()) {
                sb.append("无明显映射");
            } else {
                List<String> parts = new ArrayList<>();
                for (PolymarketWatchImpact impact : signal.watchImpacts) {
                    String direction = "positive".equalsIgnoreCase(impact.impact) ? "+" : ("negative".equalsIgnoreCase(impact.impact) ? "-" : "卤");
                    parts.add(impact.code + " (" + direction + "," + fmt1(impact.confidence) + ",\"" + trimText(impact.rationale, 40) + "\")");
                }
                sb.append(escape(String.join(" / ", parts)));
            }
            sb.append("</div></div>");
        }
    }

/**
 * 方法说明：appendDisclaimer，负责追加组装输出片段。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void appendDisclaimer(StringBuilder sb) {
        sb.append("<h2>G. 风险提示与免责声明</h2><div class='disclaimer'>");
        sb.append("1) 本报告仅用于学习和研究，不构成投资建议。<br>");
        sb.append("2) 覆盖率低于80%时请降低权重，低于50%时不建议据此交易。<br>");
        sb.append("3) INTRADAY 仅用于观察，CLOSE 才提供完整入场/止损/目标/盈亏比。<br>");
        sb.append("4) 所有价位均为模型估算，需结合流动性和风险承受能力。<br>");
        sb.append("5) Polymarket 区块为解释层，不纳入综合评分。");
        sb.append("</div>");
    }

/**
 * 方法说明：appendDiagnostics，负责追加组装输出片段。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void appendDiagnostics(StringBuilder sb, Diagnostics diagnostics) {
        if (diagnostics == null) {
            return;
        }
        sb.append("<h2>H. Diagnostics</h2>");
        sb.append("<details><summary>diagnostics</summary><div class='small'>");
        sb.append("run_id=").append(diagnostics.runId).append(" | run_mode=").append(escape(blankTo(diagnostics.runMode, "-"))).append("<br>");
        sb.append("coverage_scope=").append(escape(blankTo(diagnostics.coverageScope, "-")))
                .append(" | coverage_source=").append(escape(blankTo(diagnostics.coverageSource, "-")))
                .append(" | coverage_owner=").append(escape(blankTo(diagnostics.coverageOwner, "-"))).append("<br>");

        if (!diagnostics.coverages.isEmpty()) {
            sb.append("<b>coverage_metrics</b><br>");
            for (Diagnostics.CoverageMetric m : diagnostics.coverages.values()) {
                sb.append(escape(m.key))
                        .append(": ")
                        .append(m.numerator)
                        .append("/")
                        .append(m.denominator)
                        .append(" (")
                        .append(fmt1(m.pct))
                        .append("%)")
                        .append(" | source=")
                        .append(escape(blankTo(m.source, "-")))
                        .append(" | owner=")
                        .append(escape(blankTo(m.owner, "-")))
                        .append("<br>");
            }
        }

        if (!diagnostics.dataSourceStats.isEmpty()) {
            sb.append("<b>data_sources</b><br>");
            for (Map.Entry<String, Integer> e : diagnostics.dataSourceStats.entrySet()) {
                sb.append(escape(e.getKey())).append("=").append(e.getValue()).append("<br>");
            }
        }

        if (!diagnostics.featureStatuses.isEmpty()) {
            sb.append("<b>feature_status</b><br>");
            for (Map.Entry<String, FeatureResolution> e : diagnostics.featureStatuses.entrySet()) {
                sb.append(escape(e.getKey())).append(": ")
                        .append(escape(renderFeatureStatusSimple(e.getKey(), e.getValue())))
                        .append("<br>");
            }
        }

        if (!diagnostics.configSnapshot.isEmpty()) {
            sb.append("<b>config_snapshot</b><br>");
            for (Diagnostics.ConfigItem item : diagnostics.configSnapshot.values()) {
                sb.append(escape(item.key))
                        .append("=")
                        .append(escape(blankTo(item.value, "")))
                        .append(" (source=")
                        .append(escape(blankTo(item.source, "default")))
                        .append(")")
                        .append("<br>");
            }
        }

        if (!diagnostics.notes.isEmpty()) {
            sb.append("<b>notes</b><br>");
            for (String note : diagnostics.notes) {
                sb.append(escape(note)).append("<br>");
            }
        }
        sb.append("</div></details>");
    }

/**
 * 方法说明：buildDebugJson，负责构建目标对象或输出内容。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String buildDebugJson(
            Instant startedAt,
            ZoneId zoneId,
            List<WatchlistAnalysis> watchlistCandidates,
            ScanResultSummary scanSummary,
            boolean marketScanPartial,
            String marketScanStatus,
            Diagnostics diagnostics
    ) {
        Diagnostics diag = diagnostics == null ? new Diagnostics(0L, "") : diagnostics;
        ScanResultSummary summary = scanSummary == null
                ? new ScanResultSummary(0, 0, 0, Map.of(), Map.of(), Map.of())
                : scanSummary;
        List<WatchlistAnalysis> watchRows = sortedWatch(watchlistCandidates);

        JSONObject root = new JSONObject();
        root.put("generated_at", startedAt == null || zoneId == null ? "" : startedAt.atZone(zoneId).toString());
        root.put("run_id", diag.runId);
        root.put("run_mode", safe(diag.runMode));
        root.put("market_scan_status", blankTo(marketScanStatus, "UNKNOWN"));
        root.put("market_scan_partial", marketScanPartial);
        root.put("coverage_scope", blankTo(diag.coverageScope, "MARKET"));
        root.put("coverage_source", blankTo(diag.coverageSource, "UNKNOWN"));

        JSONObject summaryJson = new JSONObject();
        summaryJson.put("total", summary.total);
        summaryJson.put("fetch_coverage", summary.fetchCoverage);
        summaryJson.put("indicator_coverage", summary.indicatorCoverage);
        root.put("scan_summary", summaryJson);

        JSONObject coverageJson = new JSONObject();
        for (Map.Entry<String, Diagnostics.CoverageMetric> e : diag.coverages.entrySet()) {
            coverageJson.put(e.getKey(), coverageToJson(e.getValue()));
        }
        root.put("coverages", coverageJson);

        JSONArray gates = new JSONArray();
        for (Diagnostics.GateTrace gate : diag.top5Gates) {
            JSONObject item = new JSONObject();
            item.put("gate", gate.gate);
            item.put("passed", gate.passed);
            item.put("fail_count", gate.failCount);
            item.put("threshold", gate.threshold);
            item.put("cause_code", gate.causeCode == null ? CauseCode.NONE.name() : gate.causeCode.name());
            item.put("owner", safe(gate.owner));
            item.put("details", safe(gate.details));
            gates.put(item);
        }
        root.put("top5_gates", gates);

        JSONArray rows = new JSONArray();
        for (WatchlistAnalysis row : watchRows) {
            JSONObject reasonRoot = safeJson(row.technicalReasonsJson);
            IndicatorData ind = parseIndicators(row.technicalIndicatorsJson, row.lastClose);
            Outcome<TradePlan> planOutcome = tradePlanBuilder.build(toTradePlanInput(ind));
            DisplayReason displayReason = resolveDisplayReason(row, reasonRoot, planOutcome);

            JSONObject details = reasonRoot.optJSONObject("details");
            if (details == null) {
                details = new JSONObject();
            }

            JSONObject item = new JSONObject();
            item.put("watch_item", safe(row.watchItem));
            item.put("ticker", safe(row.ticker));
            item.put("code", safe(row.code));
            item.put("resolved_market", safe(row.resolvedMarket));
            item.put("resolve_status", safe(row.resolveStatus));
            item.put("normalized_ticker", safe(row.normalizedTicker));
            item.put("technical_status", safe(row.technicalStatus));
            item.put("bars_count", row.barsCount);
            item.put("fetch_success", row.fetchSuccess);
            item.put("indicator_ready", row.indicatorReady);
            item.put("cause_code", displayReason.causeCode);
            item.put("owner", displayReason.owner);
            item.put("details", details);
            item.put("display_category", displayReason.category);
            item.put("user_message", displayReason.userMessage);

            JSONObject planJson = new JSONObject();
            planJson.put("valid", planOutcome != null && planOutcome.success);
            if (planOutcome != null && !planOutcome.success) {
                planJson.put("cause_code", planOutcome.causeCode == null ? CauseCode.NONE.name() : planOutcome.causeCode.name());
                planJson.put("owner", safe(planOutcome.owner));
                planJson.put("details", new JSONObject(planOutcome.details));
            }
            item.put("plan", planJson);

            rows.put(item);
        }
        root.put("watchlist", rows);
        return root.toString(2);
    }

/**
 * 方法说明：coverageToJson，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private JSONObject coverageToJson(Diagnostics.CoverageMetric metric) {
        JSONObject json = new JSONObject();
        if (metric == null) {
            json.put("numerator", 0);
            json.put("denominator", 0);
            json.put("pct", 0.0);
            json.put("source", "");
            json.put("owner", "");
            return json;
        }
        json.put("numerator", metric.numerator);
        json.put("denominator", metric.denominator);
        json.put("pct", round2(metric.pct));
        json.put("source", safe(metric.source));
        json.put("owner", safe(metric.owner));
        return json;
    }

/**
 * 方法说明：appendWatchWhy，负责追加组装输出片段。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void appendWatchWhy(StringBuilder sb, DisplayReason displayReason) {
        if (displayReason == null) {
            return;
        }
        sb.append("<div class='small'><b>")
                .append(escape(displayReason.category))
                .append("</b>: ")
                .append(escape(displayReason.userMessage))
                .append("</div>");
    }

/**
 * 方法说明：resolveDisplayReason，负责解析规则并确定最终结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private DisplayReason resolveDisplayReason(WatchlistAnalysis row, JSONObject reasonRoot, Outcome<TradePlan> planOutcome) {
        JSONObject root = reasonRoot == null ? new JSONObject() : reasonRoot;
        String causeRaw = root.optString("cause_code", row.fetchSuccess ? CauseCode.NONE.name() : CauseCode.FETCH_FAILED.name());
        CauseCode causeCode = toCauseCode(causeRaw);
        String owner = root.optString("owner", "com.stockbot.jp.runner.DailyRunner#scanWatchRecord(...)");
        JSONObject details = root.optJSONObject("details");
        if (details == null) {
            details = new JSONObject();
        }

        String category = "OK";
        String message = "Passed filters.";

        if (causeCode == CauseCode.TICKER_RESOLVE_FAILED) {
            category = "SYMBOL_ERROR";
            message = blankTo(details.optString("user_message", ""), "Symbol market mismatch: use ####.T (JP) or NVDA.US (US).");
        } else if ((causeCode == CauseCode.FETCH_FAILED || causeCode == CauseCode.NO_BARS) && row.barsCount <= 0) {
            category = "NO_MARKET_DATA";
            message = "No market data bars were fetched.";
        } else if (causeCode == CauseCode.HISTORY_SHORT) {
            category = "INSUFFICIENT_HISTORY";
            message = "History is too short for required indicators.";
        } else if (causeCode == CauseCode.INDICATOR_ERROR) {
            category = "INDICATOR_FAILURE";
            message = "Indicator calculation failed.";
        } else if (causeCode == CauseCode.PLAN_INVALID) {
            category = "PLAN_UNAVAILABLE";
            message = "Trade plan is unavailable.";
        } else if (causeCode == CauseCode.FILTER_REJECTED || causeCode == CauseCode.RISK_REJECTED || causeCode == CauseCode.SCORE_BELOW_THRESHOLD) {
            category = "REJECTED_BY_RULES";
            message = "Rejected by rules (" + filterReasonSummary(root) + ").";
        } else if (planOutcome != null && !planOutcome.success && planOutcome.causeCode == CauseCode.PLAN_INVALID) {
            category = "PLAN_UNAVAILABLE";
            message = "Trade plan is unavailable.";
        }

        return new DisplayReason(category, message, causeCode.name(), owner, details);
    }

/**
 * 方法说明：filterReasonSummary，负责按规则过滤无效数据。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String filterReasonSummary(JSONObject reasonRoot) {
        if (reasonRoot == null) {
            return "rule_not_met";
        }
        JSONArray reasons = reasonRoot.optJSONArray("filter_reasons");
        if (reasons == null || reasons.length() == 0) {
            return "rule_not_met";
        }
        List<String> items = new ArrayList<>();
        for (int i = 0; i < reasons.length(); i++) {
            String text = reasons.optString(i, "").trim();
            if (!text.isEmpty()) {
                items.add(text);
            }
            if (items.size() >= 2) {
                break;
            }
        }
        if (items.isEmpty()) {
            return "rule_not_met";
        }
        return String.join(", ", items);
    }

/**
 * 方法说明：toCauseCode，负责转换数据结构用于后续处理。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private CauseCode toCauseCode(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return CauseCode.NONE;
        }
        try {
            return CauseCode.valueOf(raw.trim().toUpperCase(Locale.ROOT));
        } catch (Exception ignored) {
            return CauseCode.NONE;
        }
    }
/**
 * 方法说明：renderFeatureStatus，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String renderFeatureStatus(String featureKey, FeatureResolution feature, ScanResultSummary summary) {
        if (feature == null) {
            return "<b>未启用</b>";
        }
        if (feature.status == FeatureStatus.ENABLED) {
            int shortCount = summary == null ? 0 : summary.insufficientCount(DataInsufficientReason.HISTORY_SHORT);
            if (shortCount > 0) {
                return "<b>INSUFFICIENT_HISTORY</b>（cause_code=HISTORY_SHORT，owner=com.stockbot.jp.db.ScanResultDao#summarizeByRun(...)，n=" + shortCount + "）";
            }
            double winRate = config.getDouble("report.metrics.top5_perf.win_rate_30d", Double.NaN);
            double drawdown = config.getDouble("report.metrics.top5_perf.max_drawdown_30d", Double.NaN);
            if (Double.isFinite(winRate) || Double.isFinite(drawdown)) {
                return "<b>" + fmt1(winRate) + "%</b>；近30日最大回撤：<b>" + fmt1(drawdown) + "%</b>";
            }
            return "<b>已启用</b>";
        }
        if (feature.status == FeatureStatus.DISABLED_BY_CONFIG) {
            return "<b>未启用</b>（config: " + featureKey + "=false）";
        }
        if (feature.status == FeatureStatus.DISABLED_NOT_IMPLEMENTED) {
            return "<b>未启用</b>（cause_code=" + feature.causeCode.name() + "，owner=" + escape(feature.owner) + "）";
        }
        return "<b>未启用</b>（runtime_error=" + escape(blankTo(feature.runtimeExceptionClass, "unknown")) + "）";
    }

/**
 * 方法说明：renderFeatureStatusSimple，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String renderFeatureStatusSimple(String featureKey, FeatureResolution feature) {
        if (feature == null) {
            return "disabled (unknown)";
        }
        if (feature.status == FeatureStatus.ENABLED) {
            return "enabled";
        }
        if (feature.status == FeatureStatus.DISABLED_BY_CONFIG) {
            return "disabled_by_config (" + featureKey + "=false)";
        }
        if (feature.status == FeatureStatus.DISABLED_NOT_IMPLEMENTED) {
            return "disabled_not_implemented (cause_code=" + feature.causeCode.name() + ", owner=" + feature.owner + ")";
        }
        return "disabled_runtime_error (" + blankTo(feature.runtimeExceptionClass, "unknown") + ")";
    }
/**
 * 方法说明：sortedWatch，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private List<WatchlistAnalysis> sortedWatch(List<WatchlistAnalysis> rows) {
        if (rows == null || rows.isEmpty()) return List.of();
        List<WatchlistAnalysis> out = new ArrayList<>(rows);
        out.sort(Comparator.comparingDouble((WatchlistAnalysis x) -> x.technicalScore).reversed());
        return out;
    }

/**
 * 方法说明：sortedMarket，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private List<ScoredCandidate> sortedMarket(List<ScoredCandidate> rows) {
        if (rows == null || rows.isEmpty()) return List.of();
        List<ScoredCandidate> out = new ArrayList<>(rows);
        out.sort(Comparator.comparingDouble((ScoredCandidate x) -> x.score).reversed());
        return out;
    }

/**
 * 方法说明：buildTopSelection，负责构建目标对象或输出内容。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private CandidateSelection buildTopSelection(
            List<ScoredCandidate> rows,
            RunType runType,
            Map<String, Double> prev,
            double minScore,
            int limit,
            int candidateSize,
            boolean marketScanPartial,
            double fetchCoveragePct,
            String marketScanStatus,
            Diagnostics diagnostics
    ) {
        TopFunnel funnel = new TopFunnel();
        funnel.candidatesFromMarketScan = Math.max(0, candidateSize);

        boolean skipOnPartial = config.getBoolean("report.top5.skip_on_partial", true);
        double minFetchCoverage = config.getDouble("report.top5.min_fetch_coverage_pct", 80.0);
        double allowPartialWhenCoverageGe = config.getDouble("report.top5.allow_partial_when_coverage_ge", 101.0);
        boolean partialBlocked = skipOnPartial && marketScanPartial && fetchCoveragePct < allowPartialWhenCoverageGe;
        if (diagnostics != null) {
            diagnostics.addTop5Gate(
                    "skip_on_partial",
                    !partialBlocked,
                    partialBlocked ? Math.max(0, candidateSize) : 0,
                    "report.top5.skip_on_partial=true && report.top5.allow_partial_when_coverage_ge=" + fmt1(allowPartialWhenCoverageGe),
                    CauseCode.GATE_SKIP_ON_PARTIAL,
                    OWNER_TOP5,
                    "market_scan_status=" + blankTo(marketScanStatus, "UNKNOWN") + ", fetch_coverage_pct=" + fmt1(fetchCoveragePct)
            );
        }
        if (partialBlocked || fetchCoveragePct < minFetchCoverage) {
            if (diagnostics != null) {
                diagnostics.addTop5Gate(
                        "min_fetch_coverage_pct",
                        fetchCoveragePct >= minFetchCoverage,
                        fetchCoveragePct < minFetchCoverage ? Math.max(0, candidateSize) : 0,
                        "report.top5.min_fetch_coverage_pct=" + fmt1(minFetchCoverage),
                        CauseCode.GATE_MIN_FETCH_COVERAGE,
                        OWNER_TOP5,
                        "fetch_coverage_pct=" + fmt1(fetchCoveragePct)
                );
            }
            String reason = partialBlocked
                    ? ("Top5：跳过（scan=" + blankTo(marketScanStatus, "PARTIAL") + " 且 coverage="
                    + fmt1(fetchCoveragePct) + "% < " + fmt1(minFetchCoverage) + "%）")
                    : ("Top5：跳过（coverage=" + fmt1(fetchCoveragePct) + "% < " + fmt1(minFetchCoverage) + "%）");
            funnel.mainReasons = List.of(reason);
            return new CandidateSelection(List.of(), List.of(), funnel, reason);
        }
        if (diagnostics != null) {
            diagnostics.addTop5Gate(
                    "min_fetch_coverage_pct",
                    true,
                    0,
                    "report.top5.min_fetch_coverage_pct=" + fmt1(minFetchCoverage),
                    CauseCode.NONE,
                    OWNER_TOP5,
                    "fetch_coverage_pct=" + fmt1(fetchCoveragePct)
            );
        }

        List<CandidateCard> cards = new ArrayList<>();
        List<String> excluded = new ArrayList<>();
        int rank = 1;

        for (ScoredCandidate c : rows) {
            if (c == null) continue;
            if (c.score < minScore) {
                funnel.scoreBelowThreshold++;
                continue;
            }
            if (isDerivative(c)) {
                funnel.excludedDerivatives++;
                excluded.add(candidateName(c));
                continue;
            }

            IndicatorData ind = parseIndicators(c.indicatorsJson, c.close);
            if (!ind.hasRiskInputs()) {
                funnel.missingIndicators++;
                continue;
            }

            Outcome<TradePlan> planOutcome = tradePlanBuilder.build(toTradePlanInput(ind));
            TradePlan plan = planOutcome.value == null ? TradePlan.invalid() : planOutcome.value;
            if (runType == RunType.CLOSE && !plan.valid) {
                funnel.planInvalid++;
                continue;
            }

            RiskAssessment risk = assessRisk(ind);
            List<String> reasons = normalizeReasons(c.reasonsJson, 3);
            if (reasons.isEmpty()) reasons = List.of("DATA_GAP");
            String key = safeTickerKey(c.ticker);
            Double old = prev == null ? null : prev.get(key);
            cards.add(new CandidateCard(rank++, candidateName(c), c.score, ind.lastClose, risk, reasons, plan, old == null, old == null ? null : c.score - old));
            if (cards.size() >= limit) break;
        }

        funnel.finalTop5Count = cards.size();
        if (cards.isEmpty()) {
            List<String> reasons = new ArrayList<>();
            if (funnel.missingIndicators > 0) reasons.add("missing_indicators n=" + funnel.missingIndicators);
            if (funnel.planInvalid > 0) reasons.add("plan_invalid n=" + funnel.planInvalid);
            if (funnel.excludedDerivatives > 0) reasons.add("excluded_derivatives n=" + funnel.excludedDerivatives);
            if (funnel.scoreBelowThreshold > 0) reasons.add("score_below_threshold n=" + funnel.scoreBelowThreshold);
            if (reasons.size() > 2) reasons = reasons.subList(0, 2);
            funnel.mainReasons = reasons;
        }
        if (diagnostics != null) {
            diagnostics.addTop5Gate(
                    "missing_indicators",
                    funnel.missingIndicators == 0,
                    funnel.missingIndicators,
                    "-",
                    CauseCode.MISSING_INDICATORS,
                    OWNER_TOP5,
                    "requires indicator fields for risk and plan"
            );
            diagnostics.addTop5Gate(
                    "plan_invalid",
                    funnel.planInvalid == 0,
                    funnel.planInvalid,
                    "runType=CLOSE",
                    CauseCode.PLAN_INVALID,
                    OWNER_TOP5,
                    "trade plan validation failed"
            );
            diagnostics.addTop5Gate(
                    "score_below_threshold",
                    funnel.scoreBelowThreshold == 0,
                    funnel.scoreBelowThreshold,
                    "scan.min_score=" + fmt1(minScore),
                    CauseCode.SCORE_BELOW_THRESHOLD,
                    OWNER_TOP5,
                    "candidate score below threshold"
            );
        }

        return new CandidateSelection(cards, excluded, funnel, "");
    }

/**
 * 方法说明：parseIndicators，负责解析输入内容并转换结构。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private IndicatorData parseIndicators(String json, double fallbackClose) {
        JSONObject root = safeJson(json);
        double last = finitePositive(root.optDouble("last_close", Double.NaN), fallbackClose);
        double sma20 = finitePositive(root.optDouble("sma20", Double.NaN), Double.NaN);
        double sma60 = finitePositive(root.optDouble("sma60", Double.NaN), Double.NaN);
        double sma60Prev5 = finitePositive(root.optDouble("sma60_prev5", Double.NaN), Double.NaN);
        double avgVol20 = finitePositive(root.optDouble("avg_volume20", Double.NaN), Double.NaN);
        double volRatio = root.optDouble("volatility20_ratio", Double.NaN);
        if (!Double.isFinite(volRatio)) {
            double volPct = root.optDouble("volatility20_pct", Double.NaN);
            volRatio = Double.isFinite(volPct) ? volPct / 100.0 : Double.NaN;
        }
        double atr14 = finitePositive(root.optDouble("atr14", Double.NaN), Double.NaN);
        double lowLookback = finitePositive(root.optDouble("low_lookback", Double.NaN), Double.NaN);
        double highLookback = finitePositive(root.optDouble("high_lookback", Double.NaN), Double.NaN);
        if (!Double.isFinite(lowLookback)) lowLookback = finitePositive(root.optDouble("bollinger_lower", Double.NaN), Double.NaN);
        if (!Double.isFinite(highLookback)) highLookback = finitePositive(root.optDouble("bollinger_upper", Double.NaN), Double.NaN);
        return new IndicatorData(last, sma20, sma60, sma60Prev5, avgVol20, volRatio, atr14, lowLookback, highLookback);
    }

/**
 * 方法说明：assessRisk，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private RiskAssessment assessRisk(IndicatorData ind) {
        double minVol = config.getDouble("risk.minVolume", 50000.0);
        double volMax = config.getDouble("risk.volMax", 0.06);
        boolean high = false;
        boolean mid = false;
        boolean missing = false;
        List<String> tags = new ArrayList<>();

        if (!Double.isFinite(ind.avgVolume20)) {
            missing = true;
        } else if (ind.avgVolume20 < minVol) {
            high = true;
            tags.add("LOW_LIQ");
        }
        if (!Double.isFinite(ind.volatility20Ratio)) {
            missing = true;
        } else if (ind.volatility20Ratio > volMax) {
            high = true;
            tags.add("HIGH_VOL");
        }
        if (!Double.isFinite(ind.lastClose) || !Double.isFinite(ind.sma60) || !Double.isFinite(ind.sma60Prev5)) {
            missing = true;
        } else if (ind.lastClose < ind.sma60 && ind.sma60 < ind.sma60Prev5) {
            mid = true;
            tags.add("DOWN_TREND");
        }

        if (tags.isEmpty() && missing) tags.add("DATA_GAP");
        if (tags.size() > 2) tags = new ArrayList<>(tags.subList(0, 2));

        RiskGrade grade = high ? RiskGrade.HIGH : (mid ? RiskGrade.MID : RiskGrade.LOW);
        if (missing && grade == RiskGrade.LOW) grade = RiskGrade.MID;
        return new RiskAssessment(grade, tags, missing);
    }

/**
 * 方法说明：toTradePlanInput，负责转换数据结构用于后续处理。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private TradePlanBuilder.Input toTradePlanInput(IndicatorData ind) {
        return new TradePlanBuilder.Input(
                ind.lastClose,
                ind.sma20,
                ind.lowLookback,
                ind.highLookback,
                ind.atr14
        );
    }
/**
 * 方法说明：normalizeReasons，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private List<String> normalizeReasons(String reasonsJson, int maxItems) {
        JSONObject root = safeJson(reasonsJson);
        JSONArray arr = root.optJSONArray("filter_reasons");
        if (arr == null || arr.length() == 0) return List.of();
        Set<String> out = new LinkedHashSet<>();
        for (int i = 0; i < arr.length(); i++) {
            String raw = arr.optString(i, "").trim().toLowerCase(Locale.ROOT);
            if (raw.isEmpty()) continue;
            out.add(raw.toUpperCase(Locale.ROOT));
            if (out.size() >= maxItems) break;
        }
        return new ArrayList<>(out);
    }

/**
 * 方法说明：aiSummaryLines，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private List<String> aiSummaryLines(WatchlistAnalysis row) {
        List<String> out = new ArrayList<>();
        String summary = blankTo(row.aiSummary, "").replace("\r", " ").replace("\n", " ").trim();
        if (summary.isEmpty()) {
            out.add("1) 今日关键事件：无重大");
            out.add("2) 利好：无重大；利空：无重大");
            out.add("3) 关注点：无重大");
            return out;
        }
        out.add("1) 今日关键事件：" + trimText(summary, 100));
        out.add("2) 利好：无重大；利空：无重大");
        out.add("3) 关注点：" + trimText(summary, 80));
        return out;
    }

/**
 * 方法说明：actionAdvice，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private ActionAdvice actionAdvice(double fetchCoveragePct, double indicatorCoveragePct, int candidateCount, List<CandidateCard> cards) {
        double fetchLowPct = config.getDouble("report.advice.fetch_low_pct", 50.0);
        double indicatorLowPct = config.getDouble("report.advice.indicator_low_pct", 50.0);
        double fetchWarnPct = config.getDouble("report.advice.fetch_warn_pct", 80.0);
        int candidateTryMax = Math.max(1, config.getInt("report.advice.candidate_try_max", 4));
        double avgScoreTryThreshold = config.getDouble("report.advice.avg_score_try_threshold", 72.0);
        double avgScore = cards.isEmpty() ? 0.0 : cards.stream().mapToDouble(c -> c.score).average().orElse(0.0);
        if (fetchCoveragePct < fetchLowPct) {
            return new ActionAdvice("观望", "bad", "FETCH_COVERAGE 低于" + fmt1(fetchLowPct) + "%，覆盖不足，不建议据此交易。");
        }
        if (indicatorCoveragePct < indicatorLowPct) {
            return new ActionAdvice("谨慎", "warn", "INDICATOR_COVERAGE 偏低，指标缺失较多。");
        }
        if (fetchCoveragePct < fetchWarnPct) {
            return new ActionAdvice("谨慎", "warn", "FETCH_COVERAGE 在" + fmt1(fetchLowPct) + "%~" + fmt1(fetchWarnPct) + "%，数据不完整。");
        }
        if (candidateCount <= 1) {
            return new ActionAdvice("观望", "gray", "候选数量过少，信号不足。");
        }
        if (candidateCount <= candidateTryMax || avgScore < avgScoreTryThreshold) {
            return new ActionAdvice("小仓试错", "info", "信号强度一般，建议小仓验证。");
        }
        return new ActionAdvice("正常", "good", "覆盖率与候选数量均正常。");
    }

/**
 * 方法说明：countPriceSuspects，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private int countPriceSuspects(List<WatchlistAnalysis> rows) {
        int count = 0;
        if (rows == null) return 0;
        for (WatchlistAnalysis row : rows) {
            if (row != null && row.priceSuspect) count++;
        }
        return count;
    }

/**
 * 方法说明：joinSuspectTickers，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String joinSuspectTickers(List<WatchlistAnalysis> rows) {
        LinkedHashSet<String> out = new LinkedHashSet<>();
        if (rows != null) {
            for (WatchlistAnalysis row : rows) {
                if (row != null && row.priceSuspect) {
                    String code = blankTo(row.code, row.ticker).toUpperCase(Locale.ROOT);
                    if (!code.isEmpty()) out.add(code);
                }
            }
        }
        return out.isEmpty() ? "-" : String.join(", ", out);
    }

/**
 * 方法说明：watchName，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String watchName(WatchlistAnalysis row) {
        String code = blankTo(row.code, "").toUpperCase(Locale.ROOT);
        String name = blankTo(row.companyNameLocal, blankTo(row.displayName, blankTo(row.ticker, "")));
        return code.isEmpty() ? name : (code + " " + name);
    }

/**
 * 方法说明：candidateName，负责判断条件是否满足。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String candidateName(ScoredCandidate c) {
        String code = blankTo(c.code, "").toUpperCase(Locale.ROOT);
        String name = blankTo(c.name, blankTo(c.ticker, ""));
        return code.isEmpty() ? name : (code + " " + name);
    }

/**
 * 方法说明：isDerivative，负责判断条件是否满足。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private boolean isDerivative(ScoredCandidate c) {
        String text = (blankTo(c.name, "") + " " + blankTo(c.market, "") + " " + blankTo(c.code, "")).toUpperCase(Locale.ROOT);
        for (String kw : DERIVATIVE_KEYWORDS) {
            if (text.contains(kw.toUpperCase(Locale.ROOT))) return true;
        }
        return false;
    }

/**
 * 方法说明：scoreTier，负责计算评分并输出分值。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String scoreTier(double score) {
        double focusThreshold = config.getDouble("report.score.tier.focus_threshold", 80.0);
        double observeThreshold = config.getDouble("report.score.tier.observe_threshold", 65.0);
        if (score >= focusThreshold) return "关注";
        if (score >= observeThreshold) return "观察";
        return "弱";
    }

/**
 * 方法说明：riskText，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String riskText(RiskAssessment risk) {
        return risk.grade == RiskGrade.HIGH ? "高" : (risk.grade == RiskGrade.MID ? "中" : "低");
    }

/**
 * 方法说明：safeTickerKey，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String safeTickerKey(String ticker) {
        return blankTo(ticker, "").toLowerCase(Locale.ROOT);
    }

/**
 * 方法说明：tile，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String tile(String key, String value) {
        return "<div class='tile'><div class='k'>" + escape(key) + "</div><div class='v'>" + value + "</div></div>";
    }

/**
 * 方法说明：safeJson，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private JSONObject safeJson(String raw) {
        if (raw == null || raw.trim().isEmpty()) return new JSONObject();
        try {
            return new JSONObject(raw);
        } catch (Exception ignored) {
            return new JSONObject();
        }
    }

/**
 * 方法说明：finitePositive，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double finitePositive(double first, double fallback) {
        if (Double.isFinite(first) && first > 0.0) return first;
        return (Double.isFinite(fallback) && fallback > 0.0) ? fallback : Double.NaN;
    }

/**
 * 方法说明：coveragePct，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double coveragePct(int universeSize, int scannedSize) {
        if (universeSize <= 0) return 0.0;
        return scannedSize * 100.0 / universeSize;
    }

/**
 * 方法说明：formatCoverage，负责格式化数据用于展示或传输。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String formatCoverage(Diagnostics.CoverageMetric metric) {
        if (metric == null) {
            return "-";
        }
        return metric.numerator + " / " + metric.denominator + " (" + fmt1(metric.pct) + "%)";
    }

/**
 * 方法说明：round2，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

/**
 * 方法说明：clamp，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double clamp(double value, double min, double max) {
        if (!Double.isFinite(value)) return min;
        if (value < min) return min;
        if (value > max) return max;
        return value;
    }

/**
 * 方法说明：fmt1，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String fmt1(double value) {
        return Double.isFinite(value) ? String.format(Locale.US, "%.1f", value) : "-";
    }

/**
 * 方法说明：fmt2，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String fmt2(double value) {
        return Double.isFinite(value) ? String.format(Locale.US, "%.2f", value) : "-";
    }

/**
 * 方法说明：signed，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String signed(double value) {
        return Double.isFinite(value) ? String.format(Locale.US, "%+.2f", value) : "-";
    }

/**
 * 方法说明：trimText，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String trimText(String text, int maxLen) {
        String t = blankTo(text, "");
        if (t.length() <= maxLen) return t;
        return t.substring(0, Math.max(0, maxLen - 3)) + "...";
    }

/**
 * 方法说明：blankTo，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String blankTo(String value, String fallback) {
        String t = value == null ? "" : value.trim();
        return t.isEmpty() ? fallback : t;
    }

/**
 * 方法说明：safe，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String safe(String value) {
        return value == null ? "" : value.trim();
    }

/**
 * 方法说明：escape，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String escape(String text) {
        if (text == null) return "";
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
    }

    private static final class DisplayReason {
        final String category;
        final String userMessage;
        final String causeCode;
        final String owner;
        final JSONObject details;

        private DisplayReason(String category, String userMessage, String causeCode, String owner, JSONObject details) {
            this.category = category == null ? "OK" : category;
            this.userMessage = userMessage == null ? "" : userMessage;
            this.causeCode = causeCode == null ? CauseCode.NONE.name() : causeCode;
            this.owner = owner == null ? "" : owner;
            this.details = details == null ? new JSONObject() : details;
        }
    }

    private static final class CandidateSelection {
        final List<CandidateCard> cards;
        final List<String> excludedDerivatives;
        final TopFunnel funnel;
        final String skipReason;

        private CandidateSelection(List<CandidateCard> cards, List<String> excludedDerivatives, TopFunnel funnel, String skipReason) {
            this.cards = cards == null ? List.of() : cards;
            this.excludedDerivatives = excludedDerivatives == null ? List.of() : excludedDerivatives.stream().limit(6).collect(Collectors.toList());
            this.funnel = funnel == null ? new TopFunnel() : funnel;
            this.skipReason = skipReason == null ? "" : skipReason;
        }
    }

    private static final class TopFunnel {
        int candidatesFromMarketScan;
        int excludedDerivatives;
        int missingIndicators;
        int planInvalid;
        int scoreBelowThreshold;
        int finalTop5Count;
        List<String> mainReasons = List.of();
    }

    private static final class CandidateCard {
        final int rank;
        final String name;
        final double score;
        final double latestPrice;
        final RiskAssessment risk;
        final List<String> reasons;
        final TradePlan plan;
        final boolean isNewCandidate;
        final Double scoreDelta;

        private CandidateCard(int rank, String name, double score, double latestPrice, RiskAssessment risk, List<String> reasons, TradePlan plan, boolean isNewCandidate, Double scoreDelta) {
            this.rank = rank;
            this.name = name;
            this.score = score;
            this.latestPrice = latestPrice;
            this.risk = risk;
            this.reasons = reasons == null ? List.of() : reasons;
            this.plan = plan;
            this.isNewCandidate = isNewCandidate;
            this.scoreDelta = scoreDelta;
        }
    }

    private static final class IndicatorData {
        final double lastClose;
        final double sma20;
        final double sma60;
        final double sma60Prev5;
        final double avgVolume20;
        final double volatility20Ratio;
        final double atr14;
        final double lowLookback;
        final double highLookback;

        private IndicatorData(double lastClose, double sma20, double sma60, double sma60Prev5, double avgVolume20, double volatility20Ratio, double atr14, double lowLookback, double highLookback) {
            this.lastClose = lastClose;
            this.sma20 = sma20;
            this.sma60 = sma60;
            this.sma60Prev5 = sma60Prev5;
            this.avgVolume20 = avgVolume20;
            this.volatility20Ratio = volatility20Ratio;
            this.atr14 = atr14;
            this.lowLookback = lowLookback;
            this.highLookback = highLookback;
        }

/**
 * 方法说明：hasRiskInputs，负责判断条件是否满足。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
        private boolean hasRiskInputs() {
            return Double.isFinite(lastClose) && lastClose > 0.0
                    && Double.isFinite(avgVolume20)
                    && Double.isFinite(volatility20Ratio)
                    && Double.isFinite(sma60)
                    && Double.isFinite(sma60Prev5);
        }

    }

    private static final class RiskAssessment {
        final RiskGrade grade;
        final List<String> tags;
        final boolean insufficient;

        private RiskAssessment(RiskGrade grade, List<String> tags, boolean insufficient) {
            this.grade = grade;
            this.tags = tags == null ? List.of() : tags;
            this.insufficient = insufficient;
        }
    }

    private static final class ActionAdvice {
        final String level;
        final String css;
        final String reason;

        private ActionAdvice(String level, String css, String reason) {
            this.level = level;
            this.css = css;
            this.reason = reason;
        }
    }
}



