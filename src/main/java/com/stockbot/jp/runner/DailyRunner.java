package com.stockbot.jp.runner;

import com.stockbot.app.Prompts;
import com.stockbot.core.diagnostics.CauseCode;
import com.stockbot.core.diagnostics.Diagnostics;
import com.stockbot.core.diagnostics.FeatureResolution;
import com.stockbot.core.diagnostics.FeatureStatusResolver;
import com.stockbot.core.diagnostics.Outcome;
import com.stockbot.data.IndustryService;
import com.stockbot.data.MacroService;
import com.stockbot.data.MarketDataService;
import com.stockbot.data.NewsService;
import com.stockbot.data.OllamaClient;
import com.stockbot.data.http.HttpClientEx;
import com.stockbot.factors.FactorEngine;
import com.stockbot.model.DailyPrice;
import com.stockbot.model.NewsItem;
import com.stockbot.model.StockContext;
import com.stockbot.scoring.GatePolicy;
import com.stockbot.jp.config.Config;
import com.stockbot.jp.db.BarDailyDao;
import com.stockbot.jp.db.MetadataDao;
import com.stockbot.jp.db.RunDao;
import com.stockbot.jp.db.ScanResultDao;
import com.stockbot.jp.db.UniverseDao;
import com.stockbot.jp.indicator.IndicatorEngine;
import com.stockbot.jp.model.BarDaily;
import com.stockbot.jp.model.DataInsufficientReason;
import com.stockbot.jp.model.DailyRunOutcome;
import com.stockbot.jp.model.FilterDecision;
import com.stockbot.jp.model.IndicatorSnapshot;
import com.stockbot.jp.model.RiskDecision;
import com.stockbot.jp.model.RunRow;
import com.stockbot.jp.model.ScanFailureReason;
import com.stockbot.jp.model.ScanResultSummary;
import com.stockbot.jp.model.ScoreResult;
import com.stockbot.jp.model.ScoredCandidate;
import com.stockbot.jp.model.TickerScanResult;
import com.stockbot.jp.model.UniverseRecord;
import com.stockbot.jp.model.UniverseUpdateResult;
import com.stockbot.jp.model.WatchlistAnalysis;
import com.stockbot.jp.output.ReportBuilder;
import com.stockbot.jp.polymarket.PolymarketSignalReport;
import com.stockbot.jp.polymarket.PolymarketService;
import com.stockbot.jp.strategy.CandidateFilter;
import com.stockbot.jp.strategy.ReasonJsonBuilder;
import com.stockbot.jp.strategy.RiskFilter;
import com.stockbot.jp.strategy.ScoringEngine;
import com.stockbot.jp.universe.JpxUniverseUpdater;
import com.stockbot.jp.watch.TickerResolver;
import com.stockbot.jp.watch.TickerSpec;
import org.json.JSONArray;
import org.json.JSONObject;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 模块说明：DailyRunner（class）。
 * 主要职责：承载 runner 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class DailyRunner {
    public static final String RUN_MODE_DAILY = "DAILY";
    public static final String RUN_MODE_MARKET_SCAN = "DAILY_MARKET_SCAN";
    public static final String RUN_MODE_DAILY_REPORT = "DAILY_REPORT";
    private static final String OWNER_RUNNER = "com.stockbot.jp.runner.DailyRunner";
    private static final String OWNER_SCAN_SUMMARY = OWNER_RUNNER + "#loadScanSummary(...)";
    private static final String OWNER_DERIVE_COVERAGE = OWNER_RUNNER + "#deriveCoverageFromRunAndCandidates(...)";
    private static final String OWNER_WATCH_SCAN = OWNER_RUNNER + "#scanWatchRecord(...)";
    private static final String OWNER_WATCH_FETCH = OWNER_RUNNER + "#fetchWatchPriceTrace(...)";
    private static final String OWNER_WATCH_RESOLVE = "com.stockbot.jp.watch.TickerResolver#resolve(...)";
    private static final String OWNER_FEATURE_RESOLVE = "com.stockbot.core.diagnostics.FeatureStatusResolver#resolveFeatureStatus(...)";
    private static final String OWNER_FILTER = "com.stockbot.jp.strategy.CandidateFilter#evaluate(...)";
    private static final String OWNER_RISK = "com.stockbot.jp.strategy.RiskFilter#evaluate(...)";
    private static final String OWNER_SCORE = "com.stockbot.jp.strategy.ScoringEngine#score(...)";
    private static final List<String> DIAGNOSTIC_CONFIG_KEYS = List.of(
            "app.zone",
            "app.schedule.enabled",
            "schedule.zone",
            "schedule.times",
            "schedule.time",
            "report.dir",
            "scan.top_n",
            "scan.market_reference_top_n",
            "scan.min_score",
            "scan.min_history_bars",
            "scan.fresh_days",
            "scan.cache.fresh_days",
            "backtest.hold_days",
            "report.top5.skip_on_partial",
            "report.top5.min_fetch_coverage_pct",
            "report.top5.allow_partial_when_coverage_ge",
            "report.metrics.top5_perf.enabled",
            "report.metrics.top5_perf.win_rate_30d",
            "report.metrics.top5_perf.max_drawdown_30d",
            "report.coverage.show_scope",
            "report.mode.intraday.hideEntry",
            "report.advice.fetch_low_pct",
            "report.advice.indicator_low_pct",
            "report.advice.fetch_warn_pct",
            "report.advice.candidate_try_max",
            "report.advice.avg_score_try_threshold",
            "report.score.tier.focus_threshold",
            "report.score.tier.observe_threshold",
            "filter.min_signals",
            "filter.hard.max_drop_3d_pct",
            "risk.max_atr_pct",
            "risk.max_volatility_pct",
            "risk.max_drawdown_pct",
            "risk.min_volume_ratio",
            "risk.fail_atr_multiplier",
            "risk.fail_volatility_multiplier",
            "risk.penalty.atr_scale",
            "risk.penalty.atr_cap",
            "risk.penalty.volatility_scale",
            "risk.penalty.volatility_cap",
            "risk.penalty.drawdown_scale",
            "risk.penalty.drawdown_cap",
            "risk.penalty.liquidity",
            "rr.min",
            "plan.rr.min_floor",
            "plan.entry.buffer_pct",
            "plan.stop.atr_mult",
            "plan.target.high_lookback_mult",
            "position.single.maxPct",
            "position.total.maxPct",
            "report.position.max_single_pct",
            "report.position.max_total_pct",
            "polymarket.enabled",
            "polymarket.impact.mode",
            "watchlist.path",
            "watchlist.non_jp_handling",
            "watchlist.default_market_for_alpha"
    );

    private enum NonJpHandling {
        SKIP_WITH_REASON,
        PROCESS_SEPARATELY
    }

    private final Config config;
    private final UniverseDao universeDao;
    private final MetadataDao metadataDao;
    private final BarDailyDao barDailyDao;
    private final RunDao runDao;
    private final ScanResultDao scanResultDao;
    private final IndicatorEngine indicatorEngine;
    private final CandidateFilter candidateFilter;
    private final RiskFilter riskFilter;
    private final ScoringEngine scoringEngine;
    private final ReasonJsonBuilder reasonJsonBuilder;
    private final ReportBuilder reportBuilder;
    private final HttpClientEx legacyHttp;
    private final MarketDataService marketDataService;
    private final IndustryService industryService;
    private final NewsService newsService;
    private final FactorEngine factorEngine;
    private final com.stockbot.scoring.ScoringEngine legacyScoringEngine;
    private final GatePolicy gatePolicy;
    private final OllamaClient ollamaClient;
    private final PolymarketService polymarketService;
    private final TickerResolver tickerResolver;
    private final NonJpHandling nonJpHandling;
    private final int watchlistMaxAiChars;
    private final int maxBars;
    private static final DateTimeFormatter NEWS_TS_FMT = DateTimeFormatter.ofPattern("MM-dd HH:mm");

/**
 * 方法说明：DailyRunner，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public DailyRunner(
            Config config,
            UniverseDao universeDao,
            MetadataDao metadataDao,
            BarDailyDao barDailyDao,
            RunDao runDao,
            ScanResultDao scanResultDao
    ) {
        this.config = config;
        this.universeDao = universeDao;
        this.metadataDao = metadataDao;
        this.barDailyDao = barDailyDao;
        this.runDao = runDao;
        this.scanResultDao = scanResultDao;
        this.indicatorEngine = new IndicatorEngine(Math.max(5, config.getInt("stop.loss.lookbackDays", 20)));
        this.candidateFilter = new CandidateFilter(config);
        this.riskFilter = new RiskFilter(config);
        this.scoringEngine = new ScoringEngine(config);
        this.reasonJsonBuilder = new ReasonJsonBuilder();
        this.reportBuilder = new ReportBuilder(config);
        this.legacyHttp = new HttpClientEx();
        this.marketDataService = new MarketDataService(legacyHttp);
        this.industryService = new IndustryService(legacyHttp);
        String newsLang = config.getString("watchlist.news.lang", "ja");
        String newsRegion = config.getString("watchlist.news.region", "JP");
        int newsMaxItems = Math.max(1, config.getInt("watchlist.news.max_items", 12));
        String newsSources = config.getString(
                "watchlist.news.sources",
                "google,bing,yahoo,cnbc,marketwatch,wsj,nytimes,yahoonews"
        );
        int queryVariants = Math.max(1, config.getInt("watchlist.news.query_variants", 4));
        this.newsService = new NewsService(legacyHttp, newsLang, newsRegion, newsMaxItems, newsSources, queryVariants);
        this.factorEngine = new FactorEngine(
                new com.stockbot.data.FundamentalsService(legacyHttp),
                industryService,
                new MacroService(marketDataService)
        );
        this.legacyScoringEngine = new com.stockbot.scoring.ScoringEngine();
        this.gatePolicy = new GatePolicy(
                config.getDouble("watchlist.ai.score_threshold", -2.0),
                Math.max(1, config.getInt("watchlist.ai.news_min", 8)),
                config.getDouble("watchlist.ai.drop_pct_threshold", -2.0)
        );
        this.ollamaClient = new OllamaClient(
                legacyHttp,
                config.getString("watchlist.ai.base_url", "http://127.0.0.1:11434"),
                config.getString("watchlist.ai.model", "llama3.1:latest"),
                Math.max(5, config.getInt("watchlist.ai.timeout_sec", 180)),
                Math.max(0, config.getInt("watchlist.ai.max_tokens", 80))
        );
        this.polymarketService = new PolymarketService(config, legacyHttp, ollamaClient);
        this.tickerResolver = new TickerResolver(config.getString("watchlist.default_market_for_alpha", "US"));
        this.nonJpHandling = parseNonJpHandling(config.getString("watchlist.non_jp_handling", "PROCESS_SEPARATELY"));
        this.watchlistMaxAiChars = Math.max(120, config.getInt("watchlist.ai.max_chars", 900));
        this.maxBars = Math.max(60, config.getInt("yahoo.max_bars_per_ticker", 420));
    }

/**
 * 方法说明：run，负责执行核心流程并返回执行结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public DailyRunOutcome run(boolean forceUniverseUpdate, Integer topNOverride) throws Exception {
        return run(forceUniverseUpdate, topNOverride, false, List.of());
    }

/**
 * 方法说明：run，负责执行核心流程并返回执行结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public DailyRunOutcome run(boolean forceUniverseUpdate, Integer topNOverride, boolean resetBatchCheckpoint) throws Exception {
        return run(forceUniverseUpdate, topNOverride, resetBatchCheckpoint, List.of());
    }

/**
 * 方法说明：run，负责执行核心流程并返回执行结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public DailyRunOutcome run(
            boolean forceUniverseUpdate,
            Integer topNOverride,
            boolean resetBatchCheckpoint,
            List<String> watchlist
    ) throws Exception {
        Instant startedAt = Instant.now();
        long runId = runDao.startRun(RUN_MODE_DAILY, "JP all-market scanner");
        Diagnostics diagnostics = new Diagnostics(runId, RUN_MODE_DAILY);
        captureConfigSnapshot(diagnostics);
        try {
            MarketScanSnapshot scan = executeMarketScan(runId, forceUniverseUpdate, topNOverride, resetBatchCheckpoint);
            List<WatchlistAnalysis> watchlistCandidates = analyzeWatchlist(watchlist, scan.universe);
            addMarketDataSourceStats(diagnostics, runId, scan.stats);
            addWatchlistCoverageDiagnostics(diagnostics, watchlistCandidates);
            Map<String, Double> previousScores = loadPreviousCandidateScoreMap(
                    runId,
                    Math.max(scan.topN, config.getInt("scan.top_n", 15))
            );
            runDao.insertCandidates(runId, scan.topCandidates);

            ZoneId zoneId = ZoneId.of(config.getString("app.zone", "Asia/Tokyo"));
            Path reportDir = config.getPath("report.dir");
            ReportBuilder.RunType runType = ReportBuilder.detectRunType(startedAt, zoneId);
            ScanSummaryEnvelope scanSummaryEnvelope = loadScanSummary(
                    runId,
                    scan.stats,
                    null,
                    Math.max(0, scan.stats.scanned + scan.stats.failed)
            );
            ScanResultSummary scanSummary = scanSummaryEnvelope.summary;
            addMarketCoverageDiagnostics(
                    diagnostics,
                    scanSummary,
                    Math.max(0, scan.stats.scanned + scan.stats.failed),
                    "MARKET",
                    scanSummaryEnvelope.source,
                    scanSummaryEnvelope.owner
            );
            diagnostics.addFeatureStatus(
                    "report.metrics.top5_perf",
                    FeatureStatusResolver.resolveFeatureStatus(
                            "report.metrics.top5_perf.enabled",
                            config.getBoolean("report.metrics.top5_perf.enabled", false),
                            false,
                            null,
                            OWNER_FEATURE_RESOLVE
                    )
            );
            PolymarketResult polymarket = collectPolymarketSignals(watchlistCandidates, diagnostics);
            Path reportPath = reportBuilder.writeDailyReport(
                    reportDir,
                    startedAt,
                    zoneId,
                    scan.universeSize,
                    scan.stats.scanned,
                    scan.stats.candidateCount,
                    scan.topN,
                    watchlist,
                    watchlistCandidates,
                    scan.topCandidates,
                    config.getDouble("scan.min_score", 55.0),
                    runType,
                    previousScores,
                    scanSummary,
                    scan.partialRun,
                    scan.partialRun ? "PARTIAL" : "SUCCESS",
                    polymarket.report,
                    diagnostics
            );

            String notes = String.format(
                    Locale.US,
                    "universe_update=%s; failures=%d; message=%s; batch_progress=%d/%d; partial=%s; watchlist=%d; market_ref_top=%d",
                    scan.updateResult.updated,
                    scan.stats.failed,
                    scan.updateResult.message,
                    scan.nextSegmentIndex,
                    scan.totalSegments,
                    scan.partialRun,
                    watchlistCandidates.size(),
                    scan.marketReferenceCandidates.size()
            );
            runDao.finishRun(
                    runId,
                    scan.partialRun ? "PARTIAL" : "SUCCESS",
                    scan.universeSize,
                    scan.stats.scanned,
                    scan.stats.candidateCount,
                    scan.topN,
                    reportPath.toString(),
                    notes
            );
            System.out.println(String.format(
                    Locale.US,
                    "Watchlist analysis complete. analyzed=%d",
                    watchlistCandidates.size()
            ));
            System.out.println(String.format(
                    Locale.US,
                    "Market reference candidates ready. top=%d",
                    scan.marketReferenceCandidates.size()
            ));

            return new DailyRunOutcome(
                    runId,
                    startedAt,
                    scan.updateResult,
                    scan.universeSize,
                    scan.stats.scanned,
                    scan.stats.failed,
                    scan.stats.candidateCount,
                    scan.topN,
                    reportPath,
                    scan.topCandidates,
                    watchlistCandidates,
                    scan.marketReferenceCandidates,
                    scan.totalSegments,
                    scan.nextSegmentIndex,
                    scan.nextSegmentIndex,
                    scan.partialRun
            );
        } catch (Exception e) {
            safeFinishFailed(runId, e);
            throw e;
        }
    }

/**
 * 方法说明：runMarketScanOnly，负责执行核心流程并返回执行结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public DailyRunOutcome runMarketScanOnly(
            boolean forceUniverseUpdate,
            Integer topNOverride,
            boolean resetBatchCheckpoint
    ) throws Exception {
        Instant startedAt = Instant.now();
        long runId = runDao.startRun(RUN_MODE_MARKET_SCAN, "JP all-market background scanner");
        try {
            MarketScanSnapshot scan = executeMarketScan(runId, forceUniverseUpdate, topNOverride, resetBatchCheckpoint);
            runDao.insertCandidates(runId, scan.topCandidates);
            String notes = String.format(
                    Locale.US,
                    "background_scan=true; universe_update=%s; failures=%d; message=%s; batch_progress=%d/%d; partial=%s; watchlist=0; market_ref_top=%d",
                    scan.updateResult.updated,
                    scan.stats.failed,
                    scan.updateResult.message,
                    scan.nextSegmentIndex,
                    scan.totalSegments,
                    scan.partialRun,
                    scan.marketReferenceCandidates.size()
            );
            runDao.finishRun(
                    runId,
                    scan.partialRun ? "PARTIAL" : "SUCCESS",
                    scan.universeSize,
                    scan.stats.scanned,
                    scan.stats.candidateCount,
                    scan.topN,
                    null,
                    notes
            );
            return new DailyRunOutcome(
                    runId,
                    startedAt,
                    scan.updateResult,
                    scan.universeSize,
                    scan.stats.scanned,
                    scan.stats.failed,
                    scan.stats.candidateCount,
                    scan.topN,
                    null,
                    scan.topCandidates,
                    List.of(),
                    scan.marketReferenceCandidates,
                    scan.totalSegments,
                    scan.nextSegmentIndex,
                    scan.nextSegmentIndex,
                    scan.partialRun
            );
        } catch (Exception e) {
            safeFinishFailed(runId, e);
            throw e;
        }
    }

/**
 * 方法说明：runWatchlistReportFromLatestMarket，负责执行核心流程并返回执行结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public DailyRunOutcome runWatchlistReportFromLatestMarket(List<String> watchlist) throws Exception {
        Instant startedAt = Instant.now();
        long runId = runDao.startRun(RUN_MODE_DAILY_REPORT, "watchlist report merged with latest market scan");
        Diagnostics diagnostics = new Diagnostics(runId, RUN_MODE_DAILY_REPORT);
        captureConfigSnapshot(diagnostics);
        try {
            Optional<RunRow> latestMarketRun = runDao.findLatestMarketScanRun();
            if (latestMarketRun.isEmpty()) {
                throw new IllegalStateException(
                        "No usable market scan result found (missing run or candidate rows). "
                                + "Background scanner may not have finished yet, or the latest scan produced zero candidates."
                );
            }
            RunRow marketRun = latestMarketRun.get();

            int topN = marketRun.topN > 0 ? marketRun.topN : Math.max(1, config.getInt("scan.top_n", 15));
            List<ScoredCandidate> topCandidates = runDao.listScoredCandidates(marketRun.id, topN);
            if (topCandidates.isEmpty()) {
                throw new IllegalStateException("Latest market scan has no candidate rows. run_id=" + marketRun.id);
            }
            int marketReferenceTopN = Math.max(1, config.getInt("scan.market_reference_top_n", 5));
            List<ScoredCandidate> marketReferenceCandidates = topCandidates.size() <= marketReferenceTopN
                    ? new ArrayList<>(topCandidates)
                    : new ArrayList<>(topCandidates.subList(0, marketReferenceTopN));

            int maxUniverse = config.getInt("scan.max_universe_size", 0);
            List<UniverseRecord> universe = universeDao.listActive(maxUniverse);
            if (universe.isEmpty()) {
                JpxUniverseUpdater universeUpdater = new JpxUniverseUpdater(config, metadataDao, universeDao);
                universeUpdater.updateIfNeeded(true);
                universe = universeDao.listActive(maxUniverse);
            }
            if (universe.isEmpty()) {
                throw new IllegalStateException("Universe is empty. JPX update may have failed.");
            }

            List<WatchlistAnalysis> watchlistCandidates = analyzeWatchlist(watchlist, universe);
            ZoneId zoneId = ZoneId.of(config.getString("app.zone", "Asia/Tokyo"));
            Path reportDir = config.getPath("report.dir");
            ReportBuilder.RunType runType = ReportBuilder.detectRunType(startedAt, zoneId);
            Map<String, Double> previousScores = loadPreviousCandidateScoreMap(
                    marketRun.id,
                    Math.max(topN, config.getInt("scan.top_n", 15))
            );
            ScanSummaryEnvelope scanSummaryEnvelope = loadScanSummary(
                    marketRun.id,
                    null,
                    marketRun,
                    Math.max(0, marketRun.scannedSize)
            );
            ScanResultSummary scanSummary = scanSummaryEnvelope.summary;
            addMarketDataSourceStats(diagnostics, marketRun.id, null);
            addWatchlistCoverageDiagnostics(diagnostics, watchlistCandidates);
            addMarketCoverageDiagnostics(
                    diagnostics,
                    scanSummary,
                    Math.max(0, marketRun.scannedSize),
                    "MARKET",
                    scanSummaryEnvelope.source,
                    scanSummaryEnvelope.owner
            );
            diagnostics.addFeatureStatus(
                    "report.metrics.top5_perf",
                    FeatureStatusResolver.resolveFeatureStatus(
                            "report.metrics.top5_perf.enabled",
                            config.getBoolean("report.metrics.top5_perf.enabled", false),
                            false,
                            null,
                            OWNER_FEATURE_RESOLVE
                    )
            );
            PolymarketResult polymarket = collectPolymarketSignals(watchlistCandidates, diagnostics);
            boolean marketPartial = "PARTIAL".equalsIgnoreCase(safeText(marketRun.status));
            Path reportPath = reportBuilder.writeDailyReport(
                    reportDir,
                    startedAt,
                    zoneId,
                    marketRun.universeSize > 0 ? marketRun.universeSize : universe.size(),
                    marketRun.scannedSize,
                    marketRun.candidateSize > 0 ? marketRun.candidateSize : topCandidates.size(),
                    topN,
                    watchlist,
                    watchlistCandidates,
                    topCandidates,
                    config.getDouble("scan.min_score", 55.0),
                    runType,
                    previousScores,
                    scanSummary,
                    marketPartial,
                    safeText(marketRun.status),
                    polymarket.report,
                    diagnostics
            );

            runDao.insertCandidates(runId, topCandidates);
            String notes = String.format(
                    Locale.US,
                    "merged_from_run=%d(%s); watchlist=%d; market_ref_top=%d",
                    marketRun.id,
                    safeText(marketRun.mode),
                    watchlistCandidates.size(),
                    marketReferenceCandidates.size()
            );
            runDao.finishRun(
                    runId,
                    "SUCCESS",
                    marketRun.universeSize > 0 ? marketRun.universeSize : universe.size(),
                    marketRun.scannedSize,
                    marketRun.candidateSize > 0 ? marketRun.candidateSize : topCandidates.size(),
                    topN,
                    reportPath.toString(),
                    notes
            );
            UniverseUpdateResult updateResult = new UniverseUpdateResult(
                    false,
                    marketRun.universeSize > 0 ? marketRun.universeSize : universe.size(),
                    "merged latest market scan run #" + marketRun.id
            );
            return new DailyRunOutcome(
                    runId,
                    startedAt,
                    updateResult,
                    marketRun.universeSize > 0 ? marketRun.universeSize : universe.size(),
                    marketRun.scannedSize,
                    0,
                    marketRun.candidateSize > 0 ? marketRun.candidateSize : topCandidates.size(),
                    topN,
                    reportPath,
                    topCandidates,
                    watchlistCandidates,
                    marketReferenceCandidates,
                    1,
                    1,
                    1,
                    false
            );
        } catch (Exception e) {
            safeFinishFailed(runId, e);
            throw e;
        }
    }

/**
 * 方法说明：executeMarketScan，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private MarketScanSnapshot executeMarketScan(
            long runId,
            boolean forceUniverseUpdate,
            Integer topNOverride,
            boolean resetBatchCheckpoint
    ) throws Exception {
        JpxUniverseUpdater universeUpdater = new JpxUniverseUpdater(config, metadataDao, universeDao);
        UniverseUpdateResult updateResult = universeUpdater.updateIfNeeded(forceUniverseUpdate);

        int maxUniverse = config.getInt("scan.max_universe_size", 0);
        List<UniverseRecord> universe = universeDao.listActive(maxUniverse);
        int universeSize = universe.size();
        if (universeSize == 0) {
            throw new IllegalStateException("Universe is empty. JPX update may have failed.");
        }

        int topN = topNOverride != null ? topNOverride : config.getInt("scan.top_n", 15);
        topN = Math.max(1, topN);

        BatchPlan plan = prepareBatchPlan(universe, topN, resetBatchCheckpoint);
        BatchState state = loadBatchState(plan, topN);

        int maxSegmentsPerRun = Math.max(0, config.getInt("scan.batch.max_segments_per_run", 0));
        int remaining = Math.max(0, plan.segments.size() - state.nextSegmentIndex);
        int allowedThisRun = maxSegmentsPerRun <= 0 ? remaining : Math.min(maxSegmentsPerRun, remaining);
        boolean retryWhenCacheExists = config.getBoolean("scan.network.retry_when_cache_exists", false);
        if (retryWhenCacheExists) {
            System.out.println("Data source priority: cache(fresh) -> yahoo -> cache(retry_enabled)");
        } else {
            System.out.println("Data source priority: cache(fresh) -> yahoo -> cache");
        }

        for (int offset = 0; offset < allowedThisRun; offset++) {
            int segmentIndex = state.nextSegmentIndex;
            MarketSegment segment = plan.segments.get(segmentIndex);
            System.out.println(String.format(
                    Locale.US,
                    "Batch segment %d/%d market=%s size=%d",
                    segmentIndex + 1,
                    plan.segments.size(),
                    segment.segmentKey,
                    segment.records.size()
            ));
            ScanStats segmentStats = scanUniverse(
                    runId,
                    segment.records,
                    topN,
                    segmentIndex + 1,
                    plan.segments.size(),
                    segment.segmentKey
            );
            state.stats.merge(segmentStats);
            state.nextSegmentIndex = segmentIndex + 1;
            saveCheckpoint(plan, state, topN);
        }

        boolean completedAllSegments = state.nextSegmentIndex >= plan.segments.size();
        if (completedAllSegments) {
            clearCheckpoint(plan);
        }

        List<ScoredCandidate> rankedTop = new ArrayList<>(state.stats.topCandidates());
        rankedTop.sort(Comparator.comparingDouble((ScoredCandidate c) -> c.score).reversed());
        List<ScoredCandidate> top = rankedTop.size() <= topN
                ? rankedTop
                : new ArrayList<>(rankedTop.subList(0, topN));
        int marketReferenceTopN = Math.max(1, config.getInt("scan.market_reference_top_n", 5));
        List<ScoredCandidate> marketReferenceCandidates = top.size() <= marketReferenceTopN
                ? new ArrayList<>(top)
                : new ArrayList<>(top.subList(0, marketReferenceTopN));
        return new MarketScanSnapshot(
                updateResult,
                universe,
                universeSize,
                topN,
                top,
                marketReferenceCandidates,
                state.stats,
                plan.segments.size(),
                state.nextSegmentIndex,
                !completedAllSegments
        );
    }

/**
 * 方法说明：analyzeWatchlist，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private List<WatchlistAnalysis> analyzeWatchlist(List<String> watchlist, List<UniverseRecord> universe) {
        List<String> watchItems = sanitizeWatchlist(watchlist);
        if (watchItems.isEmpty()) {
            return List.of();
        }

        double minScore = config.getDouble("scan.min_score", 55.0);
        Map<String, UniverseRecord> byCode = new HashMap<>();
        Map<String, UniverseRecord> byTicker = new HashMap<>();
        for (UniverseRecord record : universe) {
            if (record.code != null) {
                String codeKey = record.code.trim().toUpperCase(Locale.ROOT);
                if (!codeKey.isEmpty()) {
                    byCode.putIfAbsent(codeKey, record);
                }
            }
            if (record.ticker != null) {
                String tickerKey = record.ticker.trim().toLowerCase(Locale.ROOT);
                if (!tickerKey.isEmpty()) {
                    byTicker.putIfAbsent(tickerKey, record);
                }
            }
        }

        System.out.println(String.format(Locale.US, "Watchlist analysis start. size=%d", watchItems.size()));
        List<WatchlistAnalysis> out = new ArrayList<>();
        for (int i = 0; i < watchItems.size(); i++) {
            String watchItem = watchItems.get(i);
            TickerSpec tickerSpec = tickerResolver.resolve(watchItem);
            WatchlistAnalysis row;
            if (!tickerSpec.isOk()) {
                String userMessage = tickerSpec.resolveStatus == TickerSpec.ResolveStatus.NEED_MARKET_HINT
                        ? "Market suffix required: use ####.T (JP) or NVDA.US (US)."
                        : "Unrecognized ticker: use ####.T (JP) or NVDA.US (US).";
                row = buildSkippedWatchRow(
                        watchItem,
                        tickerSpec,
                        "SYMBOL_ERROR",
                        userMessage,
                        CauseCode.TICKER_RESOLVE_FAILED,
                        OWNER_WATCH_RESOLVE,
                        Map.of("reason", "ticker_resolve_failed")
                );
            } else if (tickerSpec.market != TickerSpec.Market.JP && nonJpHandling == NonJpHandling.SKIP_WITH_REASON) {
                row = buildSkippedWatchRow(
                        watchItem,
                        tickerSpec,
                        "SYMBOL_ERROR",
                        "Non-JP ticker is skipped by configuration: set watchlist.non_jp_handling=PROCESS_SEPARATELY to process it.",
                        CauseCode.TICKER_RESOLVE_FAILED,
                        OWNER_RUNNER + "#analyzeWatchlist(...)",
                        Map.of("reason", "non_jp_skipped")
                );
            } else {
                UniverseRecord record = resolveJpWatchRecord(tickerSpec, byCode, byTicker);
                String yahooTicker = toYahooTicker(tickerSpec, record);
                PriceFetchTrace priceTrace = fetchWatchPriceTrace(record.ticker, yahooTicker);
                logPriceTrace(record.ticker, priceTrace);

                LegacyWatchResult legacy = buildLegacyWatchResult(record, watchItem, yahooTicker, priceTrace);
                WatchlistScanResult technical = scanWatchRecord(record, watchItem, priceTrace);
                String industryEn = blankTo(industryService.industryOf(yahooTicker), "Unknown");
                String industryZh = blankTo(industryService.industryZhOf(yahooTicker), "Unknown");
                String companyLocal = resolveCompanyLocalName(record, yahooTicker);
                String displayName = buildDisplayName(companyLocal, record.code, industryZh, industryEn);
                String technicalStatus = toWatchStatus(technical, minScore);
                String technicalReasonsJson = enrichWatchReasonJson(
                        technical.candidate.reasonsJson,
                        technical,
                        technicalStatus,
                        minScore,
                        watchItem,
                        tickerSpec,
                        record,
                        yahooTicker,
                        priceTrace
                );
                String watchDiagnosticsJson = buildWatchDiagnosticsJson(
                        watchItem,
                        tickerSpec,
                        record,
                        yahooTicker,
                        technical,
                        technicalStatus,
                        priceTrace
                );

                row = new WatchlistAnalysis(
                        watchItem,
                        safeText(record.code),
                        safeText(record.ticker),
                        displayName,
                        companyLocal,
                        industryZh,
                        industryEn,
                        tickerSpec.market.name(),
                        tickerSpec.resolveStatus.name(),
                        tickerSpec.normalized,
                        safeDouble(legacy.context.lastClose),
                        safeDouble(legacy.context.prevClose),
                        safeDouble(legacy.context.pctChange),
                        safeText(priceTrace.dataSource),
                        safeText(priceTrace.priceTimestamp),
                        priceTrace.barsCount,
                        priceTrace.cacheHit,
                        priceTrace.fetchLatencyMs,
                        technical.fetchSuccess,
                        technical.indicatorReady,
                        false,
                        safeDouble(legacy.context.totalScore),
                        safeText(legacy.context.rating),
                        safeText(legacy.context.risk),
                        legacy.aiTriggered,
                        safeText(legacy.context.gateReason),
                        legacy.context.news.size(),
                        newsService.sourceLabel(),
                        trimChars(safeText(legacy.context.aiSummary), watchlistMaxAiChars),
                        buildNewsDigestLines(legacy.context.news),
                        technical.candidate.score,
                        technicalStatus,
                        technicalReasonsJson,
                        technical.candidate.indicatorsJson,
                        watchDiagnosticsJson,
                        joinErrors(legacy.error, technical.error),
                        legacy.context.priceHistory
                );
            }
            out.add(row);
            System.out.println(String.format(
                    Locale.US,
                    "Watchlist %d/%d item=%s ticker=%s score=%.2f rating=%s risk=%s pct=%.2f%% ai=%s gate=%s ai_text=%s news=%d tech=%.2f tech_status=%s source=%s date=%s bars=%d latency=%dms",
                    i + 1,
                    watchItems.size(),
                    watchItem,
                    row.ticker,
                    row.totalScore,
                    row.rating,
                    row.risk,
                    row.pctChange,
                    row.aiTriggered ? "triggered" : "not_triggered",
                    trimChars(safeText(row.gateReason), 80),
                    trimChars(safeText(row.aiSummary).replace("\r", " ").replace("\n", " "), 120),
                    row.newsCount,
                    row.technicalScore,
                    row.technicalStatus,
                    row.dataSource,
                    row.priceTimestamp,
                    row.barsCount,
                    row.fetchLatencyMs
            ));
        }

        Set<String> suspectTickers = detectPriceSuspects(out);
        if (!suspectTickers.isEmpty()) {
            List<WatchlistAnalysis> flagged = new ArrayList<>(out.size());
            for (WatchlistAnalysis row : out) {
                boolean suspect = suspectTickers.contains(safeText(row.ticker).toLowerCase(Locale.ROOT));
                if (suspect) {
                    flagged.add(copyWatchRowWithSuspect(row, true));
                } else {
                    flagged.add(row);
                }
            }
            out = flagged;
        }

        out.sort(Comparator.comparingDouble((WatchlistAnalysis c) -> c.totalScore).reversed());
        return out;
    }

/**
 * 方法说明：toWatchStatus，负责转换数据结构用于后续处理。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String toWatchStatus(WatchlistScanResult result, double minScore) {
        if (!result.error.isEmpty()) {
            return "ERROR";
        }
        if (!result.filterPassed) {
            return "OBSERVE";
        }
        if (!result.riskPassed) {
            return "RISK";
        }
        if (result.candidate.score < minScore) {
            return "OBSERVE";
        }
        return "CANDIDATE";
    }

/**
 * 方法说明：buildSkippedWatchRow，负责构建目标对象或输出内容。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private WatchlistAnalysis buildSkippedWatchRow(
            String watchItem,
            TickerSpec tickerSpec,
            String category,
            String userMessage,
            CauseCode causeCode,
            String owner,
            Map<String, Object> extraDetails
    ) {
        Map<String, Object> details = new LinkedHashMap<>();
        details.put("user_message", safeText(userMessage));
        details.put("category", safeText(category));
        details.put("raw", safeText(tickerSpec == null ? "" : tickerSpec.raw));
        details.put("normalized", safeText(tickerSpec == null ? "" : tickerSpec.normalized));
        details.put("market", tickerSpec == null || tickerSpec.market == null ? "UNKNOWN" : tickerSpec.market.name());
        details.put("resolve_status", tickerSpec == null || tickerSpec.resolveStatus == null
                ? TickerSpec.ResolveStatus.INVALID.name()
                : tickerSpec.resolveStatus.name());
        if (extraDetails != null) {
            details.putAll(extraDetails);
        }

        JSONObject reasonRoot = new JSONObject();
        reasonRoot.put("filter_passed", false);
        reasonRoot.put("risk_passed", false);
        reasonRoot.put("filter_reasons", new JSONArray());
        reasonRoot.put("risk_flags", new JSONArray());
        reasonRoot.put("filter_metrics", new JSONObject());
        reasonRoot.put("score_breakdown", new JSONObject());
        reasonRoot.put("watch_item", safeText(watchItem));
        reasonRoot.put("technical_status", "SKIPPED");
        reasonRoot.put("score_owner", OWNER_SCORE);
        reasonRoot.put("filter_owner", OWNER_FILTER);
        reasonRoot.put("risk_owner", OWNER_RISK);
        reasonRoot.put("plan_owner", "com.stockbot.jp.plan.TradePlanBuilder#build(...)");
        reasonRoot.put("cause_code", causeCode == null ? CauseCode.NONE.name() : causeCode.name());
        reasonRoot.put("owner", safeText(owner));
        reasonRoot.put("details", new JSONObject(details));

        JSONObject fetchTrace = new JSONObject();
        fetchTrace.put("ticker_normalized", safeText(tickerSpec == null ? "" : tickerSpec.normalized));
        fetchTrace.put("resolved_exchange", tickerSpec == null || tickerSpec.market == null ? "UNKNOWN" : tickerSpec.market.name());
        fetchTrace.put("fetcher_class", OWNER_WATCH_RESOLVE);
        fetchTrace.put("fallback_path", "watchlist_skip");
        fetchTrace.put("request_failure_category", "skipped");
        fetchTrace.put("request_error", safeText(userMessage));
        fetchTrace.put("data_source", "skipped_non_jp");
        reasonRoot.put("fetch_trace", fetchTrace);

        JSONObject diag = new JSONObject();
        diag.put("watch_item", safeText(watchItem));
        diag.put("ticker", safeText(tickerSpec == null ? "" : tickerSpec.normalized));
        diag.put("code", extractCode(watchItem));
        diag.put("technical_status", "SKIPPED");
        diag.put("fetch_success", false);
        diag.put("indicator_ready", false);
        diag.put("cause_code", causeCode == null ? CauseCode.NONE.name() : causeCode.name());
        diag.put("owner", safeText(owner));
        diag.put("details", new JSONObject(details));
        diag.put("fetch_trace", fetchTrace);

        String code = extractCode(watchItem);
        String display = buildDisplayName(
                safeText(watchItem).toUpperCase(Locale.ROOT),
                code,
                "Unknown",
                "Unknown"
        );

        return new WatchlistAnalysis(
                watchItem,
                code,
                safeText(tickerSpec == null ? "" : tickerSpec.normalized),
                display,
                safeText(watchItem).toUpperCase(Locale.ROOT),
                "Unknown",
                "Unknown",
                tickerSpec == null || tickerSpec.market == null ? "UNKNOWN" : tickerSpec.market.name(),
                tickerSpec == null || tickerSpec.resolveStatus == null
                        ? TickerSpec.ResolveStatus.INVALID.name()
                        : tickerSpec.resolveStatus.name(),
                safeText(tickerSpec == null ? "" : tickerSpec.normalized),
                0.0,
                0.0,
                0.0,
                "skipped_non_jp",
                "",
                0,
                false,
                0L,
                false,
                false,
                false,
                0.0,
                "N/A",
                "N/A",
                false,
                safeText(category),
                0,
                "",
                "",
                List.of(),
                0.0,
                "SKIPPED",
                reasonRoot.toString(),
                "{}",
                diag.toString(),
                safeText(userMessage),
                List.of()
        );
    }

/**
 * 方法说明：resolveJpWatchRecord，负责解析规则并确定最终结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private UniverseRecord resolveJpWatchRecord(
            TickerSpec tickerSpec,
            Map<String, UniverseRecord> byCode,
            Map<String, UniverseRecord> byTicker
    ) {
        String normalized = tickerSpec == null ? "" : tickerSpec.normalized;
        String jpTicker = toJpTicker(normalized);
        if (!jpTicker.isEmpty()) {
            UniverseRecord exactTicker = byTicker.get(jpTicker.toLowerCase(Locale.ROOT));
            if (exactTicker != null) {
                return exactTicker;
            }
        }

        String code = extractCode(normalized);
        if (!code.isEmpty()) {
            UniverseRecord byCodeRecord = byCode.get(code);
            if (byCodeRecord != null) {
                return byCodeRecord;
            }
        }

        String fallbackCode = code.isEmpty() ? extractCode(tickerSpec == null ? "" : tickerSpec.raw) : code;
        String fallbackName = tickerSpec == null || tickerSpec.raw.isEmpty() ? normalized : tickerSpec.raw;
        return new UniverseRecord(
                jpTicker.isEmpty() ? normalized.toLowerCase(Locale.ROOT) : jpTicker.toLowerCase(Locale.ROOT),
                fallbackCode,
                fallbackName,
                "WATCHLIST_JP"
        );
    }

/**
 * 方法说明：extractCode，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String extractCode(String watchItem) {
        if (watchItem == null) {
            return "";
        }
        String token = watchItem.trim();
        if (token.isEmpty()) {
            return "";
        }
        int dot = token.indexOf('.');
        if (dot > 0) {
            token = token.substring(0, dot);
        }
        String upper = token.trim().toUpperCase(Locale.ROOT);
        if (upper.matches("[0-9A-Z]{4,6}")) {
            return upper;
        }
        return "";
    }

/**
 * 方法说明：toJpTicker，负责转换数据结构用于后续处理。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String toJpTicker(String normalizedTicker) {
        String token = normalizedTicker == null ? "" : normalizedTicker.trim();
        if (token.isEmpty()) {
            return "";
        }
        String lower = token.toLowerCase(Locale.ROOT);
        if (lower.endsWith(".t")) {
            return lower.substring(0, lower.length() - 2) + ".jp";
        }
        if (lower.matches("^\\d{4}$")) {
            return lower + ".jp";
        }
        return lower;
    }

    private WatchlistScanResult scanWatchRecord(UniverseRecord record, String watchItem, PriceFetchTrace priceTrace) {
        try {
            List<BarDaily> bars = priceTrace == null ? List.of() : priceTrace.bars;
            if (bars.isEmpty()) {
                return failedWatchRecord(
                        record,
                        watchItem,
                        "fetch_failed:no_data",
                        CauseCode.FETCH_FAILED,
                        Map.of("bars_count", 0),
                        false,
                        false
                );
            }

            int freshDays = Math.max(0, config.getInt("scan.fresh_days", config.getInt("scan.cache.fresh_days", 2)));
            if (!isBarsFreshEnough(bars, freshDays)) {
                return failedWatchRecord(
                        record,
                        watchItem,
                        "stale_data",
                        CauseCode.STALE,
                        Map.of("fresh_days", freshDays),
                        true,
                        false
                );
            }

            int minHistory = Math.max(120, config.getInt("scan.min_history_bars", 180));
            if (bars.size() < minHistory) {
                return failedWatchRecord(
                        record,
                        watchItem,
                        "history_short",
                        CauseCode.HISTORY_SHORT,
                        Map.of("bars_count", bars.size(), "min_history", minHistory),
                        true,
                        false
                );
            }

            IndicatorSnapshot ind = indicatorEngine.compute(bars);
            if (ind == null) {
                return failedWatchRecord(
                        record,
                        watchItem,
                        "indicator_failed",
                        CauseCode.INDICATOR_ERROR,
                        Map.of("bars_count", bars.size()),
                        true,
                        false
                );
            }

            FilterDecision filter = candidateFilter.evaluate(bars, ind);
            RiskDecision risk = riskFilter.evaluate(ind);
            ScoreResult score = scoringEngine.score(ind, risk);
            String reasonsJson = reasonJsonBuilder.buildReasonsJson(filter, risk, score);
            String indicatorsJson = reasonJsonBuilder.buildIndicatorsJson(ind);
            ScoredCandidate candidate = new ScoredCandidate(
                    record.ticker,
                    record.code,
                    record.name,
                    record.market,
                    score.score,
                    ind.lastClose,
                    appendWatchMeta(reasonsJson, watchItem),
                    indicatorsJson
            );
            Outcome<Void> outcome;
            if (!filter.passed) {
                outcome = Outcome.failure(
                        CauseCode.FILTER_REJECTED,
                        OWNER_FILTER,
                        Map.of("filter_reasons", filter.reasons, "filter_metrics", filter.metrics)
                );
            } else if (!risk.passed) {
                outcome = Outcome.failure(
                        CauseCode.RISK_REJECTED,
                        OWNER_RISK,
                        Map.of("risk_flags", risk.flags, "risk_penalty", risk.penalty)
                );
            } else {
                outcome = Outcome.success(null, OWNER_WATCH_SCAN);
            }
            return new WatchlistScanResult(candidate, filter.passed, risk.passed, true, true, outcome, null);
        } catch (Exception e) {
            return failedWatchRecord(
                    record,
                    watchItem,
                    "unexpected:" + e.getMessage(),
                    CauseCode.RUNTIME_ERROR,
                    Map.of("error_class", e.getClass().getSimpleName()),
                    priceTrace != null && priceTrace.barsCount > 0,
                    false
            );
        }
    }

/**
 * 方法说明：fetchWatchPriceTrace，负责拉取外部数据并做基础处理。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private PriceFetchTrace fetchWatchPriceTrace(String jpTicker, String yahooTicker) {
        long started = System.nanoTime();

        List<BarDaily> fromYahoo = fetchBarsFromYahoo(jpTicker, yahooTicker);
        if (!fromYahoo.isEmpty()) {
            return PriceFetchTrace.fromBars(
                    fromYahoo,
                    "yahoo",
                    false,
                    nanosToMillis(System.nanoTime() - started),
                    false,
                    "",
                    "",
                    "yahoo"
            );
        }

        List<BarDaily> cached = loadCachedBars(jpTicker);
        if (!cached.isEmpty()) {
            return PriceFetchTrace.fromBars(
                    cached,
                    "cache",
                    true,
                    nanosToMillis(System.nanoTime() - started),
                    true,
                    "no_data",
                    "no_data",
                    "yahoo->cache"
            );
        }

        return PriceFetchTrace.failed(
                "fetch_failed",
                nanosToMillis(System.nanoTime() - started),
                true,
                "no_data",
                "no_data",
                "yahoo->cache->failed"
        );
    }

/**
 * 方法说明：logPriceTrace，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void logPriceTrace(String ticker, PriceFetchTrace trace) {
        if (trace == null) {
            System.out.println(String.format(Locale.US,
                    "[PRICE] ticker=%s source=fetch_failed tradeDate= lastClose=NaN bars=0 latency=0ms cache_hit=false",
                    safeText(ticker)));
            return;
        }
        System.out.println(String.format(
                Locale.US,
                "[PRICE] ticker=%s source=%s tradeDate=%s lastClose=%.4f bars=%d latency=%dms cache_hit=%s fallback=%s",
                safeText(ticker),
                safeText(trace.dataSource),
                safeText(trace.priceTimestamp),
                trace.lastClose,
                trace.barsCount,
                trace.fetchLatencyMs,
                trace.cacheHit,
                safeText(trace.fallbackPath)
        ));
    }

/**
 * 方法说明：fetchBarsFromYahoo，负责拉取外部数据并做基础处理。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private List<BarDaily> fetchBarsFromYahoo(String jpTicker, String yahooTicker) {
        if (yahooTicker == null || yahooTicker.trim().isEmpty()) {
            return List.of();
        }
        try {
            List<MarketDataService.DailyBar> history = marketDataService.fetchDailyHistoryBars(yahooTicker, "2y", "1d");
            return toBarsFromYahoo(jpTicker, history);
        } catch (Exception ignored) {
            return List.of();
        }
    }

/**
 * 方法说明：failedWatchRecord，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private WatchlistScanResult failedWatchRecord(
            UniverseRecord record,
            String watchItem,
            String error,
            CauseCode causeCode,
            Map<String, Object> details,
            boolean fetchSuccess,
            boolean indicatorReady
    ) {
        JSONObject reasonRoot = new JSONObject();
        reasonRoot.put("filter_passed", false);
        reasonRoot.put("risk_passed", false);
        reasonRoot.put("filter_reasons", new JSONArray());
        reasonRoot.put("risk_flags", new JSONArray());
        reasonRoot.put("filter_metrics", new JSONObject());
        reasonRoot.put("score_breakdown", new JSONObject());
        reasonRoot.put("watch_item", watchItem == null ? "" : watchItem);
        reasonRoot.put("error", error == null ? "unknown" : error);
        reasonRoot.put("cause_code", causeCode == null ? CauseCode.NONE.name() : causeCode.name());
        reasonRoot.put("owner", OWNER_WATCH_SCAN);
        reasonRoot.put("details", new JSONObject(details == null ? Map.of() : details));
        ScoredCandidate candidate = new ScoredCandidate(
                record.ticker,
                record.code,
                record.name,
                record.market,
                0.0,
                0.0,
                reasonRoot.toString(),
                "{}"
        );
        Outcome<Void> outcome = Outcome.failure(
                causeCode == null ? CauseCode.NONE : causeCode,
                OWNER_WATCH_SCAN,
                details == null ? Map.of() : details
        );
        return new WatchlistScanResult(candidate, false, false, fetchSuccess, indicatorReady, outcome, error);
    }

/**
 * 方法说明：appendWatchMeta，负责追加组装输出片段。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String appendWatchMeta(String reasonsJson, String watchItem) {
        JSONObject root = new JSONObject(reasonsJson == null ? "{}" : reasonsJson);
        root.put("watch_item", watchItem == null ? "" : watchItem);
        return root.toString();
    }

/**
 * 方法说明：enrichWatchReasonJson，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String enrichWatchReasonJson(
            String rawReasonsJson,
            WatchlistScanResult technical,
            String technicalStatus,
            double minScore,
            String watchItem,
            TickerSpec tickerSpec,
            UniverseRecord record,
            String yahooTicker,
            PriceFetchTrace priceTrace
    ) {
        JSONObject root = safeJsonObject(rawReasonsJson);
        root.put("watch_item", watchItem == null ? "" : watchItem);
        root.put("technical_status", safeText(technicalStatus));
        root.put("market", tickerSpec == null || tickerSpec.market == null ? "UNKNOWN" : tickerSpec.market.name());
        root.put("resolve_status", tickerSpec == null || tickerSpec.resolveStatus == null
                ? TickerSpec.ResolveStatus.INVALID.name()
                : tickerSpec.resolveStatus.name());
        root.put("normalized_ticker", safeText(tickerSpec == null ? "" : tickerSpec.normalized));
        root.put("score_owner", OWNER_SCORE);
        root.put("filter_owner", OWNER_FILTER);
        root.put("risk_owner", OWNER_RISK);
        root.put("plan_owner", "com.stockbot.jp.plan.TradePlanBuilder#build(...)");

        Outcome<Void> outcome = technical == null ? null : technical.outcome;
        CauseCode cause = determineWatchCause(technical, technicalStatus, minScore);
        String owner = OWNER_WATCH_SCAN;
        JSONObject detailObj = new JSONObject();
        if (outcome != null && !outcome.success) {
            if (outcome.causeCode != null && outcome.causeCode != CauseCode.NONE) {
                cause = outcome.causeCode;
            }
            if (outcome.owner != null && !outcome.owner.trim().isEmpty()) {
                owner = outcome.owner;
            }
            detailObj = new JSONObject(outcome.details);
        }
        if (cause == CauseCode.SCORE_BELOW_THRESHOLD) {
            detailObj.put("score", technical == null || technical.candidate == null ? 0.0 : technical.candidate.score);
            detailObj.put("min_score", minScore);
        }

        JSONArray missingInputs = new JSONArray();
        if (cause == CauseCode.NO_BARS || cause == CauseCode.FETCH_FAILED) {
            missingInputs.put("bars");
            missingInputs.put("last_close");
        } else if (cause == CauseCode.TICKER_RESOLVE_FAILED) {
            missingInputs.put("ticker_mapping");
            missingInputs.put("bars");
        } else if (cause == CauseCode.HISTORY_SHORT) {
            missingInputs.put("min_history_bars");
        } else if (cause == CauseCode.INDICATOR_ERROR) {
            missingInputs.put("indicator_snapshot");
        }

        root.put("cause_code", cause.name());
        root.put("owner", owner);
        root.put("details", detailObj);
        root.put("missing_inputs", missingInputs);
        root.put("fetch_trace", buildFetchTraceObject(tickerSpec, record, yahooTicker, priceTrace));
        return root.toString();
    }

/**
 * 方法说明：buildWatchDiagnosticsJson，负责构建目标对象或输出内容。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String buildWatchDiagnosticsJson(
            String watchItem,
            TickerSpec tickerSpec,
            UniverseRecord record,
            String yahooTicker,
            WatchlistScanResult technical,
            String technicalStatus,
            PriceFetchTrace priceTrace
    ) {
        JSONObject root = new JSONObject();
        root.put("watch_item", safeText(watchItem));
        root.put("ticker", record == null ? "" : safeText(record.ticker));
        root.put("code", record == null ? "" : safeText(record.code));
        root.put("market", tickerSpec == null || tickerSpec.market == null ? "UNKNOWN" : tickerSpec.market.name());
        root.put("resolve_status", tickerSpec == null || tickerSpec.resolveStatus == null
                ? TickerSpec.ResolveStatus.INVALID.name()
                : tickerSpec.resolveStatus.name());
        root.put("normalized_ticker", safeText(tickerSpec == null ? "" : tickerSpec.normalized));
        root.put("technical_status", safeText(technicalStatus));
        root.put("fetch_success", technical != null && technical.fetchSuccess);
        root.put("indicator_ready", technical != null && technical.indicatorReady);
        root.put("fetch_trace", buildFetchTraceObject(tickerSpec, record, yahooTicker, priceTrace));
        if (technical != null && technical.outcome != null) {
            root.put("cause_code", technical.outcome.causeCode == null ? CauseCode.NONE.name() : technical.outcome.causeCode.name());
            root.put("owner", safeText(technical.outcome.owner));
            root.put("details", new JSONObject(technical.outcome.details));
        }
        return root.toString();
    }

    private JSONObject buildFetchTraceObject(TickerSpec tickerSpec, UniverseRecord record, String yahooTicker, PriceFetchTrace priceTrace) {
        JSONObject fetch = new JSONObject();
        fetch.put("ticker_normalized", safeText(tickerSpec == null ? "" : tickerSpec.normalized));
        fetch.put("resolved_exchange", resolvedExchange(tickerSpec, record, yahooTicker));
        fetch.put("fetcher_class", resolveFetcherClass(priceTrace == null ? "" : priceTrace.dataSource));
        fetch.put("fallback_path", priceTrace == null ? "" : safeText(priceTrace.fallbackPath));
        fetch.put("request_failure_category", priceTrace == null ? "" : safeText(priceTrace.requestFailureCategory));
        fetch.put("request_error", priceTrace == null ? "" : safeText(priceTrace.error));
        fetch.put("data_source", priceTrace == null ? "" : safeText(priceTrace.dataSource));
        return fetch;
    }

/**
 * 方法说明：determineWatchCause，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private CauseCode determineWatchCause(WatchlistScanResult technical, String technicalStatus, double minScore) {
        if (technical == null) {
            return CauseCode.RUNTIME_ERROR;
        }
        if (technical.outcome != null && !technical.outcome.success && technical.outcome.causeCode != CauseCode.NONE) {
            return technical.outcome.causeCode;
        }
        if ("ERROR".equalsIgnoreCase(safeText(technicalStatus))) {
            return CauseCode.RUNTIME_ERROR;
        }
        if (!technical.filterPassed) {
            return CauseCode.FILTER_REJECTED;
        }
        if (!technical.riskPassed) {
            return CauseCode.RISK_REJECTED;
        }
        if (technical.candidate != null && technical.candidate.score < minScore) {
            return CauseCode.SCORE_BELOW_THRESHOLD;
        }
        return CauseCode.NONE;
    }

/**
 * 方法说明：resolveFetcherClass，负责解析规则并确定最终结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String resolveFetcherClass(String dataSource) {
        String src = safeText(dataSource).toLowerCase(Locale.ROOT);
        if ("yahoo".equals(src)) {
            return "com.stockbot.data.MarketDataService#fetchDailyHistoryBars(...)";
        }
        if ("cache".equals(src)) {
            return "com.stockbot.jp.db.BarDailyDao#loadRecentBars(...)";
        }
        return OWNER_WATCH_FETCH;
    }

    private String resolvedExchange(TickerSpec tickerSpec, UniverseRecord record, String yahooTicker) {
        if (tickerSpec != null && tickerSpec.market != null && tickerSpec.market != TickerSpec.Market.UNKNOWN) {
            return tickerSpec.market.name();
        }
        String market = record == null ? "" : safeText(record.market).toUpperCase(Locale.ROOT);
        if (!market.isEmpty()) {
            return market;
        }
        String ticker = safeText(yahooTicker).toUpperCase(Locale.ROOT);
        if (ticker.endsWith(".T")) {
            return "TSE";
        }
        return "US_OR_OTHER";
    }

/**
 * 方法说明：safeJsonObject，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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

/**
 * 方法说明：buildLegacyWatchResult，负责构建目标对象或输出内容。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private LegacyWatchResult buildLegacyWatchResult(
            UniverseRecord record,
            String watchItem,
            String yahooTicker,
            PriceFetchTrace priceTrace
    ) {
        StockContext sc = new StockContext(yahooTicker);
        String error = "";
        boolean aiTriggered = false;
        try {
            List<DailyPrice> history = toDailyPrices(priceTrace == null ? List.of() : priceTrace.bars);
            if (history.isEmpty()) {
                history = marketDataService.fetchDailyHistory(yahooTicker, "1y", "1d");
            }
            sc.priceHistory.addAll(history);

            MarketDataService.PricePair pair = MarketDataService.lastTwoFromHistory(sc.priceHistory);
            Double tracedLast = (priceTrace != null && Double.isFinite(priceTrace.lastClose) && priceTrace.lastClose > 0.0)
                    ? priceTrace.lastClose
                    : null;
            sc.lastClose = tracedLast != null ? tracedLast : pair.last;
            sc.prevClose = pair.prev;
            sc.pctChange = computePctChange(sc.lastClose, sc.prevClose);

            List<String> queries = buildNewsQueries(record, watchItem, yahooTicker);
            List<NewsItem> news = newsService.fetchNews(yahooTicker, queries);
            sc.news.addAll(news);

            factorEngine.computeFactors(sc);
            legacyScoringEngine.score(sc);
            aiTriggered = gatePolicy.shouldRunAi(sc);
            if (aiTriggered) {
                sc.aiRan = true;
                sc.aiSummary = ollamaClient.summarize(Prompts.buildPrompt(sc));
            } else {
                sc.aiRan = false;
                sc.aiSummary = "";
            }
        } catch (Exception e) {
            error = e.getMessage() == null ? "legacy_failed" : e.getMessage();
            sc.aiSummary = "";
        }
        return new LegacyWatchResult(sc, aiTriggered, error);
    }

/**
 * 方法说明：toBarsFromYahoo，负责转换数据结构用于后续处理。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private List<BarDaily> toBarsFromYahoo(String jpTicker, List<MarketDataService.DailyBar> history) {
        if (history == null || history.isEmpty()) {
            return List.of();
        }
        List<BarDaily> out = new ArrayList<>(history.size());
        for (MarketDataService.DailyBar p : history) {
            if (p == null || p.date == null || !Double.isFinite(p.close) || p.close <= 0.0) {
                continue;
            }
            double open = (Double.isFinite(p.open) && p.open > 0.0) ? p.open : p.close;
            double high = (Double.isFinite(p.high) && p.high > 0.0) ? p.high : Math.max(open, p.close);
            double low = (Double.isFinite(p.low) && p.low > 0.0) ? p.low : Math.min(open, p.close);
            if (high < Math.max(open, p.close)) {
                high = Math.max(open, p.close);
            }
            if (low > Math.min(open, p.close)) {
                low = Math.min(open, p.close);
            }
            double volume = (Double.isFinite(p.volume) && p.volume > 0.0) ? p.volume : 0.0;
            out.add(new BarDaily(jpTicker, p.date, open, high, low, p.close, volume));
        }
        return out;
    }

/**
 * 方法说明：toDailyPrices，负责转换数据结构用于后续处理。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private List<DailyPrice> toDailyPrices(List<BarDaily> bars) {
        if (bars == null || bars.isEmpty()) {
            return List.of();
        }
        List<DailyPrice> out = new ArrayList<>(bars.size());
        for (BarDaily b : bars) {
            LocalDate d = b.tradeDate;
            if (d == null || !Double.isFinite(b.close) || b.close <= 0.0) {
                continue;
            }
            out.add(new DailyPrice(d, b.close));
        }
        return out;
    }

    private List<String> buildNewsQueries(UniverseRecord record, String watchItem, String yahooTicker) {
        List<String> queries = new ArrayList<>();
        if (record.name != null && !record.name.trim().isEmpty()) {
            queries.add(record.name.trim());
        }
        if (record.code != null && !record.code.trim().isEmpty()) {
            queries.add(record.code.trim());
        }
        String company = industryService.companyNameOf(yahooTicker);
        if (company != null && !company.trim().isEmpty()) {
            queries.add(company.trim());
        }
        String industry = industryService.industryOf(yahooTicker);
        if (industry != null && !industry.trim().isEmpty()) {
            String prefix = (company == null || company.trim().isEmpty()) ? safeText(record.name) : company.trim();
            if (prefix.isEmpty()) {
                prefix = safeText(record.code);
            }
            queries.add((prefix + " " + industry).trim());
        }
        if (watchItem != null && !watchItem.trim().isEmpty()) {
            queries.add(watchItem.trim());
        }
        return queries;
    }

    private String toYahooTicker(TickerSpec tickerSpec, UniverseRecord record) {
        String normalized = safeText(tickerSpec == null ? "" : tickerSpec.normalized).toUpperCase(Locale.ROOT);
        if (normalized.endsWith(".T")) {
            return normalized;
        }
        if (normalized.endsWith(".JP")) {
            return normalized.substring(0, normalized.length() - 3) + ".T";
        }
        if (record != null && record.code != null && record.code.trim().matches("\\d{4,6}")) {
            return record.code.trim() + ".T";
        }
        if (record != null && record.ticker != null && record.ticker.toLowerCase(Locale.ROOT).endsWith(".jp")) {
            String code = record.ticker.substring(0, record.ticker.length() - 3);
            return code.toUpperCase(Locale.ROOT) + ".T";
        }
        return normalized;
    }

    private String toYahooTicker(UniverseRecord record) {
        if (record == null) {
            return "";
        }
        String code = safeText(record.code).toUpperCase(Locale.ROOT);
        if (!code.isEmpty()) {
            return code + ".T";
        }
        String ticker = safeText(record.ticker).toLowerCase(Locale.ROOT);
        if (ticker.endsWith(".jp")) {
            return ticker.substring(0, ticker.length() - 3).toUpperCase(Locale.ROOT) + ".T";
        }
        if (ticker.endsWith(".t")) {
            return ticker.toUpperCase(Locale.ROOT);
        }
        return ticker.toUpperCase(Locale.ROOT);
    }

    private String resolveCompanyLocalName(UniverseRecord record, String yahooTicker) {
        if (record.name != null && !record.name.trim().isEmpty()) {
            return record.name.trim();
        }
        String fromIndustry = industryService.companyNameOf(yahooTicker);
        if (fromIndustry != null && !fromIndustry.trim().isEmpty() && !fromIndustry.equalsIgnoreCase(yahooTicker)) {
            return fromIndustry.trim();
        }
        if (record.code != null && !record.code.trim().isEmpty()) {
            return record.code.trim();
        }
        return record.ticker == null ? "" : record.ticker;
    }

/**
 * 方法说明：buildDisplayName，负责构建目标对象或输出内容。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String buildDisplayName(String localName, String code, String industryZh, String industryEn) {
        String n = safeText(localName);
        String c = safeText(code);
        String zh = safeText(industryZh);
        String en = safeText(industryEn);
        String industry;
        if (!zh.isEmpty() && !en.isEmpty()) {
            industry = zh.equalsIgnoreCase(en) ? zh : (zh + "/" + en);
        } else if (!zh.isEmpty()) {
            industry = zh;
        } else if (!en.isEmpty()) {
            industry = en;
        } else {
            industry = "Unknown";
        }
        if (n.isEmpty()) {
            n = c;
        }
        if (c.isEmpty()) {
            return String.format(Locale.US, "%s (%s)", n, industry);
        }
        return String.format(Locale.US, "%s %s (%s)", n, c, industry);
    }

/**
 * 方法说明：computePctChange，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double computePctChange(Double last, Double prev) {
        if (last == null || prev == null || !Double.isFinite(last) || !Double.isFinite(prev) || prev == 0.0) {
            return 0.0;
        }
        return (last - prev) / prev * 100.0;
    }

/**
 * 方法说明：detectPriceSuspects，负责检测条件并输出判断结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private Set<String> detectPriceSuspects(List<WatchlistAnalysis> rows) {
        if (rows == null || rows.isEmpty()) {
            return Set.of();
        }
        int duplicateMinCount = Math.max(2, config.getInt("watchlist.price.duplicate_min_count", 2));
        Map<String, List<WatchlistAnalysis>> grouped = new LinkedHashMap<>();
        for (WatchlistAnalysis row : rows) {
            if (row == null || !Double.isFinite(row.lastClose) || row.lastClose <= 0.0) {
                continue;
            }
            String tradeDate = safeText(row.priceTimestamp);
            if (tradeDate.isEmpty()) {
                continue;
            }
            String key = tradeDate + "|" + String.format(Locale.US, "%.8f", row.lastClose);
            grouped.computeIfAbsent(key, x -> new ArrayList<>()).add(row);
        }

        Set<String> suspects = new HashSet<>();
        for (Map.Entry<String, List<WatchlistAnalysis>> entry : grouped.entrySet()) {
            List<WatchlistAnalysis> bucket = entry.getValue();
            if (bucket.size() < duplicateMinCount) {
                continue;
            }
            LinkedHashMap<String, String> byTicker = new LinkedHashMap<>();
            for (WatchlistAnalysis row : bucket) {
                String tickerKey = safeText(row.ticker).toLowerCase(Locale.ROOT);
                if (!tickerKey.isEmpty()) {
                    byTicker.putIfAbsent(tickerKey, safeText(row.ticker).toUpperCase(Locale.ROOT));
                }
            }
            if (byTicker.size() < duplicateMinCount) {
                continue;
            }
            suspects.addAll(byTicker.keySet());
            WatchlistAnalysis sample = bucket.get(0);
            System.err.println(String.format(
                    Locale.US,
                    "WARN: [PRICE] duplicate_trade_close tradeDate=%s lastClose=%.4f tickers=%s",
                    safeText(sample.priceTimestamp),
                    sample.lastClose,
                    String.join(",", byTicker.values())
            ));
        }
        return suspects;
    }

/**
 * 方法说明：copyWatchRowWithSuspect，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private WatchlistAnalysis copyWatchRowWithSuspect(WatchlistAnalysis row, boolean suspect) {
        return new WatchlistAnalysis(
                row.watchItem,
                row.code,
                row.ticker,
                row.displayName,
                row.companyNameLocal,
                row.industryZh,
                row.industryEn,
                row.resolvedMarket,
                row.resolveStatus,
                row.normalizedTicker,
                row.lastClose,
                row.prevClose,
                row.pctChange,
                row.dataSource,
                row.priceTimestamp,
                row.barsCount,
                row.cacheHit,
                row.fetchLatencyMs,
                row.fetchSuccess,
                row.indicatorReady,
                suspect,
                row.totalScore,
                row.rating,
                row.risk,
                row.aiTriggered,
                row.gateReason,
                row.newsCount,
                row.newsSource,
                row.aiSummary,
                row.newsDigests,
                row.technicalScore,
                row.technicalStatus,
                row.technicalReasonsJson,
                row.technicalIndicatorsJson,
                row.diagnosticsJson,
                row.error,
                row.priceHistory
        );
    }

/**
 * 方法说明：trimChars，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String trimChars(String text, int maxChars) {
        String t = safeText(text);
        if (t.length() <= maxChars) {
            return t;
        }
        return t.substring(0, Math.max(0, maxChars - 3)) + "...";
    }

/**
 * 方法说明：joinErrors，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String joinErrors(String a, String b) {
        String x = safeText(a);
        String y = safeText(b);
        if (x.isEmpty()) {
            return y;
        }
        if (y.isEmpty()) {
            return x;
        }
        if (x.equals(y)) {
            return x;
        }
        return x + " | " + y;
    }

/**
 * 方法说明：loadPreviousCandidateScoreMap，负责加载配置或数据。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private Map<String, Double> loadPreviousCandidateScoreMap(long beforeRunId, int limit) {
        Map<String, Double> out = new HashMap<>();
        int topK = Math.max(30, limit);
        try {
            Optional<RunRow> previous = runDao.findLatestRunWithCandidatesBefore(beforeRunId);
            if (previous.isEmpty()) {
                return out;
            }
            List<ScoredCandidate> prevCandidates = runDao.listScoredCandidates(previous.get().id, topK);
            for (ScoredCandidate c : prevCandidates) {
                if (c == null || c.ticker == null || c.ticker.trim().isEmpty()) {
                    continue;
                }
                out.put(c.ticker.trim().toLowerCase(Locale.ROOT), c.score);
            }
        } catch (Exception ignored) {
            // Best effort. Missing prior data should not block report generation.
        }
        return out;
    }

/**
 * 方法说明：loadScanSummary，负责加载配置或数据。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private ScanSummaryEnvelope loadScanSummary(long runId, ScanStats fallbackStats, RunRow fallbackRun, int fallbackTotal) {
        try {
            ScanResultSummary summary = scanResultDao.summarizeByRun(runId);
            if (summary.total > 0) {
                return new ScanSummaryEnvelope(summary, "SCAN_RESULTS_DB", "com.stockbot.jp.db.ScanResultDao#summarizeByRun(...)");
            }
        } catch (Exception e) {
            System.err.println("WARN: failed to summarize scan_results for run_id=" + runId + ", err=" + e.getMessage());
        }

        Map<ScanFailureReason, Integer> failure = zeroFailureMap();
        Map<ScanFailureReason, Integer> requestFailure = zeroFailureMap();
        Map<DataInsufficientReason, Integer> insufficient = zeroInsufficientMap();

        if (fallbackStats != null) {
            int total = Math.max(Math.max(0, fallbackTotal), fallbackStats.scanned + fallbackStats.failed);
            for (ScanFailureReason reason : ScanFailureReason.values()) {
                failure.put(reason, fallbackStats.failureReason(reason));
                requestFailure.put(reason, fallbackStats.requestFailure(reason));
            }
            for (DataInsufficientReason reason : DataInsufficientReason.values()) {
                insufficient.put(reason, fallbackStats.insufficient(reason));
            }
            ScanResultSummary summary = new ScanResultSummary(
                    total,
                    fallbackStats.fetchCoverageCount,
                    fallbackStats.indicatorCoverageCount,
                    failure,
                    requestFailure,
                    insufficient
            );
            return new ScanSummaryEnvelope(summary, "IN_MEMORY_STATS", OWNER_SCAN_SUMMARY);
        }

        CoverageDerivation derived = deriveCoverageFromRunAndCandidates(runId, fallbackRun, fallbackTotal);
        ScanResultSummary summary = new ScanResultSummary(
                derived.total,
                derived.fetchCoverage,
                derived.indicatorCoverage,
                failure,
                requestFailure,
                insufficient
        );
        return new ScanSummaryEnvelope(summary, "DERIVED", OWNER_DERIVE_COVERAGE);
    }

/**
 * 方法说明：zeroFailureMap，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private Map<ScanFailureReason, Integer> zeroFailureMap() {
        Map<ScanFailureReason, Integer> out = new EnumMap<>(ScanFailureReason.class);
        for (ScanFailureReason reason : ScanFailureReason.values()) {
            out.put(reason, 0);
        }
        return out;
    }

/**
 * 方法说明：zeroInsufficientMap，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private Map<DataInsufficientReason, Integer> zeroInsufficientMap() {
        Map<DataInsufficientReason, Integer> out = new EnumMap<>(DataInsufficientReason.class);
        for (DataInsufficientReason reason : DataInsufficientReason.values()) {
            out.put(reason, 0);
        }
        return out;
    }

/**
 * 方法说明：deriveCoverageFromRunAndCandidates，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private CoverageDerivation deriveCoverageFromRunAndCandidates(long runId, RunRow fallbackRun, int fallbackTotal) {
        int minHistoryBars = Math.max(120, config.getInt("scan.min_history_bars", 180));
        int candidateLimit = 600;
        List<String> tickers = List.of();
        try {
            tickers = runDao.listCandidateTickers(runId, candidateLimit);
        } catch (Exception ignored) {
            // best effort.
        }

        int fetch = 0;
        int indicator = 0;
        try {
            if (!tickers.isEmpty()) {
                fetch = barDailyDao.countTickersWithMinBars(tickers, 1);
                indicator = barDailyDao.countTickersWithMinBars(tickers, minHistoryBars);
            }
        } catch (Exception ignored) {
            // best effort.
        }

        int fallbackScanned = fallbackRun == null ? 0 : Math.max(0, fallbackRun.scannedSize);
        int fallbackCandidate = fallbackRun == null ? 0 : Math.max(0, fallbackRun.candidateSize);
        int total = Math.max(Math.max(0, fallbackTotal), fallbackScanned);
        if (total <= 0) {
            total = Math.max(fetch, tickers.size());
        }
        if (fetch <= 0) {
            fetch = Math.max(fallbackScanned, Math.max(fallbackCandidate, tickers.size()));
        }
        if (indicator <= 0) {
            indicator = Math.max(fallbackCandidate, Math.min(fetch, tickers.size()));
        }
        if (total < fetch) {
            total = fetch;
        }
        if (indicator > fetch) {
            indicator = fetch;
        }
        return new CoverageDerivation(total, fetch, indicator);
    }

/**
 * 方法说明：captureConfigSnapshot，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void captureConfigSnapshot(Diagnostics diagnostics) {
        for (String key : DIAGNOSTIC_CONFIG_KEYS) {
            Config.ResolvedValue resolved = config.resolve(key);
            diagnostics.addConfig(resolved.key, resolved.value, resolved.source);
        }
    }

/**
 * 方法说明：addMarketDataSourceStats，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void addMarketDataSourceStats(Diagnostics diagnostics, long runId, ScanStats fallbackStats) {
        Map<String, Integer> out = new LinkedHashMap<>();
        try {
            out.putAll(scanResultDao.dataSourceCountsByRun(runId));
        } catch (Exception ignored) {
            // fallback below
        }
        if (out.isEmpty() && fallbackStats != null) {
            out.put("yahoo", fallbackStats.sourceYahooCount);
            out.put("cache", fallbackStats.sourceCacheCount);
            out.put("other", fallbackStats.sourceUnknownCount);
        }
        if (out.isEmpty()) {
            out.put("yahoo", 0);
            out.put("cache", 0);
            out.put("other", 0);
        }
        diagnostics.addDataSourceStat("market_yahoo", out.getOrDefault("yahoo", 0));
        diagnostics.addDataSourceStat("market_cache", out.getOrDefault("cache", 0));
        diagnostics.addDataSourceStat("market_other", out.getOrDefault("other", 0));
    }

/**
 * 方法说明：addWatchlistCoverageDiagnostics，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void addWatchlistCoverageDiagnostics(Diagnostics diagnostics, List<WatchlistAnalysis> watchRows) {
        int total = 0;
        int fetch = 0;
        int indicator = 0;
        if (watchRows != null) {
            for (WatchlistAnalysis row : watchRows) {
                if (row == null) {
                    continue;
                }
                if ("JP".equalsIgnoreCase(safeText(row.resolvedMarket))) {
                    total++;
                    if (row.fetchSuccess) {
                        fetch++;
                    }
                    if (row.indicatorReady) {
                        indicator++;
                    }
                }
                String source = safeText(row.dataSource).toLowerCase(Locale.ROOT);
                if (source.isEmpty()) {
                    source = "other";
                }
                diagnostics.addDataSourceStat("watchlist_" + source,
                        diagnostics.dataSourceStats.getOrDefault("watchlist_" + source, 0) + 1);
            }
        }
        diagnostics.addCoverage(
                "watchlist_fetch_coverage",
                fetch,
                total,
                "WATCHLIST_ROWS",
                OWNER_RUNNER + "#addWatchlistCoverageDiagnostics(...)"
        );
        diagnostics.addCoverage(
                "watchlist_indicator_coverage",
                indicator,
                total,
                "WATCHLIST_ROWS",
                OWNER_RUNNER + "#addWatchlistCoverageDiagnostics(...)"
        );
    }

/**
 * 方法说明：addMarketCoverageDiagnostics，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void addMarketCoverageDiagnostics(
            Diagnostics diagnostics,
            ScanResultSummary summary,
            int fallbackDenominator,
            String preferredScope,
            String source,
            String owner
    ) {
        int marketTotal = summary == null ? 0 : Math.max(summary.total, Math.max(0, fallbackDenominator));
        int marketFetch = summary == null ? 0 : Math.max(0, summary.fetchCoverage);
        int marketIndicator = summary == null ? 0 : Math.max(0, summary.indicatorCoverage);
        diagnostics.addCoverage("market_scan_fetch_coverage", marketFetch, marketTotal, source, owner);
        diagnostics.addCoverage("market_scan_indicator_coverage", marketIndicator, marketTotal, source, owner);

        boolean marketAvailable = marketTotal > 0;
        boolean watchAvailable = diagnostics.coverages.containsKey("watchlist_fetch_coverage");
        if ("WATCHLIST".equalsIgnoreCase(preferredScope) && watchAvailable) {
            diagnostics.selectCoverage(
                    "WATCHLIST",
                    "watchlist_fetch_coverage",
                    "watchlist_indicator_coverage",
                    "WATCHLIST_ROWS",
                    OWNER_RUNNER + "#addWatchlistCoverageDiagnostics(...)"
            );
            diagnostics.addNote("coverage_scope switched to WATCHLIST because market summary is unavailable.");
            return;
        }

        if (marketAvailable) {
            diagnostics.selectCoverage(
                    "MARKET",
                    "market_scan_fetch_coverage",
                    "market_scan_indicator_coverage",
                    source,
                    owner
            );
        } else if (watchAvailable) {
            diagnostics.selectCoverage(
                    "WATCHLIST",
                    "watchlist_fetch_coverage",
                    "watchlist_indicator_coverage",
                    "WATCHLIST_ROWS",
                    OWNER_RUNNER + "#addWatchlistCoverageDiagnostics(...)"
            );
            diagnostics.addNote("coverage_scope switched to WATCHLIST because market summary is unavailable.");
        } else {
            diagnostics.selectCoverage("MARKET", "market_scan_fetch_coverage", "market_scan_indicator_coverage", source, owner);
        }
    }

/**
 * 方法说明：collectPolymarketSignals，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private PolymarketResult collectPolymarketSignals(List<WatchlistAnalysis> watchRows, Diagnostics diagnostics) {
        Throwable runtimeError = null;
        PolymarketSignalReport report;
        try {
            report = polymarketService.collectSignals(watchRows);
        } catch (Exception e) {
            runtimeError = e;
            report = PolymarketSignalReport.disabled("runtime_error: " + e.getClass().getSimpleName());
        }
        boolean enabledConfig = config.getBoolean("polymarket.enabled", true);
        FeatureResolution resolution = FeatureStatusResolver.resolveFeatureStatus(
                "polymarket.enabled",
                enabledConfig,
                true,
                runtimeError,
                OWNER_FEATURE_RESOLVE
        );
        diagnostics.addFeatureStatus("polymarket", resolution);
        return new PolymarketResult(report, resolution);
    }

/**
 * 方法说明：buildNewsDigestLines，负责构建目标对象或输出内容。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private List<String> buildNewsDigestLines(List<NewsItem> news) {
        if (news == null || news.isEmpty()) {
            return List.of();
        }
        List<String> lines = new ArrayList<>();
        for (NewsItem item : news) {
            if (item == null) {
                continue;
            }
            String title = safeText(item.title);
            if (title.isEmpty()) {
                continue;
            }
            String source = safeText(item.source);
            String ts = item.publishedAt == null ? "" : NEWS_TS_FMT.format(item.publishedAt);
            StringBuilder line = new StringBuilder();
            line.append(trimChars(title, 70));
            if (!source.isEmpty()) {
                line.append(" | ").append(source);
            }
            if (!ts.isEmpty()) {
                line.append(" | ").append(ts);
            }
            lines.add(line.toString());
            if (lines.size() >= 3) {
                break;
            }
        }
        return lines;
    }

/**
 * 方法说明：safeText，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String safeText(String value) {
        return value == null ? "" : value.trim();
    }

/**
 * 方法说明：blankTo，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String blankTo(String value, String fallback) {
        String t = safeText(value);
        return t.isEmpty() ? fallback : t;
    }

/**
 * 方法说明：safeDouble，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double safeDouble(Double value) {
        if (value == null || !Double.isFinite(value)) {
            return 0.0;
        }
        return value;
    }

/**
 * 方法说明：sanitizeWatchlist，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private List<String> sanitizeWatchlist(List<String> watchlist) {
        List<String> out = new ArrayList<>();
        if (watchlist == null) {
            return out;
        }
        for (String raw : watchlist) {
            if (raw == null) {
                continue;
            }
            String token = raw.trim();
            if (token.isEmpty()) {
                continue;
            }
            out.add(token);
        }
        return out;
    }

/**
 * 方法说明：parseNonJpHandling，负责解析输入内容并转换结构。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private NonJpHandling parseNonJpHandling(String raw) {
        String value = raw == null ? "" : raw.trim().toUpperCase(Locale.ROOT);
        if ("PROCESS_SEPARATELY".equals(value)) {
            return NonJpHandling.PROCESS_SEPARATELY;
        }
        return NonJpHandling.SKIP_WITH_REASON;
    }

/**
 * 方法说明：scanUniverse，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private ScanStats scanUniverse(
            long runId,
            List<UniverseRecord> universe,
            int topN,
            int segmentNo,
            int segmentCount,
            String segmentLabel
    ) throws InterruptedException {
        int total = universe.size();
        if (total == 0) {
            return new ScanStats(topN);
        }
        int threads = Math.max(1, config.getInt("scan.threads", 8));
        int logEvery = Math.max(0, config.getInt("scan.progress.log_every", 100));
        long startedNanos = System.nanoTime();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CompletionService<TickerScanResult> completion = new ExecutorCompletionService<>(pool);
        for (UniverseRecord record : universe) {
            completion.submit(new TickerTask(record));
        }

        ScanStats stats = new ScanStats(topN);
        List<TickerScanResult> scanRows = new ArrayList<>(total);
        try {
            for (int i = 0; i < total; i++) {
                Future<TickerScanResult> future = completion.take();
                try {
                    TickerScanResult result = future.get();
                    scanRows.add(result);
                    stats.recordFetch(result.downloadNanos, result.parseNanos, result.dataSource);
                    stats.recordScanOutcome(result);
                    if (result.error != null) {
                        stats.failed++;
                    } else {
                        if (result.bars != null && !result.bars.isEmpty()) {
                            boolean shouldUpsert = "yahoo".equalsIgnoreCase(safeText(result.dataSource));
                            if (shouldUpsert) {
                                long upsertStarted = System.nanoTime();
                                try {
                                    int initialDays = Math.max(60, config.getInt("scan.upsert.initial_days", 300));
                                    int recentDays = Math.max(1, config.getInt("scan.upsert.incremental_recent_days", 10));
                                    int upsertedBars = barDailyDao.upsertBarsIncremental(
                                            result.universe.ticker,
                                            result.bars,
                                            "yahoo",
                                            initialDays,
                                            recentDays
                                    );
                                    long upsertNanos = System.nanoTime() - upsertStarted;
                                    stats.recordUpsert(upsertNanos, upsertedBars);
                                } catch (SQLException e) {
                                    stats.failed++;
                                    System.err.println("Stage[upsert] failed ticker=" + result.universe.ticker + ", err=" + e.getMessage());
                                    continue;
                                }
                            }
                            stats.scanned++;
                        }
                        if (result.candidate != null) {
                            stats.addCandidate(result.candidate);
                        }
                    }
                } catch (ExecutionException e) {
                    stats.failed++;
                }

                int completed = i + 1;
                if (shouldLogProgress(completed, total, logEvery)) {
                    logScanProgress(
                            segmentNo,
                            segmentCount,
                            segmentLabel,
                            completed,
                            total,
                            stats,
                            startedNanos
                    );
                }
            }
        } finally {
            pool.shutdown();
        }
        try {
            scanResultDao.insertBatch(runId, scanRows);
        } catch (SQLException e) {
            System.err.println("WARN: failed to persist scan_results for run_id=" + runId + ", err=" + e.getMessage());
        }
        return stats;
    }

/**
 * 方法说明：shouldLogProgress，负责判断条件是否满足。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private boolean shouldLogProgress(int completed, int total, int logEvery) {
        if (completed >= total) {
            return true;
        }
        if (logEvery <= 0) {
            return false;
        }
        return completed % logEvery == 0;
    }

/**
 * 方法说明：logScanProgress，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void logScanProgress(
            int segmentNo,
            int segmentCount,
            String segmentLabel,
            int completed,
            int total,
            ScanStats stats,
            long startedNanos
    ) {
        long elapsedSec = Math.max(0L, Math.round((System.nanoTime() - startedNanos) / 1_000_000_000.0));
        int remaining = Math.max(0, total - completed);
        long etaSec = completed <= 0 ? 0L : Math.round(elapsedSec * (remaining / (double) completed));
        double pct = total <= 0 ? 100.0 : completed * 100.0 / total;
        String elapsedText = formatSeconds(elapsedSec);
        String etaText = formatSeconds(etaSec);
        System.out.println(String.format(
                Locale.US,
                "Progress segment %d/%d market=%s done=%d/%d (%.1f%%) scanned=%d failed=%d candidates=%d elapsed=%s eta=%s",
                segmentNo,
                segmentCount,
                segmentLabel,
                completed,
                total,
                pct,
                stats.scanned,
                stats.failed,
                stats.candidateCount,
                elapsedText,
                etaText
        ));
        System.out.println(String.format(
                Locale.US,
                "Stage metrics: download(avg=%.1fms,total=%.2fs,n=%d) parse(avg=%.1fms,total=%.2fs,n=%d) upsert(avg=%.1fms,total=%.2fs,ops=%d,bars=%d) source(yahoo=%d,cache=%d,unknown=%d) coverage(fetch=%d,indicator=%d) failure(timeout=%d,http_404/no_data=%d,parse_error=%d,rate_limit=%d,stale=%d,history_short=%d,filtered_non_tradable=%d,other=%d)",
                avgMillis(stats.downloadNanosTotal, stats.downloadCount),
                seconds(stats.downloadNanosTotal),
                stats.downloadCount,
                avgMillis(stats.parseNanosTotal, stats.parseCount),
                seconds(stats.parseNanosTotal),
                stats.parseCount,
                avgMillis(stats.upsertNanosTotal, stats.upsertOps),
                seconds(stats.upsertNanosTotal),
                stats.upsertOps,
                stats.upsertBarCount,
                stats.sourceYahooCount,
                stats.sourceCacheCount,
                stats.sourceUnknownCount,
                stats.fetchCoverageCount,
                stats.indicatorCoverageCount,
                stats.requestFailure(ScanFailureReason.TIMEOUT),
                stats.requestFailure(ScanFailureReason.HTTP_404_NO_DATA),
                stats.requestFailure(ScanFailureReason.PARSE_ERROR),
                stats.requestFailure(ScanFailureReason.RATE_LIMIT),
                stats.failureReason(ScanFailureReason.STALE),
                stats.failureReason(ScanFailureReason.HISTORY_SHORT),
                stats.failureReason(ScanFailureReason.FILTERED_NON_TRADABLE),
                stats.failureReason(ScanFailureReason.OTHER) + stats.requestFailure(ScanFailureReason.OTHER)
        ));
    }

/**
 * 方法说明：formatSeconds，负责格式化数据用于展示或传输。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String formatSeconds(long seconds) {
        long sec = Math.max(0L, seconds);
        long h = sec / 3600;
        long m = (sec % 3600) / 60;
        long s = sec % 60;
        return String.format(Locale.US, "%02d:%02d:%02d", h, m, s);
    }

/**
 * 方法说明：avgMillis，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double avgMillis(long nanos, int count) {
        if (count <= 0) {
            return 0.0;
        }
        return nanos / 1_000_000.0 / count;
    }

/**
 * 方法说明：seconds，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double seconds(long nanos) {
        return nanos / 1_000_000_000.0;
    }

/**
 * 方法说明：nanosToMillis，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private long nanosToMillis(long nanos) {
        return Math.max(0L, Math.round(nanos / 1_000_000.0));
    }

/**
 * 方法说明：normalizeRequestFailureCategory，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String normalizeRequestFailureCategory(String category, String errorMessage) {
        String raw = category == null ? "" : category.trim().toLowerCase(Locale.ROOT);
        String msg = errorMessage == null ? "" : errorMessage.trim().toLowerCase(Locale.ROOT);

        if (raw.isEmpty()) {
            raw = msg;
        }

        if (raw.contains("timeout") || msg.contains("timed out")) {
            return "timeout";
        }
        if ("no_data".equals(raw) || msg.contains("no_data") || msg.contains("404")) {
            return "no_data";
        }
        if ("rate_limit".equals(raw) || msg.contains("rate limit")) {
            return "rate_limit";
        }
        if ("parse_error".equals(raw)
                || msg.contains("parse")
                || msg.contains("json")) {
            return "parse_error";
        }
        if ("other".equals(raw)) {
            return "other";
        }
        return raw.isEmpty() ? "other" : raw;
    }

/**
 * 方法说明：requestFailureReason，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private ScanFailureReason requestFailureReason(String category) {
        String c = category == null ? "" : category.trim().toLowerCase(Locale.ROOT);
        if ("timeout".equals(c)) {
            return ScanFailureReason.TIMEOUT;
        }
        if ("no_data".equals(c)) {
            return ScanFailureReason.HTTP_404_NO_DATA;
        }
        if ("parse_error".equals(c)) {
            return ScanFailureReason.PARSE_ERROR;
        }
        if ("rate_limit".equals(c)) {
            return ScanFailureReason.RATE_LIMIT;
        }
        if (c.isEmpty()) {
            return ScanFailureReason.NONE;
        }
        return ScanFailureReason.OTHER;
    }

/**
 * 方法说明：scanTicker，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private TickerScanResult scanTicker(UniverseRecord universe) {
        long started = System.nanoTime();
        try {
            int minHistoryBars = Math.max(120, config.getInt("scan.min_history_bars", 180));
            int freshDays = Math.max(0, config.getInt("scan.fresh_days", config.getInt("scan.cache.fresh_days", 2)));
            boolean cachePreferEnabled = config.getBoolean("scan.cache.prefer_enabled", true);
            int cacheFreshDays = Math.max(0, config.getInt("scan.cache.fresh_days", 2));
            boolean retryWhenCacheExists = config.getBoolean("scan.network.retry_when_cache_exists", false);

            List<BarDaily> cachedBars = loadCachedBars(universe.ticker);
            boolean cacheHasScreeningShape = hasScreeningShape(cachedBars);
            String yahooTicker = toYahooTicker(universe);
            if (cachePreferEnabled
                    && isCacheFreshEnough(cachedBars, minHistoryBars, cacheFreshDays)
                    && cacheHasScreeningShape) {
                return evaluateBars(
                        universe,
                        cachedBars,
                        0L,
                        0L,
                        "cache",
                        false,
                        "",
                        nanosToMillis(System.nanoTime() - started),
                        ScanFailureReason.NONE
                );
            }

            List<BarDaily> yahooBars = fetchBarsFromYahoo(universe.ticker, yahooTicker);
            boolean yahooHasScreeningShape = hasScreeningShape(yahooBars);
            if (!yahooBars.isEmpty() && yahooHasScreeningShape) {
                return evaluateBars(
                        universe,
                        yahooBars,
                        0L,
                        0L,
                        "yahoo",
                        false,
                        "",
                        nanosToMillis(System.nanoTime() - started),
                        ScanFailureReason.NONE
                );
            }

            if (!cachedBars.isEmpty() && !retryWhenCacheExists) {
                return evaluateBars(
                        universe,
                        cachedBars,
                        0L,
                        0L,
                        "cache",
                        false,
                        "",
                        nanosToMillis(System.nanoTime() - started),
                        ScanFailureReason.NONE
                );
            }

            if (!yahooBars.isEmpty()) {
                return evaluateBars(
                        universe,
                        yahooBars,
                        0L,
                        0L,
                        "yahoo",
                        false,
                        "",
                        nanosToMillis(System.nanoTime() - started),
                        ScanFailureReason.NONE
                );
            }

            if (!cachedBars.isEmpty()) {
                return evaluateBars(
                        universe,
                        cachedBars,
                        0L,
                        0L,
                        "cache",
                        true,
                        "no_data",
                        nanosToMillis(System.nanoTime() - started),
                        ScanFailureReason.HTTP_404_NO_DATA
                );
            }

            return TickerScanResult.failed(
                    universe,
                    "no_data",
                    0L,
                    0L,
                    "yahoo",
                    true,
                    "no_data",
                    nanosToMillis(System.nanoTime() - started),
                    false,
                    0,
                    null,
                    Double.NaN,
                    false,
                    DataInsufficientReason.NO_DATA,
                    ScanFailureReason.HTTP_404_NO_DATA
            );
        } catch (Exception e) {
            return TickerScanResult.failed(
                    universe,
                    e.getMessage(),
                    0L,
                    0L,
                    "",
                    false,
                    "",
                    nanosToMillis(System.nanoTime() - started),
                    false,
                    0,
                    null,
                    Double.NaN,
                    false,
                    DataInsufficientReason.NONE,
                    ScanFailureReason.OTHER
            );
        }
    }

/**
 * 方法说明：evaluateBars，负责评估条件并输出判定结论。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private TickerScanResult evaluateBars(
            UniverseRecord universe,
            List<BarDaily> bars,
            long downloadNanos,
            long parseNanos,
            String dataSource,
            boolean requestFailed,
            String requestFailureCategory,
            long fetchLatencyMs,
            ScanFailureReason requestFailureReason
    ) {
        int barsCount = bars == null ? 0 : bars.size();
        LocalDate lastTradeDate = lastTradeDateOf(bars);
        double lastClose = lastCloseOf(bars);
        boolean fetchSuccess = barsCount > 0;

        if (!fetchSuccess) {
            ScanFailureReason finalReason = requestFailureReason == ScanFailureReason.NONE
                    ? ScanFailureReason.HTTP_404_NO_DATA
                    : requestFailureReason;
            return TickerScanResult.failed(
                    universe,
                    "no_data",
                    downloadNanos,
                    parseNanos,
                    dataSource,
                    requestFailed,
                    requestFailureCategory,
                    fetchLatencyMs,
                    "cache".equalsIgnoreCase(dataSource),
                    barsCount,
                    lastTradeDate,
                    lastClose,
                    false,
                    DataInsufficientReason.NO_DATA,
                    finalReason
            );
        }

        int freshDays = Math.max(0, config.getInt("scan.fresh_days", config.getInt("scan.cache.fresh_days", 2)));
        if (!isBarsFreshEnough(bars, freshDays)) {
            return TickerScanResult.ok(
                    universe,
                    bars,
                    null,
                    downloadNanos,
                    parseNanos,
                    dataSource,
                    requestFailed,
                    requestFailureCategory,
                    fetchLatencyMs,
                    "cache".equalsIgnoreCase(dataSource),
                    barsCount,
                    lastTradeDate,
                    lastClose,
                    true,
                    false,
                    DataInsufficientReason.STALE,
                    ScanFailureReason.STALE
            );
        }

        int minHistoryBars = Math.max(120, config.getInt("scan.min_history_bars", 180));
        if (barsCount < minHistoryBars) {
            return TickerScanResult.ok(
                    universe,
                    bars,
                    null,
                    downloadNanos,
                    parseNanos,
                    dataSource,
                    requestFailed,
                    requestFailureCategory,
                    fetchLatencyMs,
                    "cache".equalsIgnoreCase(dataSource),
                    barsCount,
                    lastTradeDate,
                    lastClose,
                    true,
                    false,
                    DataInsufficientReason.HISTORY_SHORT,
                    ScanFailureReason.HISTORY_SHORT
            );
        }

        try {
            if (!isTradableAndLiquid(bars)) {
                return TickerScanResult.ok(
                        universe,
                        bars,
                        null,
                        downloadNanos,
                        parseNanos,
                        dataSource,
                        requestFailed,
                        requestFailureCategory,
                        fetchLatencyMs,
                        "cache".equalsIgnoreCase(dataSource),
                        barsCount,
                        lastTradeDate,
                        lastClose,
                        true,
                        false,
                        DataInsufficientReason.NONE,
                        ScanFailureReason.FILTERED_NON_TRADABLE
                );
            }

            IndicatorSnapshot ind = indicatorEngine.compute(bars);
            if (ind == null) {
                return TickerScanResult.failed(
                        universe,
                        "indicator_failed",
                        downloadNanos,
                        parseNanos,
                        dataSource,
                        requestFailed,
                        requestFailureCategory,
                        fetchLatencyMs,
                        "cache".equalsIgnoreCase(dataSource),
                        barsCount,
                        lastTradeDate,
                        lastClose,
                        true,
                        DataInsufficientReason.NONE,
                        ScanFailureReason.OTHER
                );
            }

            FilterDecision filter = candidateFilter.evaluate(bars, ind);
            if (!filter.passed) {
                return TickerScanResult.ok(
                        universe,
                        bars,
                        null,
                        downloadNanos,
                        parseNanos,
                        dataSource,
                        requestFailed,
                        requestFailureCategory,
                        fetchLatencyMs,
                        "cache".equalsIgnoreCase(dataSource),
                        barsCount,
                        lastTradeDate,
                        ind.lastClose,
                        true,
                        true,
                        DataInsufficientReason.NONE,
                        ScanFailureReason.NONE
                );
            }

            RiskDecision risk = riskFilter.evaluate(ind);
            if (!risk.passed) {
                return TickerScanResult.ok(
                        universe,
                        bars,
                        null,
                        downloadNanos,
                        parseNanos,
                        dataSource,
                        requestFailed,
                        requestFailureCategory,
                        fetchLatencyMs,
                        "cache".equalsIgnoreCase(dataSource),
                        barsCount,
                        lastTradeDate,
                        ind.lastClose,
                        true,
                        true,
                        DataInsufficientReason.NONE,
                        ScanFailureReason.NONE
                );
            }

            ScoreResult score = scoringEngine.score(ind, risk);
            double minScore = config.getDouble("scan.min_score", 55.0);
            if (score.score < minScore) {
                return TickerScanResult.ok(
                        universe,
                        bars,
                        null,
                        downloadNanos,
                        parseNanos,
                        dataSource,
                        requestFailed,
                        requestFailureCategory,
                        fetchLatencyMs,
                        "cache".equalsIgnoreCase(dataSource),
                        barsCount,
                        lastTradeDate,
                        ind.lastClose,
                        true,
                        true,
                        DataInsufficientReason.NONE,
                        ScanFailureReason.NONE
                );
            }

            String reasonsJson = reasonJsonBuilder.buildReasonsJson(filter, risk, score);
            String indicatorsJson = reasonJsonBuilder.buildIndicatorsJson(ind);
            ScoredCandidate candidate = new ScoredCandidate(
                    universe.ticker,
                    universe.code,
                    universe.name,
                    universe.market,
                    score.score,
                    ind.lastClose,
                    reasonsJson,
                    indicatorsJson
            );
            return TickerScanResult.ok(
                    universe,
                    bars,
                    candidate,
                    downloadNanos,
                    parseNanos,
                    dataSource,
                    requestFailed,
                    requestFailureCategory,
                    fetchLatencyMs,
                    "cache".equalsIgnoreCase(dataSource),
                    barsCount,
                    lastTradeDate,
                    ind.lastClose,
                    true,
                    true,
                    DataInsufficientReason.NONE,
                    ScanFailureReason.NONE
            );
        } catch (Exception e) {
            return TickerScanResult.failed(
                    universe,
                    e.getMessage(),
                    downloadNanos,
                    parseNanos,
                    dataSource,
                    requestFailed,
                    requestFailureCategory,
                    fetchLatencyMs,
                    "cache".equalsIgnoreCase(dataSource),
                    barsCount,
                    lastTradeDate,
                    lastClose,
                    true,
                    DataInsufficientReason.NONE,
                    ScanFailureReason.OTHER
            );
        }
    }

/**
 * 方法说明：lastTradeDateOf，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private LocalDate lastTradeDateOf(List<BarDaily> bars) {
        if (bars == null || bars.isEmpty()) {
            return null;
        }
        for (int i = bars.size() - 1; i >= 0; i--) {
            BarDaily bar = bars.get(i);
            if (bar != null && bar.tradeDate != null) {
                return bar.tradeDate;
            }
        }
        return null;
    }

/**
 * 方法说明：lastCloseOf，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double lastCloseOf(List<BarDaily> bars) {
        if (bars == null || bars.isEmpty()) {
            return Double.NaN;
        }
        for (int i = bars.size() - 1; i >= 0; i--) {
            BarDaily bar = bars.get(i);
            if (bar != null && Double.isFinite(bar.close) && bar.close > 0.0) {
                return bar.close;
            }
        }
        return Double.NaN;
    }

/**
 * 方法说明：isBarsFreshEnough，负责判断条件是否满足。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private boolean isBarsFreshEnough(List<BarDaily> bars, int freshDays) {
        if (bars == null || bars.isEmpty()) {
            return false;
        }
        LocalDate latest = lastTradeDateOf(bars);
        if (latest == null) {
            return false;
        }

        ZoneId zone = ZoneId.of(config.getString("app.zone", "Asia/Tokyo"));
        LocalDate today = LocalDate.now(zone);
        int effectiveFreshDays = Math.max(0, freshDays);
        DayOfWeek dow = today.getDayOfWeek();
        if (dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY) {
            effectiveFreshDays = Math.max(effectiveFreshDays, 2);
        }

        long ageDays = ChronoUnit.DAYS.between(latest, today);
        return ageDays <= effectiveFreshDays;
    }

/**
 * 方法说明：isCacheFreshEnough，负责判断条件是否满足。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private boolean isCacheFreshEnough(List<BarDaily> bars, int minHistoryBars, int freshDays) {
        if (bars == null || bars.size() < minHistoryBars) {
            return false;
        }
        return isBarsFreshEnough(bars, freshDays);
    }

    private boolean hasScreeningShape(List<BarDaily> bars) {
        if (bars == null || bars.isEmpty()) {
            return false;
        }
        for (BarDaily bar : bars) {
            if (bar == null) {
                continue;
            }
            boolean hasVolume = Double.isFinite(bar.volume) && bar.volume > 0.0;
            boolean hasRange = Math.abs(bar.high - bar.low) > 1e-9 || Math.abs(bar.open - bar.close) > 1e-9;
            if (hasVolume || hasRange) {
                return true;
            }
        }
        return false;
    }

/**
 * 方法说明：isTradableAndLiquid，负责判断条件是否满足。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private boolean isTradableAndLiquid(List<BarDaily> bars) {
        if (bars == null || bars.isEmpty()) {
            return false;
        }
        int n = bars.size();
        BarDaily last = bars.get(n - 1);
        if (last == null || !Double.isFinite(last.close) || last.close <= 0.0) {
            return false;
        }

        double minPrice = config.getDouble("scan.tradable.min_price", config.getDouble("scan.min_price", 100.0));
        if (last.close < minPrice) {
            return false;
        }

        int volumeWindow = Math.min(20, n);
        if (volumeWindow <= 0) {
            return false;
        }
        double sumVol = 0.0;
        int zeroVolDays = 0;
        for (int i = n - volumeWindow; i < n; i++) {
            BarDaily bar = bars.get(i);
            if (bar == null) {
                continue;
            }
            double vol = Double.isFinite(bar.volume) ? Math.max(0.0, bar.volume) : 0.0;
            sumVol += vol;
            if (vol <= 0.0) {
                zeroVolDays++;
            }
        }
        double avgVol20 = sumVol / volumeWindow;
        double minAvgVol20 = config.getDouble("scan.tradable.min_avg_volume_20", 50000.0);
        if (avgVol20 < minAvgVol20) {
            return false;
        }

        int maxZeroVolDays = Math.max(0, config.getInt("scan.tradable.max_zero_volume_days_20", 3));
        if (zeroVolDays > maxZeroVolDays) {
            return false;
        }

        int flatLookbackDays = Math.max(1, config.getInt("scan.tradable.flat_lookback_days", 5));
        int lookback = Math.min(flatLookbackDays, n);
        int flatDays = 0;
        for (int i = n - lookback; i < n; i++) {
            BarDaily bar = bars.get(i);
            if (bar == null) {
                continue;
            }
            boolean openCloseFlat = Math.abs(bar.open - bar.close) < 1e-9;
            boolean highLowFlat = Math.abs(bar.high - bar.low) < 1e-9;
            if (openCloseFlat && highLowFlat) {
                flatDays++;
            }
        }
        int maxFlatDays = Math.max(0, config.getInt("scan.tradable.max_flat_days", 3));
        return flatDays <= maxFlatDays;
    }

/**
 * 方法说明：loadCachedBars，负责加载配置或数据。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private List<BarDaily> loadCachedBars(String ticker) {
        try {
            return barDailyDao.loadRecentBars(ticker, maxBars);
        } catch (SQLException ignored) {
            return List.of();
        }
    }

/**
 * 方法说明：safeFinishFailed，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void safeFinishFailed(long runId, Exception e) {
        try {
            String message = e.getClass().getSimpleName() + ": " + e.getMessage();
            runDao.finishRun(runId, "FAILED", 0, 0, 0, 0, null, message);
        } catch (Exception ignored) {
            // best effort
        }
    }

/**
 * 方法说明：prepareBatchPlan，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private BatchPlan prepareBatchPlan(List<UniverseRecord> universe, int topN, boolean resetBatchCheckpoint) throws SQLException {
        boolean batchEnabled = config.getBoolean("scan.batch.enabled", true);
        boolean segmentByMarket = config.getBoolean("scan.batch.segment_by_market", true);
        boolean resumeEnabled = batchEnabled && config.getBoolean("scan.batch.resume_enabled", true);
        int marketChunkSize = Math.max(0, config.getInt("scan.batch.market_chunk_size", 0));
        String checkpointKey = config.getString("scan.batch.checkpoint_key", "daily.scan.batch.checkpoint.v1");

        if (resetBatchCheckpoint && resumeEnabled) {
            metadataDao.delete(checkpointKey);
        }

        List<MarketSegment> segments;
        if (!batchEnabled) {
            segments = List.of(new MarketSegment("ALL", new ArrayList<>(universe)));
        } else if (segmentByMarket) {
            segments = segmentByMarket(universe, marketChunkSize);
        } else {
            segments = segmentByFixedChunk(universe, marketChunkSize <= 0 ? 500 : marketChunkSize);
        }
        if (segments.isEmpty()) {
            segments = List.of(new MarketSegment("ALL", new ArrayList<>(universe)));
        }

        String signature = universeSignature(universe);
        return new BatchPlan(segments, resumeEnabled, checkpointKey, signature, topN);
    }

/**
 * 方法说明：loadBatchState，负责加载配置或数据。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private BatchState loadBatchState(BatchPlan plan, int topN) throws Exception {
        BatchState fresh = new BatchState(new ScanStats(topN), 0);
        if (!plan.resumeEnabled) {
            return fresh;
        }

        Optional<String> checkpointRaw = metadataDao.get(plan.checkpointKey);
        if (checkpointRaw.isEmpty() || checkpointRaw.get().trim().isEmpty()) {
            return fresh;
        }

        BatchCheckpoint checkpoint;
        try {
            checkpoint = BatchCheckpoint.fromJson(checkpointRaw.get());
        } catch (Exception e) {
            clearCheckpoint(plan);
            return fresh;
        }

        if (!checkpoint.universeSignature.equals(plan.universeSignature)
                || checkpoint.segmentCount != plan.segments.size()
                || checkpoint.topN != topN) {
            clearCheckpoint(plan);
            return fresh;
        }

        int next = Math.max(0, Math.min(checkpoint.nextSegmentIndex, plan.segments.size()));
        if (next >= plan.segments.size()) {
            clearCheckpoint(plan);
            return fresh;
        }

        System.out.println(String.format(
                Locale.US,
                "Resume checkpoint found: segment=%d/%d scanned=%d failed=%d candidates=%d",
                next,
                plan.segments.size(),
                checkpoint.scanned,
                checkpoint.failed,
                checkpoint.candidateCount
        ));
        return new BatchState(ScanStats.fromCheckpoint(checkpoint, topN), next);
    }

/**
 * 方法说明：saveCheckpoint，负责保存数据到目标存储。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void saveCheckpoint(BatchPlan plan, BatchState state, int topN) throws SQLException {
        if (!plan.resumeEnabled) {
            return;
        }
        BatchCheckpoint checkpoint = BatchCheckpoint.fromState(plan, state, topN);
        metadataDao.put(plan.checkpointKey, checkpoint.toJson().toString());
    }

/**
 * 方法说明：clearCheckpoint，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private void clearCheckpoint(BatchPlan plan) throws SQLException {
        if (!plan.resumeEnabled) {
            return;
        }
        metadataDao.delete(plan.checkpointKey);
    }

/**
 * 方法说明：segmentByMarket，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private List<MarketSegment> segmentByMarket(List<UniverseRecord> universe, int marketChunkSize) {
        Map<String, List<UniverseRecord>> grouped = new LinkedHashMap<>();
        for (UniverseRecord record : universe) {
            String market = normalizeMarket(record.market);
            grouped.computeIfAbsent(market, k -> new ArrayList<>()).add(record);
        }

        List<MarketSegment> out = new ArrayList<>();
        for (Map.Entry<String, List<UniverseRecord>> e : grouped.entrySet()) {
            List<UniverseRecord> records = e.getValue();
            if (marketChunkSize <= 0 || records.size() <= marketChunkSize) {
                out.add(new MarketSegment(e.getKey(), records));
            } else {
                int chunks = (records.size() + marketChunkSize - 1) / marketChunkSize;
                for (int i = 0; i < chunks; i++) {
                    int from = i * marketChunkSize;
                    int to = Math.min(records.size(), from + marketChunkSize);
                    String key = e.getKey() + "#" + (i + 1) + "/" + chunks;
                    out.add(new MarketSegment(key, new ArrayList<>(records.subList(from, to))));
                }
            }
        }
        return out;
    }

/**
 * 方法说明：segmentByFixedChunk，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private List<MarketSegment> segmentByFixedChunk(List<UniverseRecord> universe, int chunkSize) {
        List<MarketSegment> out = new ArrayList<>();
        for (int i = 0; i < universe.size(); i += chunkSize) {
            int to = Math.min(universe.size(), i + chunkSize);
            String key = "CHUNK#" + (i / chunkSize + 1);
            out.add(new MarketSegment(key, new ArrayList<>(universe.subList(i, to))));
        }
        return out;
    }

/**
 * 方法说明：normalizeMarket，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String normalizeMarket(String market) {
        if (market == null || market.trim().isEmpty()) {
            return "UNKNOWN";
        }
        return market.trim();
    }

/**
 * 方法说明：universeSignature，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String universeSignature(List<UniverseRecord> universe) {
        long hash = 1125899906842597L;
        for (UniverseRecord record : universe) {
            hash = 31L * hash + record.ticker.hashCode();
            hash = 31L * hash + normalizeMarket(record.market).hashCode();
        }
        return universe.size() + ":" + Long.toHexString(hash);
    }

    private static final class MarketScanSnapshot {
        final UniverseUpdateResult updateResult;
        final List<UniverseRecord> universe;
        final int universeSize;
        final int topN;
        final List<ScoredCandidate> topCandidates;
        final List<ScoredCandidate> marketReferenceCandidates;
        final ScanStats stats;
        final int totalSegments;
        final int nextSegmentIndex;
        final boolean partialRun;

        private MarketScanSnapshot(
                UniverseUpdateResult updateResult,
                List<UniverseRecord> universe,
                int universeSize,
                int topN,
                List<ScoredCandidate> topCandidates,
                List<ScoredCandidate> marketReferenceCandidates,
                ScanStats stats,
                int totalSegments,
                int nextSegmentIndex,
                boolean partialRun
        ) {
            this.updateResult = updateResult;
            this.universe = universe == null ? List.of() : universe;
            this.universeSize = universeSize;
            this.topN = topN;
            this.topCandidates = topCandidates == null ? List.of() : topCandidates;
            this.marketReferenceCandidates = marketReferenceCandidates == null ? List.of() : marketReferenceCandidates;
            this.stats = stats;
            this.totalSegments = totalSegments;
            this.nextSegmentIndex = nextSegmentIndex;
            this.partialRun = partialRun;
        }
    }

    private static final class ScanSummaryEnvelope {
        final ScanResultSummary summary;
        final String source;
        final String owner;

        private ScanSummaryEnvelope(ScanResultSummary summary, String source, String owner) {
            this.summary = summary;
            this.source = source == null ? "" : source;
            this.owner = owner == null ? "" : owner;
        }
    }

    private static final class CoverageDerivation {
        final int total;
        final int fetchCoverage;
        final int indicatorCoverage;

        private CoverageDerivation(int total, int fetchCoverage, int indicatorCoverage) {
            this.total = Math.max(0, total);
            this.fetchCoverage = Math.max(0, fetchCoverage);
            this.indicatorCoverage = Math.max(0, indicatorCoverage);
        }
    }

    private static final class PolymarketResult {
        final PolymarketSignalReport report;
        final FeatureResolution resolution;

        private PolymarketResult(PolymarketSignalReport report, FeatureResolution resolution) {
            this.report = report == null ? PolymarketSignalReport.disabled("runtime_error") : report;
            this.resolution = resolution;
        }
    }

    private final class TickerTask implements Callable<TickerScanResult> {
        private final UniverseRecord universe;

        private TickerTask(UniverseRecord universe) {
            this.universe = universe;
        }

/**
 * 方法说明：call，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
        @Override
        public TickerScanResult call() {
            return scanTicker(universe);
        }
    }

    private static final class WatchlistScanResult {
        final ScoredCandidate candidate;
        final boolean filterPassed;
        final boolean riskPassed;
        final boolean fetchSuccess;
        final boolean indicatorReady;
        final Outcome<Void> outcome;
        final String error;

        private WatchlistScanResult(
                ScoredCandidate candidate,
                boolean filterPassed,
                boolean riskPassed,
                boolean fetchSuccess,
                boolean indicatorReady,
                Outcome<Void> outcome,
                String error
        ) {
            this.candidate = candidate;
            this.filterPassed = filterPassed;
            this.riskPassed = riskPassed;
            this.fetchSuccess = fetchSuccess;
            this.indicatorReady = indicatorReady;
            this.outcome = outcome == null ? Outcome.success(null, OWNER_WATCH_SCAN) : outcome;
            this.error = error == null ? "" : error;
        }
    }

    private static final class LegacyWatchResult {
        final StockContext context;
        final boolean aiTriggered;
        final String error;

        private LegacyWatchResult(StockContext context, boolean aiTriggered, String error) {
            this.context = context;
            this.aiTriggered = aiTriggered;
            this.error = error == null ? "" : error;
        }
    }

    private static final class PriceFetchTrace {
        final List<BarDaily> bars;
        final String dataSource;
        final String priceTimestamp;
        final int barsCount;
        final boolean cacheHit;
        final long fetchLatencyMs;
        final boolean requestFailed;
        final String requestFailureCategory;
        final String error;
        final double lastClose;
        final String fallbackPath;

        private PriceFetchTrace(
                List<BarDaily> bars,
                String dataSource,
                String priceTimestamp,
                int barsCount,
                boolean cacheHit,
                long fetchLatencyMs,
                boolean requestFailed,
                String requestFailureCategory,
                String error,
                double lastClose,
                String fallbackPath
        ) {
            this.bars = bars == null ? List.of() : bars;
            this.dataSource = dataSource == null ? "fetch_failed" : dataSource;
            this.priceTimestamp = priceTimestamp == null ? "" : priceTimestamp;
            this.barsCount = Math.max(0, barsCount);
            this.cacheHit = cacheHit;
            this.fetchLatencyMs = Math.max(0L, fetchLatencyMs);
            this.requestFailed = requestFailed;
            this.requestFailureCategory = requestFailureCategory == null ? "" : requestFailureCategory;
            this.error = error == null ? "" : error;
            this.lastClose = lastClose;
            this.fallbackPath = fallbackPath == null ? "" : fallbackPath;
        }

        static PriceFetchTrace fromBars(
                List<BarDaily> bars,
                String dataSource,
                boolean cacheHit,
                long fetchLatencyMs,
                boolean requestFailed,
                String requestFailureCategory,
                String error,
                String fallbackPath
        ) {
            List<BarDaily> normalized = bars == null ? List.of() : bars;
            BarDaily last = null;
            for (int i = normalized.size() - 1; i >= 0; i--) {
                BarDaily b = normalized.get(i);
                if (b == null) {
                    continue;
                }
                if (b.tradeDate != null && Double.isFinite(b.close) && b.close > 0.0) {
                    last = b;
                    break;
                }
            }
            String priceTs = last == null || last.tradeDate == null ? "" : last.tradeDate.toString();
            double lastClose = last == null ? Double.NaN : last.close;
            return new PriceFetchTrace(
                    normalized,
                    dataSource,
                    priceTs,
                    normalized.size(),
                    cacheHit,
                    fetchLatencyMs,
                    requestFailed,
                    requestFailureCategory,
                    error,
                    lastClose,
                    fallbackPath
            );
        }

        static PriceFetchTrace failed(
                String dataSource,
                long fetchLatencyMs,
                boolean requestFailed,
                String requestFailureCategory,
                String error,
                String fallbackPath
        ) {
            return new PriceFetchTrace(
                    List.of(),
                    dataSource,
                    "",
                    0,
                    false,
                    fetchLatencyMs,
                    requestFailed,
                    requestFailureCategory,
                    error,
                    Double.NaN,
                    fallbackPath
            );
        }
    }

    private static final class ScanStats {
        final int topN;
        int scanned;
        int failed;
        int candidateCount;
        int fetchCoverageCount;
        int indicatorCoverageCount;
        final List<ScoredCandidate> topCandidates = new ArrayList<>();
        long downloadNanosTotal;
        long parseNanosTotal;
        long upsertNanosTotal;
        int downloadCount;
        int parseCount;
        int upsertOps;
        long upsertBarCount;
        int sourceYahooCount;
        int sourceCacheCount;
        int sourceUnknownCount;
        final EnumMap<ScanFailureReason, Integer> failureReasonCounts = new EnumMap<>(ScanFailureReason.class);
        final EnumMap<ScanFailureReason, Integer> requestFailureCounts = new EnumMap<>(ScanFailureReason.class);
        final EnumMap<DataInsufficientReason, Integer> insufficientCounts = new EnumMap<>(DataInsufficientReason.class);

        private ScanStats(int topN) {
            this.topN = Math.max(1, topN);
            for (ScanFailureReason reason : ScanFailureReason.values()) {
                failureReasonCounts.put(reason, 0);
                requestFailureCounts.put(reason, 0);
            }
            for (DataInsufficientReason reason : DataInsufficientReason.values()) {
                insufficientCounts.put(reason, 0);
            }
        }

        static ScanStats fromCheckpoint(BatchCheckpoint checkpoint, int topN) {
            ScanStats stats = new ScanStats(topN);
            stats.scanned = checkpoint.scanned;
            stats.failed = checkpoint.failed;
            stats.candidateCount = checkpoint.candidateCount;
            for (ScoredCandidate candidate : checkpoint.topCandidates) {
                stats.addTopCandidate(candidate);
            }
            return stats;
        }

        void addCandidate(ScoredCandidate candidate) {
            candidateCount++;
            addTopCandidate(candidate);
        }

        void merge(ScanStats other) {
            scanned += other.scanned;
            failed += other.failed;
            candidateCount += other.candidateCount;
            fetchCoverageCount += other.fetchCoverageCount;
            indicatorCoverageCount += other.indicatorCoverageCount;
            downloadNanosTotal += other.downloadNanosTotal;
            parseNanosTotal += other.parseNanosTotal;
            upsertNanosTotal += other.upsertNanosTotal;
            downloadCount += other.downloadCount;
            parseCount += other.parseCount;
            upsertOps += other.upsertOps;
            upsertBarCount += other.upsertBarCount;
            sourceYahooCount += other.sourceYahooCount;
            sourceCacheCount += other.sourceCacheCount;
            sourceUnknownCount += other.sourceUnknownCount;
            for (ScanFailureReason reason : ScanFailureReason.values()) {
                failureReasonCounts.put(reason, failureReason(reason) + other.failureReason(reason));
                requestFailureCounts.put(reason, requestFailure(reason) + other.requestFailure(reason));
            }
            for (DataInsufficientReason reason : DataInsufficientReason.values()) {
                insufficientCounts.put(reason, insufficient(reason) + other.insufficient(reason));
            }
            for (ScoredCandidate candidate : other.topCandidates) {
                addTopCandidate(candidate);
            }
        }

        List<ScoredCandidate> topCandidates() {
            List<ScoredCandidate> copy = new ArrayList<>(topCandidates);
            copy.sort(Comparator.comparingDouble((ScoredCandidate c) -> c.score).reversed());
            return copy;
        }

/**
 * 方法说明：addTopCandidate，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
        private void addTopCandidate(ScoredCandidate candidate) {
            topCandidates.add(candidate);
            topCandidates.sort(Comparator.comparingDouble((ScoredCandidate c) -> c.score).reversed());
            if (topCandidates.size() > topN) {
                topCandidates.remove(topCandidates.size() - 1);
            }
        }

        void recordFetch(long downloadNanos, long parseNanos, String dataSource) {
            if (downloadNanos > 0L) {
                downloadNanosTotal += downloadNanos;
                downloadCount++;
            }
            if (parseNanos > 0L) {
                parseNanosTotal += parseNanos;
                parseCount++;
            }

            String src = dataSource == null ? "" : dataSource.trim().toLowerCase(Locale.ROOT);
            if ("yahoo".equals(src)) {
                sourceYahooCount++;
            } else if ("cache".equals(src)) {
                sourceCacheCount++;
            } else {
                sourceUnknownCount++;
            }
        }

        void recordUpsert(long upsertNanos, int bars) {
            upsertNanosTotal += Math.max(0L, upsertNanos);
            upsertOps++;
            upsertBarCount += Math.max(0, bars);
        }

        void recordScanOutcome(TickerScanResult result) {
            if (result == null) {
                return;
            }
            if (result.fetchSuccess) {
                fetchCoverageCount++;
            }
            if (result.indicatorReady) {
                indicatorCoverageCount++;
            }

            ScanFailureReason failureReason = result.failureReason == null ? ScanFailureReason.NONE : result.failureReason;
            if (failureReason != ScanFailureReason.NONE) {
                failureReasonCounts.put(failureReason, failureReason(failureReason) + 1);
            }

            DataInsufficientReason insufficient = result.dataInsufficientReason == null
                    ? DataInsufficientReason.NONE
                    : result.dataInsufficientReason;
            if (insufficient != DataInsufficientReason.NONE) {
                insufficientCounts.put(insufficient, insufficient(insufficient) + 1);
            }

            if (result.requestFailed || (result.requestFailureCategory != null && !result.requestFailureCategory.trim().isEmpty())) {
                ScanFailureReason requestReason = requestReason(result.requestFailureCategory);
                requestFailureCounts.put(requestReason, requestFailure(requestReason) + 1);
            }
        }

        int failureReason(ScanFailureReason reason) {
            if (reason == null) {
                return 0;
            }
            return failureReasonCounts.getOrDefault(reason, 0);
        }

        int requestFailure(ScanFailureReason reason) {
            if (reason == null) {
                return 0;
            }
            return requestFailureCounts.getOrDefault(reason, 0);
        }

        int insufficient(DataInsufficientReason reason) {
            if (reason == null) {
                return 0;
            }
            return insufficientCounts.getOrDefault(reason, 0);
        }

/**
 * 方法说明：requestReason，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
        private ScanFailureReason requestReason(String category) {
            String c = category == null ? "" : category.trim().toLowerCase(Locale.ROOT);
            if ("timeout".equals(c)) {
                return ScanFailureReason.TIMEOUT;
            }
            if ("no_data".equals(c)) {
                return ScanFailureReason.HTTP_404_NO_DATA;
            }
            if ("parse_error".equals(c)) {
                return ScanFailureReason.PARSE_ERROR;
            }
            if ("rate_limit".equals(c)) {
                return ScanFailureReason.RATE_LIMIT;
            }
            if (c.isEmpty()) {
                return ScanFailureReason.NONE;
            }
            return ScanFailureReason.OTHER;
        }
    }

    private static final class BatchPlan {
        final List<MarketSegment> segments;
        final boolean resumeEnabled;
        final String checkpointKey;
        final String universeSignature;
        final int topN;

        private BatchPlan(List<MarketSegment> segments, boolean resumeEnabled, String checkpointKey, String universeSignature, int topN) {
            this.segments = segments;
            this.resumeEnabled = resumeEnabled;
            this.checkpointKey = checkpointKey;
            this.universeSignature = universeSignature;
            this.topN = topN;
        }
    }

    private static final class BatchState {
        final ScanStats stats;
        int nextSegmentIndex;

        private BatchState(ScanStats stats, int nextSegmentIndex) {
            this.stats = stats;
            this.nextSegmentIndex = nextSegmentIndex;
        }
    }

    private static final class MarketSegment {
        final String segmentKey;
        final List<UniverseRecord> records;

        private MarketSegment(String segmentKey, List<UniverseRecord> records) {
            this.segmentKey = segmentKey;
            this.records = records;
        }
    }

    private static final class BatchCheckpoint {
        final String universeSignature;
        final int segmentCount;
        final int nextSegmentIndex;
        final int scanned;
        final int failed;
        final int candidateCount;
        final int topN;
        final List<ScoredCandidate> topCandidates;

        private BatchCheckpoint(
                String universeSignature,
                int segmentCount,
                int nextSegmentIndex,
                int scanned,
                int failed,
                int candidateCount,
                int topN,
                List<ScoredCandidate> topCandidates
        ) {
            this.universeSignature = universeSignature;
            this.segmentCount = segmentCount;
            this.nextSegmentIndex = nextSegmentIndex;
            this.scanned = scanned;
            this.failed = failed;
            this.candidateCount = candidateCount;
            this.topN = topN;
            this.topCandidates = topCandidates;
        }

        static BatchCheckpoint fromState(BatchPlan plan, BatchState state, int topN) {
            return new BatchCheckpoint(
                    plan.universeSignature,
                    plan.segments.size(),
                    state.nextSegmentIndex,
                    state.stats.scanned,
                    state.stats.failed,
                    state.stats.candidateCount,
                    topN,
                    state.stats.topCandidates()
            );
        }

        JSONObject toJson() {
            JSONObject root = new JSONObject();
            root.put("version", 1);
            root.put("universe_signature", universeSignature);
            root.put("segment_count", segmentCount);
            root.put("next_segment_index", nextSegmentIndex);
            root.put("scanned", scanned);
            root.put("failed", failed);
            root.put("candidate_count", candidateCount);
            root.put("top_n", topN);
            JSONArray arr = new JSONArray();
            for (ScoredCandidate c : topCandidates) {
                JSONObject item = new JSONObject();
                item.put("ticker", c.ticker);
                item.put("code", c.code);
                item.put("name", c.name == null ? "" : c.name);
                item.put("market", c.market == null ? "" : c.market);
                item.put("score", c.score);
                item.put("close", c.close);
                item.put("reasons_json", c.reasonsJson == null ? "" : c.reasonsJson);
                item.put("indicators_json", c.indicatorsJson == null ? "" : c.indicatorsJson);
                arr.put(item);
            }
            root.put("top_candidates", arr);
            return root;
        }

        static BatchCheckpoint fromJson(String raw) {
            JSONObject root = new JSONObject(raw);
            JSONArray arr = root.optJSONArray("top_candidates");
            List<ScoredCandidate> topCandidates = new ArrayList<>();
            if (arr != null) {
                for (int i = 0; i < arr.length(); i++) {
                    JSONObject item = arr.getJSONObject(i);
                    topCandidates.add(new ScoredCandidate(
                            item.optString("ticker", ""),
                            item.optString("code", ""),
                            item.optString("name", ""),
                            item.optString("market", ""),
                            item.optDouble("score", 0.0),
                            item.optDouble("close", 0.0),
                            item.optString("reasons_json", ""),
                            item.optString("indicators_json", "")
                    ));
                }
            }
            return new BatchCheckpoint(
                    root.optString("universe_signature", ""),
                    root.optInt("segment_count", 0),
                    root.optInt("next_segment_index", 0),
                    root.optInt("scanned", 0),
                    root.optInt("failed", 0),
                    root.optInt("candidate_count", 0),
                    root.optInt("top_n", 15),
                    topCandidates
            );
        }
    }
}




