package com.stockbot.jp.runner;

import com.stockbot.core.ModuleResult;
import com.stockbot.core.RunTelemetry;
import com.stockbot.core.diagnostics.CauseCode;
import com.stockbot.core.diagnostics.Diagnostics;
import com.stockbot.core.diagnostics.FeatureStatusResolver;
import com.stockbot.core.diagnostics.Outcome;
import com.stockbot.data.IndustryService;
import com.stockbot.data.MarketDataService;
import com.stockbot.data.NewsService;
import com.stockbot.data.OllamaClient;
import com.stockbot.data.http.HttpClientEx;
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
import com.stockbot.jp.data.TickerNameResolver;
import com.stockbot.jp.indicator.IndicatorEngine;
import com.stockbot.jp.indicator.IndicatorCoverageResult;
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
import com.stockbot.jp.news.NewsItemDao;
import com.stockbot.jp.news.WatchlistNewsPipeline;
import com.stockbot.jp.output.ReportBuilder;
import com.stockbot.jp.strategy.CandidateFilter;
import com.stockbot.jp.strategy.ReasonJsonBuilder;
import com.stockbot.jp.strategy.RiskFilter;
import com.stockbot.jp.strategy.ScoringEngine;
import com.stockbot.jp.universe.JpxUniverseUpdater;
import com.stockbot.jp.vector.EventMemoryService;
import com.stockbot.jp.vector.VectorSearchService;
import com.stockbot.jp.watch.TickerResolver;
import com.stockbot.jp.watch.TickerSpec;
import com.stockbot.utils.TextFormatter;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
            "scan.cache.fresh_days",
            "backtest.hold_days",
            "report.top5.skip_on_partial",
            "report.top5.min_fetch_coverage_pct",
            "report.top5.min_indicator_coverage_pct",
            "report.top5.allow_partial_when_coverage_ge",
            "report.coverage.use_tradable_denominator",
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
            "watchlist.path",
            "watchlist.non_jp_handling",
            "watchlist.default_market_for_alpha",
            "vector.memory.enabled",
            "vector.memory.news.max_items",
            "vector.memory.news.top_k",
            "vector.memory.news.max_cases",
            "vector.memory.signal.top_k",
            "vector.memory.signal.max_cases",
            "fetch.bars.market",
            "fetch.bars.watchlist",
            "fetch.interval.market",
            "fetch.retry.max",
            "fetch.retry.backoff_ms",
            "fetch.concurrent",
            "news.concurrent",
            "news.query.max_variants",
            "news.query.max_results_per_variant",
            "news.vector.query_expand.enabled",
            "news.vector.query_expand.top_k",
            "news.vector.query_expand.max_extra_queries",
            "news.vector.query_expand.rounds",
            "news.vector.query_expand.seed_count",
            "news.source.google_rss",
            "news.source.bing",
            "news.source.yahoo_finance",
            "ai.enabled",
            "ai.watchlist.mode",
            "ai.timeout_sec",
            "ai.max_tokens",
            "ai.temperature",
            "vector.memory.signal.max_cases",
            "indicator.core",
            "indicator.allow_partial",
            "report.run_type.close_time",
            "report.action.buy_score_threshold",
            "report.topcards.max_items",
            "mail.dry_run",
            "mail.fail_fast"
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
    private final GatePolicy gatePolicy;
    private final OllamaClient ollamaClient;
    private final WatchlistNewsPipeline watchlistNewsPipeline;
    private final EventMemoryService eventMemoryService;
    private final RunTelemetry telemetry;
    private final TickerResolver tickerResolver;
    private final TickerNameResolver tickerNameResolver;
    private final NonJpHandling nonJpHandling;
    private final List<String> indicatorCoreFields;
    private final boolean indicatorAllowPartial;
    private final int watchlistMaxAiChars;
    private final int fetchBarsMarket;
    private final int fetchBarsWatchlist;
    private final int fetchRetryMax;
    private final int fetchRetryBackoffMs;
    private final int maxBars;
    private static final DateTimeFormatter NEWS_TS_FMT = DateTimeFormatter.ofPattern("MM-dd HH:mm");
    private static final Set<String> VECTOR_QUERY_STOPWORDS = Set.of(
            "stock", "stocks", "market", "news", "company", "companies",
            "inc", "corp", "co", "ltd", "plc", "the", "and", "or",
            "for", "to", "of", "in", "on", "at", "jp", "us", "cn", "hk",
            "earnings", "revenue", "guidance", "outlook", "forecast",
            "analyst", "rating", "price", "target", "dividend", "buyback",
            "merger", "acquisition", "partnership", "lawsuit", "regulation"
    );
    private static final Set<String> INVALID_TEXT_TOKENS = Set.of(
            "",
            "null",
            "undefined",
            "n/a",
            "na",
            "none",
            "unknown",
            "unkown",
            "-",
            "--"
    );

public DailyRunner(
            Config config,
            UniverseDao universeDao,
            MetadataDao metadataDao,
            BarDailyDao barDailyDao,
            RunDao runDao,
            ScanResultDao scanResultDao
    ) {
        this(config, universeDao, metadataDao, barDailyDao, runDao, scanResultDao, null, null);
    }

public DailyRunner(
            Config config,
            UniverseDao universeDao,
            MetadataDao metadataDao,
            BarDailyDao barDailyDao,
            RunDao runDao,
            ScanResultDao scanResultDao,
            EventMemoryService eventMemoryService
    ) {
        this(config, universeDao, metadataDao, barDailyDao, runDao, scanResultDao, eventMemoryService, null);
    }

public DailyRunner(
            Config config,
            UniverseDao universeDao,
            MetadataDao metadataDao,
            BarDailyDao barDailyDao,
            RunDao runDao,
            ScanResultDao scanResultDao,
            EventMemoryService eventMemoryService,
            RunTelemetry telemetry
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
        this.eventMemoryService = eventMemoryService;
        VectorSearchService vectorSearchService = eventMemoryService == null ? null : eventMemoryService.vectorSearchService();
        String newsLang = config.getString("watchlist.news.lang", "ja");
        String newsRegion = config.getString("watchlist.news.region", "JP");
        int tunedResultsPerVariant = resolveAutoTunedNewsInt(
                "news.query.max_results_per_variant",
                config.getInt("news.query.max_results_per_variant", 12),
                12,
                8
        );
        int newsMaxItems = Math.max(1, config.getInt(
                "watchlist.news.max_items",
                tunedResultsPerVariant
        ));
        String newsSources = buildNewsSourcesConfig();
        int queryVariants = Math.max(1, resolveAutoTunedNewsInt(
                "news.query.max_variants",
                config.getInt("news.query.max_variants", config.getInt("watchlist.news.query_variants", 4)),
                16,
                10
        ));
        boolean vectorQueryExpandEnabled = config.getBoolean("news.vector.query_expand.enabled", true);
        int vectorQueryTopK = Math.max(1, resolveAutoTunedNewsInt(
                "news.vector.query_expand.top_k",
                config.getInt("news.vector.query_expand.top_k", 8),
                12,
                8
        ));
        int vectorQueryMaxExtra = Math.max(0, resolveAutoTunedNewsInt(
                "news.vector.query_expand.max_extra_queries",
                config.getInt("news.vector.query_expand.max_extra_queries", 2),
                4,
                2
        ));
        int vectorQueryRounds = Math.max(1, resolveAutoTunedNewsInt(
                "news.vector.query_expand.rounds",
                config.getInt("news.vector.query_expand.rounds", 2),
                3,
                2
        ));
        int vectorQuerySeedCount = Math.max(1, resolveAutoTunedNewsInt(
                "news.vector.query_expand.seed_count",
                config.getInt("news.vector.query_expand.seed_count", 3),
                4,
                3
        ));
        NewsService.QueryExpansionProvider queryExpansionProvider = buildNewsQueryExpansionProvider(
                vectorSearchService,
                vectorQueryExpandEnabled,
                vectorQueryTopK,
                vectorQueryRounds,
                vectorQuerySeedCount
        );
        this.newsService = new NewsService(
                legacyHttp,
                newsLang,
                newsRegion,
                newsMaxItems,
                newsSources,
                queryVariants,
                queryExpansionProvider,
                vectorQueryMaxExtra
        );
        this.gatePolicy = new GatePolicy(
                config.getDouble("watchlist.ai.score_threshold", -2.0),
                Math.max(1, config.getInt("watchlist.ai.news_min", 8)),
                config.getDouble("watchlist.ai.drop_pct_threshold", -2.0)
        );
        this.ollamaClient = new OllamaClient(
                legacyHttp,
                config.getString("watchlist.ai.base_url", config.getString("ai.base_url", "http://127.0.0.1:11434")),
                config.getString("watchlist.ai.model", "llama3.1:latest"),
                Math.max(5, config.getInt("ai.timeout_sec", config.getInt("watchlist.ai.timeout_sec", 180))),
                Math.max(0, config.getInt("ai.max_tokens", config.getInt("watchlist.ai.max_tokens", 80)))
        );
        this.telemetry = telemetry;
        NewsItemDao newsItemDao = new NewsItemDao(runDao.database());
        this.watchlistNewsPipeline = new WatchlistNewsPipeline(config, legacyHttp, newsItemDao, telemetry);
        this.tickerResolver = new TickerResolver(config.getString("watchlist.default_market_for_alpha", "US"));
        this.tickerNameResolver = new TickerNameResolver(config, legacyHttp);
        this.nonJpHandling = parseNonJpHandling(config.getString("watchlist.non_jp_handling", "PROCESS_SEPARATELY"));
        this.indicatorCoreFields = parseIndicatorCoreFields(config.getString("indicator.core", "sma20,sma60,rsi14,atr14"));
        this.indicatorAllowPartial = config.getBoolean("indicator.allow_partial", true);
        this.watchlistMaxAiChars = Math.max(120, config.getInt("watchlist.ai.max_chars", 900));
        this.fetchBarsMarket = Math.max(120, config.getInt("fetch.bars.market", config.getInt("fetch.bars", 520)));
        this.fetchBarsWatchlist = Math.max(120, config.getInt("fetch.bars.watchlist", config.getInt("fetch.bars", 520)));
        this.fetchRetryMax = Math.max(0, config.getInt("fetch.retry.max", 2));
        this.fetchRetryBackoffMs = Math.max(50, config.getInt("fetch.retry.backoff_ms", 400));
        this.maxBars = Math.max(60, Math.max(config.getInt("yahoo.max_bars_per_ticker", 420), Math.max(fetchBarsMarket, fetchBarsWatchlist)));
    }

public DailyRunOutcome run(boolean forceUniverseUpdate, Integer topNOverride) throws Exception {
        return run(forceUniverseUpdate, topNOverride, false, List.of());
    }

public DailyRunOutcome run(boolean forceUniverseUpdate, Integer topNOverride, boolean resetBatchCheckpoint) throws Exception {
        return run(forceUniverseUpdate, topNOverride, resetBatchCheckpoint, List.of());
    }

public DailyRunOutcome run(
            boolean forceUniverseUpdate,
            Integer topNOverride,
            boolean resetBatchCheckpoint,
            List<String> watchlist
    ) throws Exception {
        Instant startedAt = Instant.now();
        long runId = runDao.startRun(RUN_MODE_DAILY, "JP all-market scanner");
        if (telemetry != null) {
            telemetry.setRunId(runId);
        }
        Diagnostics diagnostics = new Diagnostics(runId, RUN_MODE_DAILY);
        captureConfigSnapshot(diagnostics);
        try {
            telemetryStart(RunTelemetry.STEP_MARKET_FETCH);
            MarketScanSnapshot scan;
            try {
                scan = executeMarketScan(runId, forceUniverseUpdate, topNOverride, resetBatchCheckpoint);
                telemetryEnd(
                        RunTelemetry.STEP_MARKET_FETCH,
                        scan.universeSize,
                        scan.stats.scanned,
                        0,
                        "partial_run=" + scan.partialRun
                );
            } catch (Exception e) {
                telemetryEnd(RunTelemetry.STEP_MARKET_FETCH, 0, 0, 1, e.getClass().getSimpleName());
                throw e;
            }
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
            ReportBuilder.RunType runType = reportBuilder.detectRunTypeByConfig(startedAt, zoneId);
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
            Map<String, ModuleResult> moduleResults = buildModuleResults(
                    watchlistCandidates,
                    scan.marketReferenceCandidates,
                    watchlist == null ? 0 : watchlist.size(),
                    true
            );
            telemetryStart(RunTelemetry.STEP_HTML_RENDER);
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
                    scan.marketReferenceCandidates,
                    config.getDouble("scan.min_score", 55.0),
                    runType,
                    previousScores,
                    scanSummary,
                    scan.partialRun,
                    scan.partialRun ? "PARTIAL" : "SUCCESS",
                    diagnostics,
                    moduleResults,
                    null
            );
            telemetryEnd(RunTelemetry.STEP_HTML_RENDER, watchlistCandidates.size(), 1, 0);

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
            if (telemetry != null) {
                telemetry.incrementErrors(1);
            }
            safeFinishFailed(runId, e);
            throw e;
        }
    }

public DailyRunOutcome runMarketScanOnly(
            boolean forceUniverseUpdate,
            Integer topNOverride,
            boolean resetBatchCheckpoint
    ) throws Exception {
        Instant startedAt = Instant.now();
        long runId = runDao.startRun(RUN_MODE_MARKET_SCAN, "JP all-market background scanner");
        if (telemetry != null) {
            telemetry.setRunId(runId);
        }
        try {
            telemetryStart(RunTelemetry.STEP_MARKET_FETCH);
            MarketScanSnapshot scan;
            try {
                scan = executeMarketScan(runId, forceUniverseUpdate, topNOverride, resetBatchCheckpoint);
                telemetryEnd(
                        RunTelemetry.STEP_MARKET_FETCH,
                        scan.universeSize,
                        scan.stats.scanned,
                        0,
                        "partial_run=" + scan.partialRun
                );
            } catch (Exception e) {
                telemetryEnd(RunTelemetry.STEP_MARKET_FETCH, 0, 0, 1, e.getClass().getSimpleName());
                throw e;
            }
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
            if (telemetry != null) {
                telemetry.incrementErrors(1);
            }
            safeFinishFailed(runId, e);
            throw e;
        }
    }

public DailyRunOutcome runWatchlistReportFromLatestMarket(List<String> watchlist) throws Exception {
        Instant startedAt = Instant.now();
        long runId = runDao.startRun(RUN_MODE_DAILY_REPORT, "watchlist report merged with latest market scan");
        if (telemetry != null) {
            telemetry.setRunId(runId);
        }
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
            ReportBuilder.RunType runType = reportBuilder.detectRunTypeByConfig(startedAt, zoneId);
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
            boolean marketPartial = "PARTIAL".equalsIgnoreCase(safeText(marketRun.status));
            Map<String, ModuleResult> moduleResults = buildModuleResults(
                    watchlistCandidates,
                    marketReferenceCandidates,
                    watchlist == null ? 0 : watchlist.size(),
                    true
            );
            telemetryStart(RunTelemetry.STEP_HTML_RENDER);
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
                    marketReferenceCandidates,
                    config.getDouble("scan.min_score", 55.0),
                    runType,
                    previousScores,
                    scanSummary,
                    marketPartial,
                    safeText(marketRun.status),
                    diagnostics,
                    moduleResults,
                    null
            );
            telemetryEnd(RunTelemetry.STEP_HTML_RENDER, watchlistCandidates.size(), 1, 0);

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
            if (telemetry != null) {
                telemetry.incrementErrors(1);
            }
            safeFinishFailed(runId, e);
            throw e;
        }
    }

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

private List<WatchlistAnalysis> analyzeWatchlist(List<String> watchlist, List<UniverseRecord> universe) {
        List<String> watchItems = sanitizeWatchlist(watchlist);
        if (watchItems.isEmpty()) {
            return List.of();
        }
        boolean aiEnabled = config.getBoolean("ai.enabled", true);
        boolean aiAllMode = "ALL".equalsIgnoreCase(config.getString("ai.watchlist.mode", "AUTO"));
        if (aiEnabled) {
            int targetCount = aiAllMode ? watchItems.size() : watchItems.size();
            System.out.println(String.format(Locale.US, "AI_TARGETS=%d mode=%s", targetCount, aiAllMode ? "ALL" : "GATED"));
        } else {
            System.out.println("AI_TARGETS=0 mode=DISABLED");
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
                telemetryStart(RunTelemetry.STEP_INDICATORS);
                WatchlistScanResult technical = scanWatchRecord(record, watchItem, priceTrace);
                long indicatorIn = priceTrace == null ? 0 : Math.max(0, priceTrace.barsCount);
                long indicatorOut = technical != null && technical.indicatorReady ? 1 : 0;
                long indicatorErr = technical == null ? 1 : (safeText(technical.error).isEmpty() ? 0 : 1);
                telemetryEnd(RunTelemetry.STEP_INDICATORS, indicatorIn, indicatorOut, indicatorErr);
                TickerNameResolver.ResolvedTickerName resolvedName =
                        tickerNameResolver.resolve(tickerSpec.normalized, tickerSpec.market.name());
                String industryEn = normalizeUnknownText(industryService.industryOf(yahooTicker), "-");
                String industryZh = normalizeUnknownText(industryService.industryZhOf(yahooTicker), "-");
                String displayCode = blankTo(resolvedName.displayCode, safeText(record.code).toUpperCase(Locale.ROOT));
                String companyLocal = blankTo(resolvedName.displayNameLocal, resolveCompanyLocalName(record, yahooTicker));
                String displayName = buildDisplayName(displayCode, companyLocal);
                String technicalStatus = toWatchStatus(technical, minScore);
                String rating = mapRatingFromTechnicalStatus(technicalStatus);
                String risk = mapRiskFromTechnicalStatus(technicalStatus);
                double technicalScore = technical == null || technical.candidate == null
                        ? 0.0
                        : safeDouble(technical.candidate.score);
                boolean aiTriggered = applyMappedAiGate(legacy.context, technicalStatus, technicalScore, aiEnabled, aiAllMode);
                EventMemoryService.MemoryInsights memoryInsights = collectMemoryInsights(
                        watchItem,
                        record,
                        industryZh,
                        industryEn,
                        legacy.context.news,
                        technicalStatus,
                        risk,
                        technical == null || technical.candidate == null ? "" : technical.candidate.reasonsJson
                );
                String technicalReasonsJson = enrichWatchReasonJson(
                        technical.candidate.reasonsJson,
                        technical,
                        technicalStatus,
                        minScore,
                        watchItem,
                        tickerSpec,
                        record,
                        yahooTicker,
                        priceTrace,
                        memoryInsights
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
                        displayCode,
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
                        technicalScore,
                        rating,
                        risk,
                        aiTriggered,
                        safeText(legacy.context.gateReason),
                        legacy.context.news.size(),
                        blankTo(legacy.newsSourceLabel, "rss->pgvector"),
                        trimChars(safeText(legacy.context.aiSummary), watchlistMaxAiChars),
                        mergeDigestLines(
                                buildNewsDigestLines(legacy.context.news),
                                legacy.clusterDigestLines,
                                memoryInsights == null ? List.of() : memoryInsights.toDigestLines()
                        ),
                        technicalScore,
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
                    "Watchlist %d/%d item=%s ticker=%s score=%.2f rating=%s risk=%s pct=%.2f%% ai=%s gate=%s ai_text=%s news=%d tech=%.2f tech_status=%s source=%s date=%s bars=%d latency=%dms err=%s",
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
                    row.fetchLatencyMs,
                    trimChars(safeText(row.error), 120)
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

        out.sort(Comparator.comparingDouble((WatchlistAnalysis c) -> c.technicalScore).reversed());
        if (telemetry != null) {
            int triggered = 0;
            for (WatchlistAnalysis row : out) {
                if (row != null && row.aiTriggered) {
                    triggered++;
                }
            }
            if (!aiEnabled) {
                telemetry.setAiUsage(false, "ai.enabled=false");
            } else if (out.isEmpty()) {
                telemetry.setAiUsage(false, "no_watchlist_items");
            } else if (triggered == 0) {
                telemetry.setAiUsage(false, "no_watchlist_item_passed_ai_gate");
            } else {
                telemetry.setAiUsage(true, "triggered_items=" + triggered);
            }
        }
        return out;
    }

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

    static double mapJpScoreToLegacyGateScore(double jpScore) {
        if (!Double.isFinite(jpScore)) {
            return 0.0;
        }
        double mapped = (jpScore - 50.0) / 5.0;
        return Math.max(-10.0, Math.min(10.0, mapped));
    }

    static String mapRatingFromTechnicalStatus(String technicalStatus) {
        String normalized = safeStaticText(technicalStatus).toUpperCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            return "OBSERVE";
        }
        return normalized;
    }

    static String mapRiskFromTechnicalStatus(String technicalStatus) {
        String normalized = safeStaticText(technicalStatus).toUpperCase(Locale.ROOT);
        if ("RISK".equals(normalized)) {
            return "RISK";
        }
        if ("ERROR".equals(normalized)) {
            return "ERROR";
        }
        if ("SKIPPED".equals(normalized)) {
            return "SKIPPED";
        }
        return "NONE";
    }

    private boolean applyMappedAiGate(
            StockContext context,
            String technicalStatus,
            double technicalScore,
            boolean aiEnabled,
            boolean aiAllMode
    ) {
        if (context == null) {
            return false;
        }
        if (!aiEnabled) {
            context.aiRan = false;
            if (safeText(context.gateReason).isEmpty()) {
                context.gateReason = "ai_disabled";
            }
            return false;
        }

        String normalizedStatus = safeText(technicalStatus).toUpperCase(Locale.ROOT);
        if ("ERROR".equals(normalizedStatus) || "SKIPPED".equals(normalizedStatus)) {
            context.totalScore = null;
        } else {
            context.totalScore = mapJpScoreToLegacyGateScore(technicalScore);
        }

        boolean triggered;
        if (aiAllMode) {
            context.gateReason = "all_mode";
            triggered = true;
        } else {
            triggered = gatePolicy.shouldRunAi(context);
        }
        context.aiRan = triggered;
        return triggered;
    }

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
        reasonRoot.put("score_passed", false);
        reasonRoot.put("filter_reasons", new JSONArray());
        reasonRoot.put("risk_flags", new JSONArray());
        reasonRoot.put("risk_reasons", new JSONArray());
        reasonRoot.put("score_reasons", new JSONArray());
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
        String normalizedCode = code.isEmpty() ? safeText(watchItem).toUpperCase(Locale.ROOT) : code;
        String display = buildDisplayName(normalizedCode, normalizedCode);

        return new WatchlistAnalysis(
                watchItem,
                code,
                safeText(tickerSpec == null ? "" : tickerSpec.normalized),
                display,
                normalizedCode,
                "-",
                "-",
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

            int freshDays = autoWatchFreshDays();
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
            IndicatorCoverageResult coverage = evaluateIndicatorCoverage(ind);
            if (!coverage.coreReady()) {
                return failedWatchRecord(
                        record,
                        watchItem,
                        "missing_core_indicators:" + String.join(",", coverage.missingCoreIndicators),
                        CauseCode.INDICATOR_ERROR,
                        Map.of(
                                "missing_core_indicators", coverage.missingCoreIndicators,
                                "missing_optional_indicators", coverage.missingOptionalIndicators
                        ),
                        true,
                        false
                );
            }
            if (!indicatorAllowPartial && !coverage.missingOptionalIndicators.isEmpty()) {
                return failedWatchRecord(
                        record,
                        watchItem,
                        "missing_optional_indicators:" + String.join(",", coverage.missingOptionalIndicators),
                        CauseCode.INDICATOR_ERROR,
                        Map.of(
                                "missing_core_indicators", coverage.missingCoreIndicators,
                                "missing_optional_indicators", coverage.missingOptionalIndicators
                        ),
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
                        Map.of(
                                "risk_flags", risk.flags,
                                "risk_penalty", risk.penalty,
                                "risk_reasons", risk.reasons
                        )
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

private PriceFetchTrace fetchWatchPriceTrace(String jpTicker, String yahooTicker) {
        long started = System.nanoTime();

        YahooFetchResult yahoo = fetchBarsFromYahoo(jpTicker, yahooTicker, fetchBarsWatchlist, "watchlist");
        List<BarDaily> fromYahoo = yahoo.bars;
        if (!fromYahoo.isEmpty()) {
            return PriceFetchTrace.fromBars(
                    fromYahoo,
                    "yahoo",
                    false,
                    nanosToMillis(System.nanoTime() - started),
                    yahoo.requestFailed,
                    yahoo.requestFailureCategory,
                    yahoo.error,
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
                    safeText(yahoo.requestFailureCategory).isEmpty() ? "no_data" : yahoo.requestFailureCategory,
                    safeText(yahoo.error).isEmpty() ? "fetch_failed" : yahoo.error,
                    "yahoo->cache"
            );
        }

        return PriceFetchTrace.failed(
                "fetch_failed",
                nanosToMillis(System.nanoTime() - started),
                true,
                safeText(yahoo.requestFailureCategory).isEmpty() ? "no_data" : yahoo.requestFailureCategory,
                safeText(yahoo.error).isEmpty() ? "fetch_failed" : yahoo.error,
                "yahoo->cache->failed"
        );
    }

private void logPriceTrace(String ticker, PriceFetchTrace trace) {
        if (trace == null) {
            System.out.println(String.format(Locale.US,
                    "[PRICE] ticker=%s source=fetch_failed tradeDate= lastClose=NaN bars=0 latency=0ms cache_hit=false reason=unknown",
                    safeText(ticker)));
            return;
        }
        System.out.println(String.format(
                Locale.US,
                "[PRICE] ticker=%s source=%s tradeDate=%s lastClose=%.4f bars=%d latency=%dms cache_hit=%s fallback=%s req_cat=%s req_err=%s",
                safeText(ticker),
                safeText(trace.dataSource),
                safeText(trace.priceTimestamp),
                trace.lastClose,
                trace.barsCount,
                trace.fetchLatencyMs,
                trace.cacheHit,
                safeText(trace.fallbackPath),
                safeText(trace.requestFailureCategory),
                safeText(trace.error)
        ));
    }

private YahooFetchResult fetchBarsFromYahoo(String jpTicker, String yahooTicker, int targetBars, String fetchScope) {
        if (yahooTicker == null || yahooTicker.trim().isEmpty()) {
            return YahooFetchResult.empty("no_data", "empty_symbol");
        }
        String normalizedYahooTicker = normalizeYahooTickerSymbol(yahooTicker);
        int maxRetry = Math.max(0, fetchRetryMax);
        int desiredBars = Math.max(120, targetBars);
        String[] ranges = desiredBars >= 500 ? new String[] {"5y", "max"} : new String[] {"2y", "5y"};
        String requestFailureCategory = "";
        String requestError = "";

        for (String range : ranges) {
            for (int attempt = 0; attempt <= maxRetry; attempt++) {
                try {
                    String interval = resolveFetchInterval(fetchScope);
                    List<MarketDataService.DailyBar> history = marketDataService.fetchDailyHistoryBars(normalizedYahooTicker, range, interval);
                    List<BarDaily> bars = toBarsFromYahoo(jpTicker, history);
                    if (bars.isEmpty()) {
                        requestFailureCategory = "no_data";
                        requestError = "no_data";
                    } else {
                        if (bars.size() > desiredBars) {
                            bars = new ArrayList<>(bars.subList(bars.size() - desiredBars, bars.size()));
                        }
                        return new YahooFetchResult(bars, false, "", "");
                    }
                } catch (Exception e) {
                    String rawCategory = normalizeRequestFailureCategory("", e.getMessage());
                    requestFailureCategory = safeText(rawCategory).isEmpty() ? "fetch_failed" : rawCategory;
                    requestError = safeText(e.getMessage());
                    boolean canRetry = attempt < maxRetry
                            && ("timeout".equals(requestFailureCategory)
                            || "rate_limit".equals(requestFailureCategory)
                            || "other".equals(requestFailureCategory)
                            || "fetch_failed".equals(requestFailureCategory));
                    if (!canRetry) {
                        break;
                    }
                    long waitMs = (long) fetchRetryBackoffMs * (1L << attempt);
                    try {
                        Thread.sleep(Math.max(50L, waitMs));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return YahooFetchResult.empty(requestFailureCategory, "interrupted");
                    }
                }
            }
        }
        if (requestFailureCategory.isEmpty()) {
            requestFailureCategory = "no_data";
        }
        if (requestError.isEmpty()) {
            requestError = "fetch_failed:" + safeText(fetchScope);
        }
        return YahooFetchResult.empty(requestFailureCategory, requestError);
    }

    private String resolveFetchInterval(String fetchScope) {
        if (!"market".equalsIgnoreCase(safeText(fetchScope))) {
            return "1d";
        }
        String configured = safeText(config.getString("fetch.interval.market", "1d"));
        return configured.isEmpty() ? "1d" : configured;
    }

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
        reasonRoot.put("score_passed", false);
        reasonRoot.put("filter_reasons", new JSONArray());
        reasonRoot.put("risk_flags", new JSONArray());
        reasonRoot.put("risk_reasons", new JSONArray());
        reasonRoot.put("score_reasons", new JSONArray());
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

private String appendWatchMeta(String reasonsJson, String watchItem) {
        JSONObject root = new JSONObject(reasonsJson == null ? "{}" : reasonsJson);
        root.put("watch_item", watchItem == null ? "" : watchItem);
        return root.toString();
    }

private String enrichWatchReasonJson(
            String rawReasonsJson,
            WatchlistScanResult technical,
            String technicalStatus,
            double minScore,
            String watchItem,
            TickerSpec tickerSpec,
            UniverseRecord record,
            String yahooTicker,
            PriceFetchTrace priceTrace,
            EventMemoryService.MemoryInsights memoryInsights
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
        if (memoryInsights != null) {
            root.put("memory", memoryInsights.toJson());
        }
        return root.toString();
    }

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

    private List<String> parseIndicatorCoreFields(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return List.of("sma20", "sma60", "rsi14", "atr14");
        }
        Set<String> out = new TreeSet<>();
        String[] tokens = raw.split("[,;]");
        for (String token : tokens) {
            String t = safeText(token).toLowerCase(Locale.ROOT);
            if (!t.isEmpty()) {
                out.add(t);
            }
        }
        if (out.isEmpty()) {
            return List.of("sma20", "sma60", "rsi14", "atr14");
        }
        return new ArrayList<>(out);
    }

    private IndicatorCoverageResult evaluateIndicatorCoverage(IndicatorSnapshot ind) {
        if (ind == null) {
            return new IndicatorCoverageResult(List.of("indicator_snapshot"), List.of());
        }
        List<String> missingCore = new ArrayList<>();
        List<String> missingOptional = new ArrayList<>();

        for (String core : indicatorCoreFields) {
            if (!isIndicatorPresent(ind, core)) {
                missingCore.add(core);
            }
        }

        String[] optional = new String[] {
                "sma120", "drawdown120_pct", "volatility20_pct", "volume_ratio20", "return3d_pct", "return5d_pct", "return10d_pct"
        };
        for (String item : optional) {
            if (!isIndicatorPresent(ind, item)) {
                missingOptional.add(item);
            }
        }
        return new IndicatorCoverageResult(missingCore, missingOptional);
    }

    private boolean isIndicatorPresent(IndicatorSnapshot ind, String indicatorName) {
        if (ind == null) {
            return false;
        }
        String name = safeText(indicatorName).toLowerCase(Locale.ROOT);
        switch (name) {
            case "last_close":
                return Double.isFinite(ind.lastClose) && ind.lastClose > 0.0;
            case "sma20":
                return Double.isFinite(ind.sma20) && ind.sma20 > 0.0;
            case "sma60":
                return Double.isFinite(ind.sma60) && ind.sma60 > 0.0;
            case "sma120":
                return Double.isFinite(ind.sma120) && ind.sma120 > 0.0;
            case "rsi14":
                return Double.isFinite(ind.rsi14) && ind.rsi14 >= 0.0 && ind.rsi14 <= 100.0;
            case "atr14":
                return Double.isFinite(ind.atr14) && ind.atr14 >= 0.0;
            case "atr_pct":
                return Double.isFinite(ind.atrPct) && ind.atrPct >= 0.0;
            case "avg_volume20":
                return Double.isFinite(ind.avgVolume20) && ind.avgVolume20 >= 0.0;
            case "volume_ratio20":
                return Double.isFinite(ind.volumeRatio20) && ind.volumeRatio20 >= 0.0;
            case "drawdown120_pct":
                return Double.isFinite(ind.drawdown120Pct);
            case "volatility20_pct":
                return Double.isFinite(ind.volatility20Pct) && ind.volatility20Pct >= 0.0;
            case "return3d_pct":
                return Double.isFinite(ind.return3dPct);
            case "return5d_pct":
                return Double.isFinite(ind.return5dPct);
            case "return10d_pct":
                return Double.isFinite(ind.return10dPct);
            default:
                return true;
        }
    }

private LegacyWatchResult buildLegacyWatchResult(
            UniverseRecord record,
            String watchItem,
            String yahooTicker,
            PriceFetchTrace priceTrace
    ) {
        StockContext sc = new StockContext(yahooTicker);
        String error = "";
        String newsSourceLabel = "";
        List<String> clusterDigestLines = List.of();
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

            List<String> baseQueries = buildNewsQueries(record, watchItem, yahooTicker);
            List<String> effectiveQueries = baseQueries;
            int droppedInvalid = 0;
            boolean vectorQueryExpandEnabled = config.getBoolean("news.vector.query_expand.enabled", true);
            int vectorQueryTopK = Math.max(1, resolveAutoTunedNewsInt(
                    "news.vector.query_expand.top_k",
                    config.getInt("news.vector.query_expand.top_k", 8),
                    12,
                    8
            ));
            int vectorQueryMaxExtra = Math.max(0, resolveAutoTunedNewsInt(
                    "news.vector.query_expand.max_extra_queries",
                    config.getInt("news.vector.query_expand.max_extra_queries", 2),
                    4,
                    2
            ));
            int vectorQueryRounds = Math.max(1, resolveAutoTunedNewsInt(
                    "news.vector.query_expand.rounds",
                    config.getInt("news.vector.query_expand.rounds", 2),
                    3,
                    2
            ));
            int vectorQuerySeedCount = Math.max(1, resolveAutoTunedNewsInt(
                    "news.vector.query_expand.seed_count",
                    config.getInt("news.vector.query_expand.seed_count", 3),
                    4,
                    3
            ));
            VectorSearchService vectorSearchService = eventMemoryService == null ? null : eventMemoryService.vectorSearchService();
            if (vectorQueryExpandEnabled && vectorSearchService != null && vectorQueryMaxExtra > 0) {
                effectiveQueries = expandNewsQueriesByVector(
                        vectorSearchService,
                        yahooTicker,
                        baseQueries,
                        vectorQueryTopK,
                        vectorQueryMaxExtra,
                        vectorQueryRounds,
                        vectorQuerySeedCount
                );
            }
            List<String> cleanedQueries = sanitizeNewsQueryList(effectiveQueries);
            droppedInvalid = Math.max(0, (effectiveQueries == null ? 0 : effectiveQueries.size()) - cleanedQueries.size());
            effectiveQueries = cleanedQueries;
            logNewsQueryPlan(yahooTicker, baseQueries, effectiveQueries, droppedInvalid);
            String companyName = resolveCompanyLocalName(record, yahooTicker);
            String industryEn = normalizeUnknownText(industryService.industryOf(yahooTicker), "");
            String industryZh = normalizeUnknownText(industryService.industryZhOf(yahooTicker), "");
            WatchlistNewsPipeline.PipelineResult newsResult = watchlistNewsPipeline.processTicker(
                    yahooTicker,
                    companyName,
                    industryZh,
                    industryEn,
                    effectiveQueries,
                    config.getString("watchlist.news.lang", "ja"),
                    config.getString("watchlist.news.region", "JP")
            );
            sc.news.addAll(newsResult.newsItems);
            newsSourceLabel = safeText(newsResult.sourceLabel);
            clusterDigestLines = newsResult.digestLines == null ? List.of() : newsResult.digestLines;
            boolean hasRelevantNews = !newsResult.newsItems.isEmpty();

            String summaryText = TextFormatter.toPlainText(safeText(newsResult.summaryHtml));
            if (summaryText.isBlank()) {
                summaryText = hasRelevantNews
                        ? "聚类摘要暂不可用。"
                        : "回溯窗口内未发现显著事件聚类。";
            }
            sc.aiSummary = hasRelevantNews
                    ? summaryText
                    : "回溯窗口内未发现显著事件聚类。";
        } catch (Exception e) {
            String msg = safeText(e.getMessage());
            if (msg.isEmpty()) {
                msg = "legacy_failed";
            }
            error = e.getClass().getSimpleName() + ": " + msg;
            sc.gateReason = "legacy_error";
            sc.aiSummary = "AI 已跳过：新闻流水线失败（" + error + "）";
            System.err.println(String.format(
                    Locale.US,
                    "WARN: legacy watch pipeline failed watch_item=%s ticker=%s reason=%s",
                    safeText(watchItem),
                    safeText(yahooTicker),
                    error
            ));
        }
        return new LegacyWatchResult(sc, error, newsSourceLabel, clusterDigestLines);
    }

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
        LinkedHashSet<String> queries = new LinkedHashSet<>();
        String recordName = normalizeNewsQueryToken(record == null ? "" : record.name);
        String recordCode = normalizeNewsQueryToken(record == null ? "" : record.code);
        String watchToken = normalizeNewsQueryToken(watchItem);
        String ticker = normalizeNewsQueryToken(yahooTicker);

        addNewsQueryIfPresent(queries, recordName);
        if (!recordCode.isEmpty()) {
            addNewsQueryIfPresent(queries, recordCode);
            if (recordCode.matches("\\d{4,6}")) {
                addNewsQueryIfPresent(queries, recordCode + ".T");
            }
        }
        addNewsQueryIfPresent(queries, ticker);
        addNewsQueryIfPresent(queries, watchToken);

        String company = normalizeNewsQueryToken(industryService.companyNameOf(yahooTicker));
        addNewsQueryIfPresent(queries, company);
        String industry = normalizeNewsQueryToken(industryService.industryOf(yahooTicker));
        if (!industry.isEmpty()) {
            String prefix = company.isEmpty() ? (recordName.isEmpty() ? recordCode : recordName) : company;
            if (!prefix.isEmpty()) {
                addNewsQueryIfPresent(queries, prefix + " " + industry);
            }
            addNewsQueryIfPresent(queries, industry);
        }

        List<String> topics = parseTopicCsv(config.getString(
                "watchlist.news.query_topics",
                "株価,決算,業績,見通し,受注,設備投資,提携,規制,為替,金利,guidance,earnings,outlook,supply chain"
        ));
        String anchor = company.isEmpty() ? (recordName.isEmpty() ? watchToken : recordName) : company;
        if (anchor.isEmpty()) {
            anchor = ticker.isEmpty() ? recordCode : ticker;
        }
        for (String topic : topics) {
            if (anchor.isEmpty()) {
                break;
            }
            addNewsQueryIfPresent(queries, anchor + " " + topic);
            if (!industry.isEmpty()) {
                addNewsQueryIfPresent(queries, industry + " " + topic);
            }
        }

        return sanitizeNewsQueryList(new ArrayList<>(queries));
    }

    private NewsService.QueryExpansionProvider buildNewsQueryExpansionProvider(
            VectorSearchService vectorSearchService,
            boolean enabled,
            int topK,
            int rounds,
            int seedCount
    ) {
        if (!enabled || vectorSearchService == null) {
            return null;
        }
        final int safeTopK = Math.max(1, topK);
        final int safeRounds = Math.max(1, rounds);
        final int safeSeedCount = Math.max(1, seedCount);
        return (ticker, baseQueries, maxExtraQueries) ->
                expandNewsQueriesByVector(
                        vectorSearchService,
                        ticker,
                        baseQueries,
                        safeTopK,
                        maxExtraQueries,
                        safeRounds,
                        safeSeedCount
                );
    }

    private List<String> expandNewsQueriesByVector(
            VectorSearchService vectorSearchService,
            String ticker,
            List<String> baseQueries,
            int topK,
            int maxExtraQueries,
            int rounds,
            int seedCount
    ) {
        if (vectorSearchService == null || baseQueries == null || baseQueries.isEmpty()) {
            return baseQueries == null ? List.of() : baseQueries;
        }

        LinkedHashSet<String> baseSet = new LinkedHashSet<>();
        for (String query : baseQueries) {
            String normalized = normalizeVectorQuery(query);
            if (!normalized.isEmpty()) {
                baseSet.add(normalized);
            }
        }
        if (baseSet.isEmpty()) {
            return List.of();
        }
        List<String> normalizedBase = new ArrayList<>(baseSet);

        int extraCap = Math.max(0, maxExtraQueries);
        if (extraCap <= 0) {
            return normalizedBase;
        }

        LinkedHashMap<String, Double> queryScores = new LinkedHashMap<>();
        for (int i = 0; i < normalizedBase.size(); i++) {
            queryScores.put(normalizedBase.get(i), 120.0 - (i * 4.0));
        }

        String tickerFilter = normalizeVectorTickerForFilter(ticker);
        int safeRounds = Math.max(1, rounds);
        int safeSeedCount = Math.max(1, seedCount);
        int addedTotal = 0;
        List<String> workingQueries = new ArrayList<>(normalizedBase);

        for (int round = 1; round <= safeRounds; round++) {
            int remainingExtra = extraCap - addedTotal;
            if (remainingExtra <= 0) {
                break;
            }

            List<String> seeds = new ArrayList<>();
            for (int i = 0; i < workingQueries.size() && seeds.size() < safeSeedCount; i++) {
                String seed = normalizeVectorQuery(workingQueries.get(i));
                if (!seed.isEmpty()) {
                    seeds.add(seed);
                }
            }
            if (seeds.isEmpty()) {
                break;
            }

            LinkedHashMap<String, Double> hintScores = new LinkedHashMap<>();
            for (String seed : seeds) {
                List<VectorSearchService.DocMatch> matches = searchVectorNews(vectorSearchService, seed, topK, tickerFilter);
                if (matches.isEmpty() && !tickerFilter.isEmpty()) {
                    matches = searchVectorNews(vectorSearchService, seed, topK, "");
                }
                int rank = 0;
                for (VectorSearchService.DocMatch match : matches) {
                    double weight = 1.0 / (1.0 + rank);
                    collectVectorHints(hintScores, match, weight);
                    rank++;
                }
            }

            if (hintScores.isEmpty()) {
                System.out.println(String.format(
                        Locale.US,
                        "[NEWS_QUERY_ROUND] ticker=%s round=%d seeds=%s hints=0 added=0 added_queries=[] added_terms=[]",
                        safeText(ticker),
                        round,
                        formatVectorQueryList(seeds, 4)
                ));
                break;
            }

            for (String query : new ArrayList<>(queryScores.keySet())) {
                String normalizedQuery = normalizeVectorText(query);
                double boost = 0.0;
                for (Map.Entry<String, Double> entry : hintScores.entrySet()) {
                    String hint = entry.getKey();
                    if (hint.isEmpty()) {
                        continue;
                    }
                    if (normalizedQuery.contains(hint) || hint.contains(normalizedQuery)) {
                        boost += entry.getValue() * 0.35;
                    }
                }
                if (boost > 0.0) {
                    queryScores.put(query, queryScores.getOrDefault(query, 0.0) + Math.min(18.0, boost));
                }
            }

            List<Map.Entry<String, Double>> orderedHints = new ArrayList<>(hintScores.entrySet());
            orderedHints.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

            String anchor = chooseVectorAnchor(ticker, workingQueries);
            int addedThisRound = 0;
            List<String> addedQueriesThisRound = new ArrayList<>();
            LinkedHashSet<String> addedTermsThisRound = new LinkedHashSet<>();
            for (Map.Entry<String, Double> entry : orderedHints) {
                if (addedThisRound >= remainingExtra) {
                    break;
                }
                String expanded = composeVectorExpandedQuery(anchor, entry.getKey());
                if (expanded.isEmpty() || queryScores.containsKey(expanded)) {
                    continue;
                }
                double baseScore = 85.0 + Math.min(25.0, entry.getValue());
                double roundDecay = Math.max(0.0, (round - 1) * 3.0);
                queryScores.put(expanded, baseScore - roundDecay);
                addedThisRound++;
                addedTotal++;
                addedQueriesThisRound.add(expanded);
                String addedTerm = normalizeNewsQueryToken(entry.getKey());
                if (!addedTerm.isEmpty()) {
                    addedTermsThisRound.add(addedTerm);
                }
            }

            int workingLimit = Math.max(
                    normalizedBase.size(),
                    Math.min(queryScores.size(), normalizedBase.size() + addedTotal)
            );
            workingQueries = rankVectorQueries(queryScores, workingLimit);
            System.out.println(String.format(
                    Locale.US,
                    "[NEWS_QUERY_ROUND] ticker=%s round=%d seeds=%s hints=%d added=%d total_added=%d",
                    safeText(ticker),
                    round,
                    formatVectorQueryList(seeds, 4),
                    hintScores.size(),
                    addedThisRound,
                    addedTotal
            ));
            System.out.println(String.format(
                    Locale.US,
                    "[NEWS_QUERY_ROUND] ticker=%s round=%d added_queries=%s added_terms=%s",
                    safeText(ticker),
                    round,
                    formatVectorQueryList(addedQueriesThisRound, 6),
                    formatVectorQueryList(new ArrayList<>(addedTermsThisRound), 8)
            ));
            if (addedThisRound <= 0) {
                break;
            }
        }

        int maxQueries = Math.max(
                normalizedBase.size(),
                Math.min(queryScores.size(), normalizedBase.size() + extraCap)
        );
        List<String> out = rankVectorQueries(queryScores, maxQueries);
        return out.isEmpty() ? normalizedBase : out;
    }

    private List<String> rankVectorQueries(Map<String, Double> queryScores, int maxQueries) {
        if (queryScores == null || queryScores.isEmpty()) {
            return List.of();
        }
        List<Map.Entry<String, Double>> rankedQueries = new ArrayList<>(queryScores.entrySet());
        rankedQueries.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));
        List<String> out = new ArrayList<>();
        for (Map.Entry<String, Double> entry : rankedQueries) {
            String normalized = normalizeVectorQuery(entry.getKey());
            if (normalized.isEmpty()) {
                continue;
            }
            out.add(normalized);
            if (out.size() >= Math.max(1, maxQueries)) {
                break;
            }
        }
        return out;
    }

    private List<VectorSearchService.DocMatch> searchVectorNews(
            VectorSearchService vectorSearchService,
            String seed,
            int topK,
            String tickerFilter
    ) {
        if (vectorSearchService == null) {
            return List.of();
        }
        String keyword = normalizeVectorQuery(seed);
        if (keyword.isEmpty()) {
            return List.of();
        }
        try {
            return vectorSearchService.searchSimilar(
                    keyword,
                    Math.max(1, topK),
                    new VectorSearchService.SearchFilters(
                            "NEWS",
                            tickerFilter == null || tickerFilter.isEmpty() ? null : tickerFilter,
                            null
                    )
            );
        } catch (Exception ignored) {
            return List.of();
        }
    }

    private void collectVectorHints(Map<String, Double> hintScores, VectorSearchService.DocMatch match, double weight) {
        if (match == null || hintScores == null) {
            return;
        }
        double w = Math.max(0.1, weight);
        addVectorHint(hintScores, match.ticker, 3.0 * w);
        addVectorHint(hintScores, match.title, 2.4 * w);

        JSONObject payload = safeJsonObject(match.content);
        addVectorHint(hintScores, payload.optString("ticker", ""), 3.0 * w);
        addVectorHint(hintScores, payload.optString("industry_en", ""), 2.2 * w);
        addVectorHint(hintScores, payload.optString("industry_zh", ""), 2.2 * w);
        addVectorHint(hintScores, payload.optString("summary", ""), 1.6 * w);
        addVectorHint(hintScores, payload.optString("title", ""), 1.8 * w);

        JSONArray industries = payload.optJSONArray("beneficiary_industries");
        if (industries != null) {
            for (int i = 0; i < industries.length(); i++) {
                addVectorHint(hintScores, industries.optString(i, ""), 1.9 * w);
            }
        }

        JSONArray impactTickers = payload.optJSONArray("impact_tickers");
        if (impactTickers != null) {
            for (int i = 0; i < impactTickers.length(); i++) {
                addVectorHint(hintScores, impactTickers.optString(i, ""), 2.6 * w);
            }
        }
    }

    private void addVectorHint(Map<String, Double> hintScores, String raw, double weight) {
        if (hintScores == null || raw == null || raw.trim().isEmpty() || weight <= 0.0) {
            return;
        }

        String[] tokens = raw.split("[^\\p{L}\\p{N}.]+");
        for (String token : tokens) {
            String normalized = normalizeVectorText(token);
            if (normalized.length() < 2 || normalized.length() > 32) {
                continue;
            }
            if (INVALID_TEXT_TOKENS.contains(normalized)) {
                continue;
            }
            if (normalized.matches("\\d{1,2}")) {
                continue;
            }
            if (normalized.startsWith("http")) {
                continue;
            }
            if (VECTOR_QUERY_STOPWORDS.contains(normalized)) {
                continue;
            }
            hintScores.put(normalized, hintScores.getOrDefault(normalized, 0.0) + weight);
        }
    }

    private String normalizeVectorText(String text) {
        if (text == null) {
            return "";
        }
        return text.toLowerCase(Locale.ROOT)
                .replace('\u3000', ' ')
                .replaceAll("\\s+", " ")
                .trim();
    }

    private String normalizeVectorQuery(String text) {
        return normalizeNewsQueryToken(text);
    }

    private String formatVectorQueryList(List<String> queries, int limit) {
        if (queries == null || queries.isEmpty()) {
            return "[]";
        }
        List<String> out = new ArrayList<>();
        int max = Math.max(1, limit);
        for (String query : queries) {
            String normalized = normalizeVectorQuery(query);
            if (normalized.isEmpty()) {
                continue;
            }
            out.add(normalized.length() > 48 ? (normalized.substring(0, 45) + "...") : normalized);
            if (out.size() >= max) {
                break;
            }
        }
        if (out.isEmpty()) {
            return "[]";
        }
        return "[" + String.join(" | ", out) + "]";
    }

    private String normalizeVectorTickerForFilter(String ticker) {
        String normalized = safeText(ticker).toUpperCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            return "";
        }
        if (normalized.endsWith(".JP")) {
            return normalized.substring(0, normalized.length() - 3) + ".T";
        }
        return normalized;
    }

    private String chooseVectorAnchor(String ticker, List<String> baseQueries) {
        String t = normalizeVectorQuery(ticker);
        if (!t.isEmpty()) {
            return t;
        }
        if (baseQueries == null || baseQueries.isEmpty()) {
            return "";
        }
        return normalizeVectorQuery(baseQueries.get(0));
    }

    private String composeVectorExpandedQuery(String anchor, String hint) {
        String h = normalizeVectorQuery(hint);
        if (h.isEmpty()) {
            return "";
        }
        String a = normalizeVectorQuery(anchor);
        if (a.isEmpty()) {
            return h;
        }
        String aLower = a.toLowerCase(Locale.ROOT);
        String hLower = h.toLowerCase(Locale.ROOT);
        if (aLower.contains(hLower)) {
            return a;
        }
        if (hLower.contains(aLower)) {
            return h;
        }
        return (a + " " + h).trim();
    }

    private String toYahooTicker(TickerSpec tickerSpec, UniverseRecord record) {
        String normalized = safeText(tickerSpec == null ? "" : tickerSpec.normalized).toUpperCase(Locale.ROOT);
        if (normalized.endsWith(".T")) {
            return normalized;
        }
        if (normalized.endsWith(".JP")) {
            return normalized.substring(0, normalized.length() - 3) + ".T";
        }
        if (normalized.endsWith(".US")) {
            return normalized.substring(0, normalized.length() - 3);
        }
        if (normalized.endsWith(".NQ") || normalized.endsWith(".N")) {
            int cut = normalized.lastIndexOf('.');
            if (cut > 0) {
                return normalized.substring(0, cut);
            }
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
        if (ticker.endsWith(".us")) {
            return ticker.substring(0, ticker.length() - 3).toUpperCase(Locale.ROOT);
        }
        if (ticker.endsWith(".nq") || ticker.endsWith(".n")) {
            int cut = ticker.lastIndexOf('.');
            if (cut > 0) {
                return ticker.substring(0, cut).toUpperCase(Locale.ROOT);
            }
        }
        return ticker.toUpperCase(Locale.ROOT);
    }

    private String normalizeYahooTickerSymbol(String symbol) {
        String token = safeText(symbol).toUpperCase(Locale.ROOT);
        if (token.endsWith(".US")) {
            return token.substring(0, token.length() - 3);
        }
        if (token.endsWith(".NQ") || token.endsWith(".N")) {
            int cut = token.lastIndexOf('.');
            if (cut > 0) {
                return token.substring(0, cut);
            }
        }
        return token;
    }

    private String resolveCompanyLocalName(UniverseRecord record, String yahooTicker) {
        String fromIndustry = normalizeUnknownText(industryService.companyNameOf(yahooTicker), "");
        if (!fromIndustry.isEmpty() && !fromIndustry.equalsIgnoreCase(yahooTicker)) {
            return fromIndustry;
        }
        if (record.name != null && !record.name.trim().isEmpty()) {
            String name = record.name.trim();
            if (!name.equalsIgnoreCase(record.ticker)) {
                return name;
            }
        }
        if (record.code != null && !record.code.trim().isEmpty()) {
            return record.code.trim();
        }
        String normalized = normalizeYahooTickerSymbol(yahooTicker);
        if (!normalized.isEmpty()) {
            return normalized;
        }
        return record.ticker == null ? "" : record.ticker;
    }

private String buildDisplayName(String code, String localName) {
        String c = safeText(code).toUpperCase(Locale.ROOT);
        String n = safeText(localName);
        if (n.isEmpty()) {
            n = c;
        }
        if (c.isEmpty()) {
            return n;
        }
        return c + " " + n;
    }

    private String normalizeUnknownText(String value, String fallback) {
        String text = safeText(value);
        if (text.isEmpty()) {
            return fallback;
        }
        String lower = text.toLowerCase(Locale.ROOT);
        if ("unknown".equals(lower)
                || "unkown".equals(lower)
                || "n/a".equals(lower)
                || "na".equals(lower)
                || "none".equals(lower)
                || "null".equals(lower)
                || "undefined".equals(lower)
                || "-".equals(lower)
                || "--".equals(lower)
                || lower.startsWith("ai unavailable:")
                || lower.startsWith("ai skipped:")) {
            return fallback;
        }
        return text;
    }

private double computePctChange(Double last, Double prev) {
        if (last == null || prev == null || !Double.isFinite(last) || !Double.isFinite(prev) || prev == 0.0) {
            return 0.0;
        }
        return (last - prev) / prev * 100.0;
    }

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

private String trimChars(String text, int maxChars) {
        String t = safeText(text);
        if (t.length() <= maxChars) {
            return t;
        }
        return t.substring(0, Math.max(0, maxChars - 3)) + "...";
    }

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

private Map<ScanFailureReason, Integer> zeroFailureMap() {
        Map<ScanFailureReason, Integer> out = new EnumMap<>(ScanFailureReason.class);
        for (ScanFailureReason reason : ScanFailureReason.values()) {
            out.put(reason, 0);
        }
        return out;
    }

private Map<DataInsufficientReason, Integer> zeroInsufficientMap() {
        Map<DataInsufficientReason, Integer> out = new EnumMap<>(DataInsufficientReason.class);
        for (DataInsufficientReason reason : DataInsufficientReason.values()) {
            out.put(reason, 0);
        }
        return out;
    }

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

private void captureConfigSnapshot(Diagnostics diagnostics) {
        for (String key : DIAGNOSTIC_CONFIG_KEYS) {
            Config.ResolvedValue resolved = config.resolve(key);
            diagnostics.addConfig(resolved.key, resolved.value, resolved.source);
        }
    }

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
        int marketIndicatorRaw = summary == null ? 0 : Math.max(0, summary.indicatorCoverage);
        int tradableDenominator = summary == null ? 0 : Math.max(0, summary.tradableDenominator);
        int tradableIndicator = summary == null ? 0 : Math.max(0, summary.tradableIndicatorCoverage);
        if (tradableDenominator <= 0) {
            tradableDenominator = marketTotal;
            tradableIndicator = marketIndicatorRaw;
        }
        boolean useTradable = config.getBoolean("report.coverage.use_tradable_denominator", true);
        int selectedIndicator = useTradable ? tradableIndicator : marketIndicatorRaw;
        int selectedIndicatorDenominator = useTradable ? tradableDenominator : marketTotal;

        diagnostics.addCoverage("market_scan_fetch_coverage", marketFetch, marketTotal, source, owner);
        diagnostics.addCoverage("market_scan_indicator_coverage_raw", marketIndicatorRaw, marketTotal, source, owner);
        diagnostics.addCoverage("market_scan_indicator_coverage_tradable", tradableIndicator, tradableDenominator, source, owner);
        diagnostics.addCoverage("market_scan_indicator_coverage", selectedIndicator, selectedIndicatorDenominator, source, owner);
        double rawPct = marketTotal <= 0 ? 0.0 : (marketIndicatorRaw * 100.0 / marketTotal);
        double tradablePct = tradableDenominator <= 0 ? 0.0 : (tradableIndicator * 100.0 / tradableDenominator);
        diagnostics.addNote(String.format(Locale.US, "market_scan_indicator_coverage_raw_pct=%.1f", rawPct));
        diagnostics.addNote(String.format(Locale.US, "market_scan_indicator_coverage_tradable_pct=%.1f", tradablePct));
        if (summary != null && summary.breakdownDenominatorExcluded != null && !summary.breakdownDenominatorExcluded.isEmpty()) {
            diagnostics.addNote("market_tradable_denominator_excluded=" + summary.breakdownDenominatorExcluded);
        }

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

        Diagnostics.CoverageMetric watchIndicator = diagnostics.coverages.get("watchlist_indicator_coverage");
        Diagnostics.CoverageMetric marketTradable = diagnostics.coverages.get("market_scan_indicator_coverage_tradable");
        double minTop5Coverage = config.getDouble("report.top5.min_indicator_coverage_pct", 80.0);
        if (watchIndicator != null
                && watchIndicator.denominator > 0
                && watchIndicator.pct >= 99.9
                && marketTradable != null
                && marketTradable.denominator > 0
                && marketTradable.pct < minTop5Coverage) {
            diagnostics.addNote("Watchlist indicators are usable, but market Top5 sample is incomplete; use Top5 as reference only.");
        }
        if (marketTradable != null && marketTradable.denominator > 0) {
            boolean top5Enabled = marketTradable.pct >= minTop5Coverage;
            System.out.println(String.format(
                    Locale.US,
                    "Top5 coverage gate: enabled=%s tradable_pct=%.1f threshold=%.1f",
                    top5Enabled,
                    marketTradable.pct,
                    minTop5Coverage
            ));
        }
    }

private EventMemoryService.MemoryInsights collectMemoryInsights(
            String watchItem,
            UniverseRecord record,
            String industryZh,
            String industryEn,
            List<NewsItem> news,
            String technicalStatus,
            String risk,
            String technicalReasonsJson
    ) {
        if (!config.getBoolean("vector.memory.enabled", true) || eventMemoryService == null) {
            return EventMemoryService.MemoryInsights.empty();
        }
        try {
            return eventMemoryService.buildInsights(
                    watchItem,
                    record == null ? "" : record.ticker,
                    industryZh,
                    industryEn,
                    news,
                    technicalStatus,
                    risk,
                    technicalReasonsJson,
                    Instant.now()
            );
        } catch (Exception e) {
            System.err.println("WARN: vector memory build failed ticker="
                    + safeText(record == null ? "" : record.ticker) + ", err=" + e.getMessage());
            return EventMemoryService.MemoryInsights.empty();
        }
    }

    private List<String> mergeDigestLines(List<String>... digestGroups) {
        List<String> out = new ArrayList<>();
        if (digestGroups == null || digestGroups.length == 0) {
            return out;
        }
        for (List<String> group : digestGroups) {
            if (group == null || group.isEmpty()) {
                continue;
            }
            for (String line : group) {
                String text = safeText(line).trim();
                if (text.isEmpty()) {
                    continue;
                }
                if (!out.contains(text)) {
                    out.add(text);
                }
            }
        }
        return out;
    }

    private List<String> buildNewsDigestLines(List<NewsItem> news) {
        if (news == null || news.isEmpty()) {
            return List.of();
        }
        int maxDigestItems = Math.max(3, config.getInt("watchlist.news.digest_items", 8));
        List<String> lines = new ArrayList<>();
        for (NewsItem item : news) {
            if (item == null) {
                continue;
            }
            String title = normalizeUnknownText(item.title, "");
            if (title.isEmpty()) {
                continue;
            }
            String source = normalizeUnknownText(item.source, "");
            String ts = item.publishedAt == null ? "" : NEWS_TS_FMT.format(item.publishedAt);
            StringBuilder line = new StringBuilder();
            String cleanedTitle = TextFormatter.cleanForEmail(title)
                    .replace("\r", " ")
                    .replace("\n", " ")
                    .replaceAll("(?i)\\bunkown\\b", "unknown")
                    .trim();
            cleanedTitle = normalizeUnknownText(cleanedTitle, "");
            if (cleanedTitle.isEmpty()) {
                continue;
            }
            line.append(cleanedTitle);
            if (!source.isEmpty()) {
                line.append(" | ").append(source);
            }
            if (!ts.isEmpty()) {
                line.append(" | ").append(ts);
            }
            lines.add(line.toString());
            if (lines.size() >= maxDigestItems) {
                break;
            }
        }
        return lines;
    }

    private List<String> parseTopicCsv(String csv) {
        LinkedHashSet<String> out = new LinkedHashSet<>();
        if (csv != null) {
            for (String raw : csv.split(",")) {
                String token = normalizeNewsQueryToken(raw);
                if (!token.isEmpty()) {
                    out.add(token);
                }
                if (out.size() >= 12) {
                    break;
                }
            }
        }
        return new ArrayList<>(out);
    }

    private int resolveAutoTunedNewsInt(String key, int configuredValue, int accuracyTarget, int balancedTarget) {
        int configured = configuredValue;
        if (!config.getBoolean("news.performance.auto_tune", true)) {
            return configured;
        }
        if (!"default".equalsIgnoreCase(config.sourceOf(key))) {
            return configured;
        }
        String profile = safeText(config.getString("news.performance.profile", "accuracy")).toLowerCase(Locale.ROOT);
        if (profile.isEmpty()) {
            profile = "accuracy";
        }
        int cpu = Runtime.getRuntime().availableProcessors();
        long maxMemGb = Runtime.getRuntime().maxMemory() / (1024L * 1024L * 1024L);
        int target = "accuracy".equals(profile) ? accuracyTarget : balancedTarget;
        if ("news.query.max_variants".equals(key)) {
            int dynamic = "accuracy".equals(profile)
                    ? Math.min(16, Math.max(8, cpu / 2 + 4))
                    : Math.min(10, Math.max(6, cpu / 3 + 3));
            target = Math.max(target, dynamic);
        }
        int tuned = Math.max(configured, target);
        if (maxMemGb <= 4) {
            if ("news.query.max_variants".equals(key)) {
                tuned = Math.min(tuned, 10);
            } else if ("news.vector.query_expand.max_extra_queries".equals(key)) {
                tuned = Math.min(tuned, 2);
            } else if ("news.vector.query_expand.top_k".equals(key)) {
                tuned = Math.min(tuned, 10);
            }
        }
        return tuned;
    }

    private void addNewsQueryIfPresent(Set<String> out, String rawValue) {
        if (out == null) {
            return;
        }
        String normalized = normalizeNewsQueryToken(rawValue);
        if (normalized.isEmpty()) {
            return;
        }
        out.add(normalized);
    }

    private List<String> sanitizeNewsQueryList(List<String> queries) {
        LinkedHashSet<String> out = new LinkedHashSet<>();
        if (queries != null) {
            for (String query : queries) {
                String normalized = normalizeNewsQueryToken(query);
                if (!normalized.isEmpty()) {
                    out.add(normalized);
                }
            }
        }
        if (out.isEmpty()) {
            out.add("market");
        }
        return new ArrayList<>(out);
    }

    private String normalizeNewsQueryToken(String rawValue) {
        if (rawValue == null) {
            return "";
        }
        String cleaned = rawValue
                .replace('\u3000', ' ')
                .replaceAll("\\s+", " ")
                .trim();
        if (cleaned.isEmpty()) {
            return "";
        }
        String lower = cleaned.toLowerCase(Locale.ROOT);
        if (INVALID_TEXT_TOKENS.contains(lower)) {
            return "";
        }
        return cleaned;
    }

    private void logNewsQueryPlan(String ticker, List<String> baseQueries, List<String> expandedQueries, int droppedInvalid) {
        if (!config.getBoolean("news.fetch.log_keywords", true)) {
            return;
        }
        List<String> base = sanitizeNewsQueryList(baseQueries);
        List<String> expanded = sanitizeNewsQueryList(expandedQueries);
        LinkedHashSet<String> added = new LinkedHashSet<>(expanded);
        added.removeAll(new LinkedHashSet<>(base));
        System.out.println(String.format(
                Locale.US,
                "[NEWS_QUERY] ticker=%s base=%s expanded=%s added=%s dropped_invalid=%d",
                safeText(ticker),
                formatVectorQueryList(base, 10),
                formatVectorQueryList(expanded, 14),
                formatVectorQueryList(new ArrayList<>(added), 8),
                Math.max(0, droppedInvalid)
        ));
    }

    private String buildNewsSourcesConfig() {
        String explicit = config.getString("watchlist.news.sources", "");
        if ("override".equalsIgnoreCase(config.sourceOf("watchlist.news.sources")) && !explicit.isBlank()) {
            return explicit;
        }
        List<String> sources = new ArrayList<>();
        if (config.getBoolean("news.source.google_rss", true)) {
            sources.add("google");
        }
        if (config.getBoolean("news.source.bing", true)) {
            sources.add("bing");
        }
        if (config.getBoolean("news.source.yahoo_finance", true)) {
            sources.add("yahoo");
        }
        if (sources.isEmpty()) {
            sources.add("google");
            sources.add("bing");
            sources.add("yahoo");
        }
        return String.join(",", sources);
    }

    private void telemetryStart(String stepName) {
        if (telemetry == null) {
            return;
        }
        telemetry.startStep(stepName);
    }

    private void telemetryEnd(String stepName, long itemsIn, long itemsOut, long errorCount) {
        telemetryEnd(stepName, itemsIn, itemsOut, errorCount, "");
    }

    private void telemetryEnd(
            String stepName,
            long itemsIn,
            long itemsOut,
            long errorCount,
            String optionalNote
    ) {
        if (telemetry == null) {
            return;
        }
        telemetry.endStep(stepName, itemsIn, itemsOut, errorCount, optionalNote);
    }

    private Map<String, ModuleResult> buildModuleResults(
            List<WatchlistAnalysis> watchlistCandidates,
            List<ScoredCandidate> marketReferenceCandidates,
            int watchlistRequested,
            boolean includeMailPlaceholder
    ) {
        Map<String, ModuleResult> modules = new LinkedHashMap<>();
        List<WatchlistAnalysis> rows = watchlistCandidates == null ? List.of() : watchlistCandidates;
        List<ScoredCandidate> topCards = marketReferenceCandidates == null ? List.of() : marketReferenceCandidates;
        int needBars = Math.max(120, config.getInt("scan.min_history_bars", 180));

        int indicatorReady = 0;
        int maxBarsSeen = 0;
        int newsTotal = 0;
        int aiTriggered = 0;
        for (WatchlistAnalysis row : rows) {
            if (row == null) {
                continue;
            }
            if (row.indicatorReady) {
                indicatorReady++;
            }
            if (row.barsCount > maxBarsSeen) {
                maxBarsSeen = row.barsCount;
            }
            newsTotal += Math.max(0, row.newsCount);
            if (row.aiTriggered) {
                aiTriggered++;
            }
        }

        if (watchlistRequested <= 0) {
            modules.put("indicators", ModuleResult.insufficient(
                    "watchlist is empty",
                    Map.of("watchlist_size", 0, "need_bars", needBars, "got_bars", 0)
            ));
        } else if (!rows.isEmpty() && indicatorReady == rows.size()) {
            modules.put("indicators", ModuleResult.ok("indicator_ready=" + indicatorReady + "/" + rows.size()));
        } else {
            modules.put("indicators", ModuleResult.insufficient(
                    "need " + needBars + " bars but got " + maxBarsSeen,
                    Map.of(
                            "need_bars", needBars,
                            "got_bars", maxBarsSeen,
                            "ready_count", indicatorReady,
                            "watchlist_size", rows.size()
                    )
            ));
        }

        if (topCards.isEmpty()) {
            modules.put("top5", ModuleResult.insufficient(
                    "no market reference candidates",
                    Map.of("candidate_count", 0)
            ));
        } else {
            modules.put("top5", ModuleResult.ok("candidate_count=" + topCards.size()));
        }

        if (newsTotal <= 0) {
            modules.put("news", ModuleResult.insufficient(
                    "no relevant news matched",
                    Map.of("watchlist_size", rows.size(), "news_items", newsTotal)
            ));
        } else {
            modules.put("news", ModuleResult.ok("news_items=" + newsTotal));
        }

        if (!config.getBoolean("ai.enabled", true)) {
            modules.put("ai", ModuleResult.disabled(
                    "ai.enabled=false",
                    Map.of("ai_enabled", false)
            ));
        } else if (aiTriggered <= 0) {
            modules.put("ai", ModuleResult.insufficient(
                    "no watchlist item passed AI gate",
                    Map.of("watchlist_size", rows.size(), "triggered_count", aiTriggered)
            ));
        } else {
            modules.put("ai", ModuleResult.ok("triggered_count=" + aiTriggered));
        }

        if (includeMailPlaceholder) {
            modules.put("mail", ModuleResult.disabled(
                    "mail status resolved after render",
                    Map.of("phase", "pre_send")
            ));
        }

        return modules;
    }

private String safeText(String value) {
        return value == null ? "" : value.trim();
    }

    private static String safeStaticText(String value) {
        return value == null ? "" : value.trim();
    }

private String blankTo(String value, String fallback) {
        String t = safeText(value);
        return t.isEmpty() ? fallback : t;
    }

private double safeDouble(Double value) {
        if (value == null || !Double.isFinite(value)) {
            return 0.0;
        }
        return value;
    }

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

private NonJpHandling parseNonJpHandling(String raw) {
        String value = raw == null ? "" : raw.trim().toUpperCase(Locale.ROOT);
        if ("PROCESS_SEPARATELY".equals(value)) {
            return NonJpHandling.PROCESS_SEPARATELY;
        }
        return NonJpHandling.SKIP_WITH_REASON;
    }

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
        int threads = Math.max(1, config.getInt("fetch.concurrent", config.getInt("scan.threads", 8)));
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

private boolean shouldLogProgress(int completed, int total, int logEvery) {
        if (completed >= total) {
            return true;
        }
        if (logEvery <= 0) {
            return false;
        }
        return completed % logEvery == 0;
    }

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

private String formatSeconds(long seconds) {
        long sec = Math.max(0L, seconds);
        long h = sec / 3600;
        long m = (sec % 3600) / 60;
        long s = sec % 60;
        return String.format(Locale.US, "%02d:%02d:%02d", h, m, s);
    }

private double avgMillis(long nanos, int count) {
        if (count <= 0) {
            return 0.0;
        }
        return nanos / 1_000_000.0 / count;
    }

private double seconds(long nanos) {
        return nanos / 1_000_000_000.0;
    }

private long nanosToMillis(long nanos) {
        return Math.max(0L, Math.round(nanos / 1_000_000.0));
    }

private String normalizeRequestFailureCategory(String category, String errorMessage) {
        String raw = category == null ? "" : category.trim().toLowerCase(Locale.ROOT);
        String msg = errorMessage == null ? "" : errorMessage.trim().toLowerCase(Locale.ROOT);

        if (raw.isEmpty()) {
            raw = msg;
        }

        if (raw.contains("timeout") || msg.contains("timed out")) {
            return "timeout";
        }
        if (raw.contains("429") || msg.contains("http 429")) {
            return "rate_limit";
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

private TickerScanResult scanTicker(UniverseRecord universe) {
        long started = System.nanoTime();
        try {
            int minHistoryBars = Math.max(120, config.getInt("scan.min_history_bars", 180));
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

            YahooFetchResult yahooFetch = fetchBarsFromYahoo(universe.ticker, yahooTicker, fetchBarsMarket, "market");
            List<BarDaily> yahooBars = yahooFetch.bars;
            boolean yahooHasScreeningShape = hasScreeningShape(yahooBars);
            if (!yahooBars.isEmpty() && yahooHasScreeningShape) {
                return evaluateBars(
                        universe,
                        yahooBars,
                        0L,
                        0L,
                        "yahoo",
                        yahooFetch.requestFailed,
                        yahooFetch.requestFailureCategory,
                        nanosToMillis(System.nanoTime() - started),
                        requestFailureReason(yahooFetch.requestFailureCategory)
                );
            }

            if (!cachedBars.isEmpty() && !retryWhenCacheExists) {
                return evaluateBars(
                        universe,
                        cachedBars,
                        0L,
                        0L,
                        "cache",
                        yahooFetch.requestFailed,
                        yahooFetch.requestFailureCategory,
                        nanosToMillis(System.nanoTime() - started),
                        requestFailureReason(yahooFetch.requestFailureCategory)
                );
            }

            if (!yahooBars.isEmpty()) {
                return evaluateBars(
                        universe,
                        yahooBars,
                        0L,
                        0L,
                        "yahoo",
                        yahooFetch.requestFailed,
                        yahooFetch.requestFailureCategory,
                        nanosToMillis(System.nanoTime() - started),
                        requestFailureReason(yahooFetch.requestFailureCategory)
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
                        safeText(yahooFetch.requestFailureCategory).isEmpty() ? "no_data" : yahooFetch.requestFailureCategory,
                        nanosToMillis(System.nanoTime() - started),
                        requestFailureReason(safeText(yahooFetch.requestFailureCategory).isEmpty() ? "no_data" : yahooFetch.requestFailureCategory)
                );
            }

            return TickerScanResult.failed(
                    universe,
                    "fetch_failed:" + safeText(yahooFetch.requestFailureCategory),
                    0L,
                    0L,
                    "yahoo",
                    true,
                    safeText(yahooFetch.requestFailureCategory).isEmpty() ? "no_data" : yahooFetch.requestFailureCategory,
                    nanosToMillis(System.nanoTime() - started),
                    false,
                    0,
                    null,
                    Double.NaN,
                    false,
                    DataInsufficientReason.NO_DATA,
                    requestFailureReason(safeText(yahooFetch.requestFailureCategory).isEmpty() ? "no_data" : yahooFetch.requestFailureCategory)
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

        int freshDays = autoMarketFreshDays();
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
            IndicatorCoverageResult coverage = evaluateIndicatorCoverage(ind);
            if (!coverage.coreReady() || (!indicatorAllowPartial && !coverage.missingOptionalIndicators.isEmpty())) {
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
                        false,
                        DataInsufficientReason.NONE,
                        ScanFailureReason.NONE
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

private int autoWatchFreshDays() {
        return autoFreshDaysFromToday();
    }

private int autoMarketFreshDays() {
        return autoFreshDaysFromToday();
    }

    private int autoFreshDaysFromToday() {
        ZoneId zone = ZoneId.of(config.getString("app.zone", "Asia/Tokyo"));
        DayOfWeek dow = LocalDate.now(zone).getDayOfWeek();
        if (dow == DayOfWeek.MONDAY) {
            return 3;
        }
        if (dow == DayOfWeek.SUNDAY) {
            return 2;
        }
        if (dow == DayOfWeek.SATURDAY) {
            return 1;
        }
        return 1;
    }

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

private List<BarDaily> loadCachedBars(String ticker) {
        try {
            return barDailyDao.loadRecentBars(ticker, maxBars);
        } catch (SQLException ignored) {
            return List.of();
        }
    }

private void safeFinishFailed(long runId, Exception e) {
        try {
            String message = e.getClass().getSimpleName() + ": " + e.getMessage();
            runDao.finishRun(runId, "FAILED", 0, 0, 0, 0, null, message);
        } catch (Exception ignored) {
            // best effort
        }
    }

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

private void saveCheckpoint(BatchPlan plan, BatchState state, int topN) throws SQLException {
        if (!plan.resumeEnabled) {
            return;
        }
        BatchCheckpoint checkpoint = BatchCheckpoint.fromState(plan, state, topN);
        metadataDao.put(plan.checkpointKey, checkpoint.toJson().toString());
    }

private void clearCheckpoint(BatchPlan plan) throws SQLException {
        if (!plan.resumeEnabled) {
            return;
        }
        metadataDao.delete(plan.checkpointKey);
    }

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

private List<MarketSegment> segmentByFixedChunk(List<UniverseRecord> universe, int chunkSize) {
        List<MarketSegment> out = new ArrayList<>();
        for (int i = 0; i < universe.size(); i += chunkSize) {
            int to = Math.min(universe.size(), i + chunkSize);
            String key = "CHUNK#" + (i / chunkSize + 1);
            out.add(new MarketSegment(key, new ArrayList<>(universe.subList(i, to))));
        }
        return out;
    }

private String normalizeMarket(String market) {
        if (market == null || market.trim().isEmpty()) {
            return "UNKNOWN";
        }
        return market.trim();
    }

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

    private final class TickerTask implements Callable<TickerScanResult> {
        private final UniverseRecord universe;

        private TickerTask(UniverseRecord universe) {
            this.universe = universe;
        }

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
        final String error;
        final String newsSourceLabel;
        final List<String> clusterDigestLines;

        private LegacyWatchResult(
                StockContext context,
                String error,
                String newsSourceLabel,
                List<String> clusterDigestLines
        ) {
            this.context = context;
            this.error = error == null ? "" : error;
            this.newsSourceLabel = newsSourceLabel == null ? "" : newsSourceLabel;
            this.clusterDigestLines = clusterDigestLines == null ? List.of() : List.copyOf(clusterDigestLines);
        }
    }

    private static final class YahooFetchResult {
        final List<BarDaily> bars;
        final boolean requestFailed;
        final String requestFailureCategory;
        final String error;

        private YahooFetchResult(List<BarDaily> bars, boolean requestFailed, String requestFailureCategory, String error) {
            this.bars = bars == null ? List.of() : bars;
            this.requestFailed = requestFailed;
            this.requestFailureCategory = requestFailureCategory == null ? "" : requestFailureCategory;
            this.error = error == null ? "" : error;
        }

        static YahooFetchResult empty(String requestFailureCategory, String error) {
            return new YahooFetchResult(List.of(), true, requestFailureCategory, error);
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






