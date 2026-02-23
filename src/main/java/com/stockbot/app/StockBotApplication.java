package com.stockbot.app;

import com.stockbot.jp.backtest.BacktestRunner;
import com.stockbot.jp.config.Config;
import com.stockbot.jp.db.BarDailyDao;
import com.stockbot.jp.db.Database;
import com.stockbot.jp.db.MetadataDao;
import com.stockbot.jp.db.MigrationRunner;
import com.stockbot.jp.db.RunDao;
import com.stockbot.jp.db.ScanResultDao;
import com.stockbot.jp.db.SqliteToPostgresMigrator;
import com.stockbot.jp.db.UniverseDao;
import com.stockbot.jp.model.BacktestReport;
import com.stockbot.jp.model.DailyRunOutcome;
import com.stockbot.jp.model.RunRow;
import com.stockbot.jp.model.ScoredCandidate;
import com.stockbot.jp.model.WatchlistAnalysis;
import com.stockbot.jp.output.HtmlPostProcessor;
import com.stockbot.jp.output.Mailer;
import com.stockbot.jp.output.ReportBuilder;
import com.stockbot.jp.runner.DailyRunner;
import com.stockbot.jp.vector.EventMemoryService;
import com.stockbot.jp.vector.VectorSearchService;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.io.IoBuilder;

import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class StockBotApplication {
    private static final DateTimeFormatter SCHEDULE_TIME_FMT = DateTimeFormatter.ofPattern("H:mm");
    private static final DateTimeFormatter DISPLAY_TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");
    private static final Pattern REPORT_TS_PATTERN = Pattern.compile("jp_daily_(\\d{8}_\\d{6})\\.html", Pattern.CASE_INSENSITIVE);
    private static final Pattern INDICATOR_COVERAGE_PCT_PATTERN = Pattern.compile("INDICATOR_COVERAGE[^\\n%]*\\(([0-9]+(?:\\.[0-9]+)?)%\\)", Pattern.CASE_INSENSITIVE);
    private static volatile boolean LOG_ROUTE_INSTALLED = false;
    private static final String BG_SCAN_LAST_STARTUP_KEY = "daily.background_scan.last_startup_at";
    private static final Duration BG_SCAN_TRIGGER_INTERVAL = Duration.ofHours(8);
    private final HtmlPostProcessor htmlPostProcessor = new HtmlPostProcessor();
    private EventMemoryService eventMemoryService;
    private boolean runtimeSummaryLogged = false;

public static void main(String[] args) {
        int exit = new StockBotApplication().run(args);
        System.exit(exit);
    }

public int run(String[] args) {
        Options options = buildOptions();
        CommandLine cmd;
        try {
            cmd = new DefaultParser().parse(options, args);
        } catch (Exception e) {
            new HelpFormatter().printHelp("stockbot", options);
            System.err.println("ERROR: " + e.getMessage());
            return 2;
        }

        if (cmd.hasOption("help")) {
            new HelpFormatter().printHelp("stockbot", options);
            return 0;
        }

        try {
            Path workingDir = Path.of(".").toAbsolutePath().normalize();
            Config config = Config.load(workingDir);
            installLogRoutingIfNeeded(config);
            String mode = resolveMode(config);
            if (!mode.equals("DAILY") && !mode.equals("BACKTEST")) {
                System.err.println("ERROR: mode must be DAILY or BACKTEST (config app.mode).");
                return 2;
            }

            boolean noArgs = args == null || args.length == 0;
            boolean explicitTestCmd = cmd.hasOption("test");
            boolean explicitMigrateCmd = cmd.hasOption("migrate-sqlite-to-postgres");
            boolean explicitMaintenanceCmd = cmd.hasOption("reset-batch") || explicitMigrateCmd;
            boolean scheduleEnabled = (!explicitTestCmd && !explicitMaintenanceCmd && config.getBoolean("app.schedule.enabled", false))
                    || (!explicitTestCmd && !explicitMaintenanceCmd && noArgs && mode.equals("DAILY"));
            if (scheduleEnabled && !mode.equals("DAILY")) {
                System.err.println("ERROR: schedule requires DAILY mode (config app.mode=DAILY).");
                return 2;
            }

            Database database = new Database(
                    readDbUrl(config),
                    readDbUser(config),
                    readDbPass(config),
                    readDbSchema(config),
                    config.getBoolean("db.sql_log.enabled", true)
            );
            System.out.println("DB type=" + database.dbType()
                    + ", url=" + database.maskedJdbcUrl()
                    + ", schema=" + database.schema());
            new MigrationRunner().run(database);

            if (explicitMigrateCmd) {
                String sqlitePathRaw = cmd.getOptionValue("sqlite-path");
                if (sqlitePathRaw == null || sqlitePathRaw.trim().isEmpty()) {
                    System.err.println("ERROR: --sqlite-path is required when --migrate-sqlite-to-postgres is set.");
                    return 2;
                }
                Path sqlitePath = workingDir.resolve(sqlitePathRaw).normalize();
                SqliteToPostgresMigrator migrator = new SqliteToPostgresMigrator(database);
                SqliteToPostgresMigrator.MigrationStats stats = migrator.migrate(sqlitePath);
                System.out.println("SQLite migration completed.");
                System.out.println("watchlist_count=" + stats.watchlistCount);
                System.out.println("price_daily_count=" + stats.priceDailyCount);
                System.out.println("signals_count=" + stats.signalsCount);
                System.out.println("run_logs_count=" + stats.runLogsCount);
                return 0;
            }

            UniverseDao universeDao = new UniverseDao(database);
            MetadataDao metadataDao = new MetadataDao(database);
            BarDailyDao barDailyDao = new BarDailyDao(database);
            RunDao runDao = new RunDao(database);
            ScanResultDao scanResultDao = new ScanResultDao(database);
            this.eventMemoryService = new EventMemoryService(
                    config,
                    new VectorSearchService(database),
                    barDailyDao
            );
            runDao.recoverDanglingRuns();

            if (scheduleEnabled) {
                return runSchedule(cmd, config, universeDao, metadataDao, barDailyDao, runDao, scanResultDao);
            }

            return runOnce(cmd, mode, config, universeDao, metadataDao, barDailyDao, runDao, scanResultDao);
        } catch (Exception e) {
            System.err.println("FATAL: " + e.getMessage());
            e.printStackTrace();
            return 1;
        }
    }

private void installLogRoutingIfNeeded(Config config) {
        if (LOG_ROUTE_INSTALLED) {
            return;
        }
        synchronized (StockBotApplication.class) {
            if (LOG_ROUTE_INSTALLED) {
                return;
            }
            try {
                Path outputsDir = config.getPath("outputs.dir");
                Path logDir = outputsDir.resolve("log");
                Files.createDirectories(logDir);
                System.setProperty("stockbot.log.dir", logDir.toAbsolutePath().toString());

                // Init Log4j context first, so ConsoleAppender keeps original stdout/stderr streams.
                LogManager.getLogger(StockBotApplication.class);
                System.setOut(IoBuilder.forLogger("STDOUT").setLevel(Level.INFO).buildPrintStream());
                System.setErr(IoBuilder.forLogger("STDERR").setLevel(Level.ERROR).buildPrintStream());

                LOG_ROUTE_INSTALLED = true;
                System.out.println("Log4j routing enabled. dir=" + logDir.toAbsolutePath());
            } catch (Exception e) {
                System.err.println("WARN: failed to initialize log4j routing: " + e.getMessage());
            }
        }
    }

private int runSchedule(
            CommandLine cmd,
            Config config,
            UniverseDao universeDao,
            MetadataDao metadataDao,
            BarDailyDao barDailyDao,
            RunDao runDao,
            ScanResultDao scanResultDao
    ) throws Exception {
        ZoneId zoneId = ZoneId.of(config.getString("schedule.zone", "Asia/Tokyo"));
        List<LocalTime> runTimes = parseTimes(config.getString("schedule.times", ""));
        if (runTimes.isEmpty()) {
            LocalTime fallback = parseTime(config.getString("schedule.time", "16:30"));
            if (fallback != null) {
                runTimes = List.of(fallback);
            }
        }
        if (runTimes.isEmpty()) {
            System.err.println("ERROR: invalid schedule config. Use schedule.times=11:30,15:00 or schedule.time=16:30");
            return 2;
        }

        ReentrantLock runLock = new ReentrantLock();
        AtomicBoolean stopBackground = new AtomicBoolean(false);
        Thread backgroundThread = startBackgroundScanner(
                config,
                universeDao,
                metadataDao,
                barDailyDao,
                runDao,
                scanResultDao,
                runLock,
                stopBackground
        );
        System.out.println("Background scanner enabled. trigger=startup_gap>=8h OR runtime_every_8h");

        System.out.println("Schedule mode started. zone=" + zoneId + ", times=" + formatTimes(runTimes));
        try {
            while (true) {
                ZonedDateTime now = ZonedDateTime.now(zoneId);
                ZonedDateTime next = nextRunTime(now, runTimes);
                System.out.println("Next run at " + DISPLAY_TS_FMT.format(next));
                if (!sleepUntil(next)) {
                    return 130;
                }

                boolean locked = false;
                try {
                    runLock.lockInterruptibly();
                    locked = true;
                    int exit = runScheduledMergeReport(
                            cmd,
                            config,
                            universeDao,
                            metadataDao,
                            barDailyDao,
                            runDao,
                            scanResultDao,
                            zoneId
                    );
                    if (exit != 0) {
                        System.err.println("WARN: scheduled report event failed. exit=" + exit);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return 130;
                } finally {
                    if (locked) {
                        runLock.unlock();
                    }
                }
            }
        } finally {
            stopBackground.set(true);
            if (backgroundThread != null) {
                backgroundThread.interrupt();
            }
        }
    }

private int runOnce(
            CommandLine cmd,
            String mode,
            Config config,
            UniverseDao universeDao,
            MetadataDao metadataDao,
            BarDailyDao barDailyDao,
            RunDao runDao,
            ScanResultDao scanResultDao
    ) throws Exception {
        if (mode.equals("DAILY")) {
            if (cmd.hasOption("reset-batch")) {
                resetBatchCheckpointOnly(config, metadataDao);
                return 0;
            }
            if (cmd.hasOption("test")) {
                return sendTestDailyReportEmail(
                        config,
                        universeDao,
                        metadataDao,
                        barDailyDao,
                        runDao,
                        scanResultDao,
                        true
                );
            }
            ZoneId zoneId = ZoneId.of(config.getString("schedule.zone", "Asia/Tokyo"));
            return runScheduledMergeReport(
                    cmd,
                    config,
                    universeDao,
                    metadataDao,
                    barDailyDao,
                    runDao,
                    scanResultDao,
                    zoneId
            );
        }
        return runBacktest(config, barDailyDao, runDao);
    }

private int runScheduledMergeReport(
            CommandLine cmd,
            Config config,
            UniverseDao universeDao,
            MetadataDao metadataDao,
            BarDailyDao barDailyDao,
            RunDao runDao,
            ScanResultDao scanResultDao,
            ZoneId zoneId
    ) throws Exception {
        logRuntimeConfigSummary(config);
        DailyRunner dailyRunner = new DailyRunner(config, universeDao, metadataDao, barDailyDao, runDao, scanResultDao, eventMemoryService);
        boolean forceUniverse = config.getBoolean("app.background_scan.force_universe_update", false);
        Integer topN = null;
        int topOverride = config.getInt("app.background_scan.top_n_override", 0);
        if (topOverride > 0) {
            topN = topOverride;
        }

        Optional<RunRow> latestMarket = runDao.findLatestMarketScanRun();
        if (needsFreshMarketScan(latestMarket, zoneId)) {
            System.out.println("Scheduled event: no fresh market scan for today, running market scan first.");
            DailyRunOutcome seeded = dailyRunner.runMarketScanOnly(forceUniverse, topN, false);
            System.out.println("Scheduled pre-scan completed. run_id=" + seeded.runId
                    + ", batch_progress=" + seeded.processedSegments + "/" + seeded.totalSegments
                    + ", partial_run=" + seeded.partialRun);
        }

        List<String> watchlist = loadWatchlist(config);
        DailyRunOutcome reportOutcome = dailyRunner.runWatchlistReportFromLatestMarket(watchlist);
        sendDailyMailIfNeeded(cmd, config, reportOutcome);
        System.out.println("SCHEDULED_REPORT completed. run_id=" + reportOutcome.runId
                + ", watchlist=" + reportOutcome.watchlistCandidates.size()
                + ", market_ref=" + reportOutcome.marketReferenceCandidates.size());
        logDailySections(reportOutcome, config);
        System.out.println("report=" + reportOutcome.reportPath.toAbsolutePath());
        return 0;
    }

private boolean needsFreshMarketScan(Optional<RunRow> latestMarket, ZoneId zoneId) {
        if (latestMarket.isEmpty()) {
            return true;
        }
        RunRow run = latestMarket.get();
        if (run.startedAt == null) {
            return true;
        }
        return !run.startedAt.atZone(zoneId).toLocalDate().equals(ZonedDateTime.now(zoneId).toLocalDate());
    }

private void resetBatchCheckpointOnly(Config config, MetadataDao metadataDao) throws Exception {
        boolean batchEnabled = config.getBoolean("scan.batch.enabled", true);
        boolean resumeEnabled = batchEnabled && config.getBoolean("scan.batch.resume_enabled", true);
        String checkpointKey = config.getString("scan.batch.checkpoint_key", "daily.scan.batch.checkpoint.v1");
        if (resumeEnabled) {
            metadataDao.delete(checkpointKey);
            System.out.println("Batch checkpoint reset. key=" + checkpointKey);
        } else {
            System.out.println("Batch checkpoint reset skipped. resume is disabled.");
        }
        metadataDao.delete(BG_SCAN_LAST_STARTUP_KEY);
        System.out.println("Background scan startup trigger reset. key=" + BG_SCAN_LAST_STARTUP_KEY);
    }

private int runBacktest(Config config, BarDailyDao barDailyDao, RunDao runDao) throws Exception {
        long runId = runDao.startRun("BACKTEST", "rolling run-based backtest");
        try {
            BacktestRunner backtestRunner = new BacktestRunner(config, runDao, barDailyDao);
            BacktestReport report = backtestRunner.run();
            String summary = backtestRunner.toSummaryText(report);
            runDao.finishRun(
                    runId,
                    "SUCCESS",
                    report.runCount,
                    report.sampleCount,
                    report.sampleCount,
                    config.getInt("backtest.top_k", 5),
                    null,
                    summary
            );
            System.out.println(summary);
            System.out.println(runDao.summarizeRecentRuns(8));
            return 0;
        } catch (Exception e) {
            runDao.finishRun(runId, "FAILED", 0, 0, 0, 0, null, e.getMessage());
            throw e;
        }
    }

private int sendTestDailyReportEmail(
            Config config,
            UniverseDao universeDao,
            MetadataDao metadataDao,
            BarDailyDao barDailyDao,
            RunDao runDao,
            ScanResultDao scanResultDao,
            boolean forceSend
    ) throws Exception {
        logRuntimeConfigSummary(config);
        boolean sendEmail = config.getBoolean("email.enabled", true);
        if (forceSend) {
            sendEmail = true;
        }
        if (!sendEmail) {
            System.out.println("Email disabled. skip test DAILY report.");
            return 0;
        }

        DailyRunner dailyRunner = new DailyRunner(config, universeDao, metadataDao, barDailyDao, runDao, scanResultDao, eventMemoryService);
        List<String> watchlist = loadWatchlist(config);
        DailyRunOutcome outcome;
        try {
            outcome = dailyRunner.runWatchlistReportFromLatestMarket(watchlist);
        } catch (Exception e) {
            System.err.println("WARN: failed to rebuild test report from existing market scan: " + e.getMessage());
            return 2;
        }
        logDailySections(outcome, config);

        Path reportPath = outcome.reportPath == null ? null : outcome.reportPath.toAbsolutePath().normalize();
        if (reportPath == null || !Files.exists(reportPath)) {
            System.err.println("WARN: report file missing: " + reportPath);
            return 2;
        }

        Mailer mailer = new Mailer();
        Mailer.Settings settings = mailer.loadSettings(config);
        settings.enabled = true;
        ZoneId zoneId = ZoneId.of(config.getString("app.zone", "Asia/Tokyo"));
        Instant runAt = outcome.startedAt == null ? Instant.now() : outcome.startedAt;
        String subject = String.format(
                Locale.US,
                "%s 豈乗律謚･蜻・%s 蛟咎画焚 %d [run_id=%d]",
                settings.subjectPrefix,
                DateTimeFormatter.ofPattern("yyyy-MM-dd").format(runAt.atZone(zoneId)),
                outcome.marketReferenceCandidates.size(),
                outcome.runId
        );
        String text = "";
        String rawHtml = Files.readString(reportPath, StandardCharsets.UTF_8);
        List<Path> attachments = collectReportAttachments(rawHtml, reportPath);
        String html = htmlPostProcessor.cleanDocument(rawHtml, true);
        boolean sent = mailer.send(settings, subject, text, html, attachments);
        if (sent) {
            String modeLabel = settings.dryRun ? "Mail dry-run completed for " : "Email sent to ";
            System.out.println(modeLabel + String.join(",", settings.to) + " from run_id=" + outcome.runId);
        } else {
            System.out.println("Report generated, but email failed (mail.fail_fast=false). run_id=" + outcome.runId);
        }
        return 0;
    }

    private void sendDailyMailIfNeeded(CommandLine cmd, Config config, DailyRunOutcome outcome) throws Exception {
        boolean sendEmail = config.getBoolean("email.enabled", true);
        if (!sendEmail) {
            return;
        }

        Mailer mailer = new Mailer();
        Mailer.Settings settings = mailer.loadSettings(config);
        settings.enabled = true;

        ZoneId zoneId = ZoneId.of(config.getString("app.zone", "Asia/Tokyo"));
        boolean noviceMode = cmd != null && cmd.hasOption("novice");
        String rawHtml = Files.readString(outcome.reportPath, StandardCharsets.UTF_8);
        List<Path> attachments = noviceMode ? List.of() : collectReportAttachments(rawHtml, outcome.reportPath);
        String html = noviceMode ? "" : htmlPostProcessor.cleanDocument(rawHtml, true);
        String text = "";
        if (noviceMode) {
            double indicatorCoveragePct = parseIndicatorCoveragePct(rawHtml);
            ReportBuilder reportBuilder = new ReportBuilder(config);
            ReportBuilder.RunType runType = ReportBuilder.detectRunType(outcome.startedAt, zoneId);
            text = reportBuilder.buildNoviceActionSummary(indicatorCoveragePct, runType, outcome.watchlistCandidates);
        }

        String subject = String.format(
                Locale.US,
                "%s DAILY %s top %d",
                settings.subjectPrefix,
                DateTimeFormatter.ofPattern("yyyy-MM-dd").format(outcome.startedAt.atZone(zoneId)),
                outcome.marketReferenceCandidates.size()
        );
        boolean sent = mailer.send(settings, subject, text, html, attachments);
        if (sent) {
            String modeLabel = settings.dryRun ? "Mail dry-run completed for " : "Email sent to ";
            System.out.println(modeLabel + String.join(",", settings.to) + (noviceMode ? " [novice]" : ""));
        } else {
            System.out.println("Report generated, but email failed (mail.fail_fast=false).");
        }
    }

    private void logRuntimeConfigSummary(Config config) {
        if (runtimeSummaryLogged) {
            return;
        }
        runtimeSummaryLogged = true;
        int cpu = Runtime.getRuntime().availableProcessors();
        long maxMemGb = Runtime.getRuntime().maxMemory() / (1024L * 1024L * 1024L);
        int fetchConcurrent = config.getInt("fetch.concurrent", config.getInt("scan.threads", 8));
        int newsConcurrent = config.getInt("news.concurrent", 10);
        int aiTimeoutSec = config.getInt("ai.timeout_sec", config.getInt("watchlist.ai.timeout_sec", 180));
        int vectorTopK = config.getInt("vector.memory.signal.top_k", 10);
        System.out.println(String.format(
                Locale.US,
                "RUN_CONFIG cpu=%d max_mem_gb=%d fetch.concurrent=%d news.concurrent=%d ai.timeout_sec=%d vector.signal.top_k=%d",
                cpu,
                maxMemGb,
                fetchConcurrent,
                newsConcurrent,
                aiTimeoutSec,
                vectorTopK
        ));
    }

private void logDailySections(DailyRunOutcome outcome, Config config) {
        int referenceTop = Math.max(1, config.getInt("scan.market_reference_top_n", 5));
        System.out.println("watchlist_analysis_count=" + outcome.watchlistCandidates.size());
        printWatchlistLog("watchlist", outcome.watchlistCandidates, outcome.watchlistCandidates.size());

        System.out.println("market_reference_count=" + outcome.marketReferenceCandidates.size() + ", target_top=" + referenceTop);
        printCandidateLog("market_ref", outcome.marketReferenceCandidates, referenceTop);
    }

private void printWatchlistLog(String prefix, List<WatchlistAnalysis> rows, int limit) {
        int size = rows == null ? 0 : rows.size();
        int max = Math.min(size, Math.max(0, limit));
        for (int i = 0; i < max; i++) {
            WatchlistAnalysis r = rows.get(i);
            System.out.println(String.format(
                    Locale.US,
                    "%s_%d=%s | score=%.2f rating=%s risk=%s pct=%.2f%% ai=%s gate=%s ai_text=%s news=%d tech=%.2f tech_status=%s source=%s date=%s bars=%d cache_hit=%s latency=%dms suspect=%s",
                    prefix,
                    i + 1,
                    safe(r.displayName),
                    r.totalScore,
                    safe(r.rating),
                    safe(r.risk),
                    r.pctChange,
                    r.aiTriggered ? "Y" : "N",
                    trimForLog(safe(r.gateReason), 80),
                    trimForLog(safe(r.aiSummary), 120),
                    r.newsCount,
                    r.technicalScore,
                    safe(r.technicalStatus),
                    safe(r.dataSource),
                    safe(r.priceTimestamp),
                    r.barsCount,
                    r.cacheHit,
                    r.fetchLatencyMs,
                    r.priceSuspect
            ));
        }
    }

private void printCandidateLog(String prefix, List<ScoredCandidate> candidates, int limit) {
        int size = candidates == null ? 0 : candidates.size();
        int max = Math.min(size, Math.max(0, limit));
        for (int i = 0; i < max; i++) {
            ScoredCandidate c = candidates.get(i);
            System.out.println(String.format(
                    Locale.US,
                    "%s_%d=%s %s (%s) score=%.2f close=%.2f",
                    prefix,
                    i + 1,
                    safe(c.code),
                    safe(c.name),
                    safe(c.ticker),
                    c.score,
                    c.close
            ));
        }
    }

private List<Path> collectReportAttachments(String html, Path reportPath) {
        LinkedHashSet<Path> files = new LinkedHashSet<>();
        if (reportPath == null) {
            return new ArrayList<>(files);
        }

        Path baseDir = reportPath.toAbsolutePath().normalize().getParent();
        if (baseDir == null) {
            return new ArrayList<>(files);
        }

        if (html != null && !html.isEmpty()) {
            for (String src : htmlPostProcessor.localImageSources(html)) {
                try {
                    Path imagePath = baseDir.resolve(src).normalize();
                    if (Files.exists(imagePath) && Files.isRegularFile(imagePath)) {
                        files.add(imagePath);
                    }
                } catch (Exception ignored) {
                    // best effort
                }
            }
        }

        String reportFile = reportPath.getFileName() == null ? "" : reportPath.getFileName().toString();
        Matcher reportMatcher = REPORT_TS_PATTERN.matcher(reportFile);
        if (reportMatcher.matches()) {
            String ts = reportMatcher.group(1);
            Path trendDir = baseDir.resolve("trends");
            if (Files.isDirectory(trendDir)) {
                String glob = "*_" + ts + ".png";
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(trendDir, glob)) {
                    for (Path image : stream) {
                        if (Files.isRegularFile(image)) {
                            files.add(image.toAbsolutePath().normalize());
                        }
                    }
                } catch (Exception ignored) {
                    // best effort
                }
            }
        }
        return new ArrayList<>(files);
    }

    private double parseIndicatorCoveragePct(String rawHtml) {
        if (rawHtml == null || rawHtml.isEmpty()) {
            return 0.0;
        }
        Matcher m = INDICATOR_COVERAGE_PCT_PATTERN.matcher(rawHtml);
        if (!m.find()) {
            return 0.0;
        }
        try {
            return Double.parseDouble(m.group(1));
        } catch (Exception ignored) {
            return 0.0;
        }
    }

private String safe(String value) {
        return value == null ? "" : value;
    }

private String trimForLog(String value, int maxLen) {
        String text = safe(value).replace("\r", " ").replace("\n", " ").trim();
        if (text.length() <= maxLen) {
            return text;
        }
        return text.substring(0, Math.max(0, maxLen - 3)) + "...";
    }

private Options buildOptions() {
        Options options = new Options();
        options.addOption(Option.builder().longOpt("reset-batch").desc("clear batch checkpoint and background startup trigger checkpoint, then exit").build());
        options.addOption(Option.builder().longOpt("test").desc("rebuild DAILY report from latest market scan data and send test email (no full-market rescan)").build());
        options.addOption(Option.builder().longOpt("novice").desc("send novice action-only mail body (max 10 lines)").build());
        options.addOption(Option.builder().longOpt("migrate-sqlite-to-postgres").desc("one-off migrate data from SQLite into PostgreSQL").build());
        options.addOption(Option.builder().longOpt("sqlite-path").hasArg().argName("path").desc("path to source SQLite database file").build());
        options.addOption(Option.builder().longOpt("help").desc("show help").build());
        return options;
    }

    private String readDbUrl(Config config) {
        return firstNonBlank(
                System.getenv("STOCKBOT_DB_URL"),
                config.getString("db.url", "jdbc:postgresql://localhost:5432/stockbot")
        );
    }

    private String readDbUser(Config config) {
        return firstNonBlank(
                System.getenv("STOCKBOT_DB_USER"),
                config.getString("db.user", "stockbot")
        );
    }

    private String readDbPass(Config config) {
        return firstNonBlank(
                System.getenv("STOCKBOT_DB_PASS"),
                config.getString("db.pass", "stockbot")
        );
    }

    private String readDbSchema(Config config) {
        return firstNonBlank(config.getString("db.schema", "stockbot"));
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return "";
        }
        for (String value : values) {
            if (value == null) {
                continue;
            }
            String trimmed = value.trim();
            if (!trimmed.isEmpty()) {
                return trimmed;
            }
        }
        return "";
    }

private String normalizeMode(String value) {
        return value == null ? "DAILY" : value.trim().toUpperCase(Locale.ROOT);
    }

private String resolveMode(Config config) {
        return normalizeMode(config.getString("app.mode", "DAILY"));
    }

private Thread startBackgroundScanner(
            Config config,
            UniverseDao universeDao,
            MetadataDao metadataDao,
            BarDailyDao barDailyDao,
            RunDao runDao,
            ScanResultDao scanResultDao,
            ReentrantLock runLock,
            AtomicBoolean stopFlag
    ) {
        Instant startupAt = Instant.now();
        Thread t = new Thread(() -> {
            StartupScanDecision decision = evaluateStartupScanDecision(metadataDao, startupAt);
            if (!decision.shouldScan) {
                if (decision.lastStartupAt == null) {
                    System.out.println("IDLE_SCAN startup trigger skipped. no prior startup timestamp.");
                } else {
                    long hours = Duration.between(decision.lastStartupAt, startupAt).toHours();
                    System.out.println("IDLE_SCAN startup trigger skipped. startup gap=" + hours + "h < 8h (last_startup="
                            + decision.lastStartupAt + ", current_startup=" + startupAt + ")");
                }
            }

            boolean startupScanPending = decision.shouldScan;
            Instant nextRuntimeScanAt = startupAt.plus(BG_SCAN_TRIGGER_INTERVAL);

            while (!stopFlag.get()) {
                if (startupScanPending) {
                    boolean triggered = tryRunBackgroundScanOnce(
                            config,
                            universeDao,
                            metadataDao,
                            barDailyDao,
                            runDao,
                            scanResultDao,
                            runLock,
                            "startup"
                    );
                    if (triggered) {
                        startupScanPending = false;
                        nextRuntimeScanAt = Instant.now().plus(BG_SCAN_TRIGGER_INTERVAL);
                    } else if (!sleepMillisInterruptible(30_000L)) {
                        return;
                    }
                    continue;
                }

                Instant now = Instant.now();
                if (now.isBefore(nextRuntimeScanAt)) {
                    long ms = Duration.between(now, nextRuntimeScanAt).toMillis();
                    long sleepMs = Math.min(30_000L, Math.max(1L, ms));
                    if (!sleepMillisInterruptible(sleepMs)) {
                        return;
                    }
                    continue;
                }

                boolean triggered = tryRunBackgroundScanOnce(
                        config,
                        universeDao,
                        metadataDao,
                        barDailyDao,
                        runDao,
                        scanResultDao,
                        runLock,
                        "runtime_8h"
                );
                if (triggered) {
                    nextRuntimeScanAt = Instant.now().plus(BG_SCAN_TRIGGER_INTERVAL);
                } else if (!sleepMillisInterruptible(30_000L)) {
                    return;
                }
            }
        }, "stockbot-background-scanner");
        t.setDaemon(true);
        t.start();
        return t;
    }

private boolean tryRunBackgroundScanOnce(
            Config config,
            UniverseDao universeDao,
            MetadataDao metadataDao,
            BarDailyDao barDailyDao,
            RunDao runDao,
            ScanResultDao scanResultDao,
            ReentrantLock runLock,
            String trigger
    ) {
        boolean locked = runLock.tryLock();
        if (!locked) {
            System.out.println("IDLE_SCAN deferred. trigger=" + trigger + ", reason=run_lock_busy");
            return false;
        }
        try {
            runBackgroundScanOnce(config, universeDao, metadataDao, barDailyDao, runDao, scanResultDao);
            return true;
        } finally {
            runLock.unlock();
        }
    }

private void runBackgroundScanOnce(
            Config config,
            UniverseDao universeDao,
            MetadataDao metadataDao,
            BarDailyDao barDailyDao,
            RunDao runDao,
            ScanResultDao scanResultDao
    ) {
        try {
            boolean forceUniverse = config.getBoolean("app.background_scan.force_universe_update", false);
            Integer topN = null;
            int topOverride = config.getInt("app.background_scan.top_n_override", 0);
            if (topOverride > 0) {
                topN = topOverride;
            }

            DailyRunner dailyRunner = new DailyRunner(config, universeDao, metadataDao, barDailyDao, runDao, scanResultDao, eventMemoryService);
            Instant start = Instant.now();
            DailyRunOutcome outcome = dailyRunner.runMarketScanOnly(forceUniverse, topN, true);
            long sec = Duration.between(start, Instant.now()).toSeconds();
            System.out.println("IDLE_SCAN completed. run_id=" + outcome.runId
                    + ", batch_progress=" + outcome.processedSegments + "/" + outcome.totalSegments
                    + ", partial_run=" + outcome.partialRun
                    + ", market_ref=" + outcome.marketReferenceCandidates.size()
                    + ", elapsed_sec=" + sec);
        } catch (Exception e) {
            System.err.println("WARN: IDLE_SCAN failed: " + e.getMessage());
        }
    }

private StartupScanDecision evaluateStartupScanDecision(MetadataDao metadataDao, Instant startupAt) {
        Instant lastStartupAt = null;
        try {
            Optional<String> raw = metadataDao.get(BG_SCAN_LAST_STARTUP_KEY);
            if (raw.isPresent() && !raw.get().trim().isEmpty()) {
                try {
                    lastStartupAt = Instant.parse(raw.get().trim());
                } catch (Exception ignored) {
                    lastStartupAt = null;
                }
            }
            metadataDao.put(BG_SCAN_LAST_STARTUP_KEY, startupAt.toString());
        } catch (Exception e) {
            System.err.println("WARN: failed to read/write startup checkpoint for background scan: " + e.getMessage());
            return new StartupScanDecision(true, lastStartupAt);
        }
        if (lastStartupAt == null) {
            return new StartupScanDecision(true, null);
        }
        Duration gap = Duration.between(lastStartupAt, startupAt);
        boolean shouldScan = !gap.isNegative() && gap.compareTo(BG_SCAN_TRIGGER_INTERVAL) >= 0;
        return new StartupScanDecision(shouldScan, lastStartupAt);
    }

private boolean sleepMillisInterruptible(long ms) {
        try {
            Thread.sleep(Math.max(1L, ms));
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static final class StartupScanDecision {
        final boolean shouldScan;
        final Instant lastStartupAt;

        private StartupScanDecision(boolean shouldScan, Instant lastStartupAt) {
            this.shouldScan = shouldScan;
            this.lastStartupAt = lastStartupAt;
        }
    }

private List<String> loadWatchlist(Config config) {
        String watchlistPath = config.getString("watchlist.path", "watchlist.txt");
        Path path = config.workingDir().resolve(watchlistPath).normalize();
        if (!Files.exists(path)) {
            return List.of();
        }
        try {
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
            List<String> out = new ArrayList<>();
            for (String line : lines) {
                if (line == null) {
                    continue;
                }
                String t = line.trim();
                if (t.isEmpty() || t.startsWith("#")) {
                    continue;
                }
                out.add(t);
            }
            return out;
        } catch (Exception e) {
            System.err.println("WARN: failed to read watchlist: " + path + ", " + e.getMessage());
            return List.of();
        }
    }

private LocalTime parseTime(String value) {
        try {
            return LocalTime.parse(value, SCHEDULE_TIME_FMT);
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }

private List<LocalTime> parseTimes(String value) {
        if (value == null || value.trim().isEmpty()) {
            return List.of();
        }
        List<LocalTime> out = new ArrayList<>();
        for (String token : value.split(",")) {
            LocalTime parsed = parseTime(token.trim());
            if (parsed != null && !out.contains(parsed)) {
                out.add(parsed);
            }
        }
        Collections.sort(out);
        return out;
    }

private String formatTimes(List<LocalTime> times) {
        return times.stream()
                .map(t -> t.format(DateTimeFormatter.ofPattern("HH:mm")))
                .collect(Collectors.joining(","));
    }

private ZonedDateTime nextRunTime(ZonedDateTime now, List<LocalTime> runTimes) {
        for (LocalTime t : runTimes) {
            ZonedDateTime candidate = now.toLocalDate().atTime(t).atZone(now.getZone());
            if (candidate.isAfter(now)) {
                return candidate;
            }
        }
        return now.toLocalDate().plusDays(1).atTime(runTimes.get(0)).atZone(now.getZone());
    }

private boolean sleepUntil(ZonedDateTime next) {
        while (true) {
            long millis = Duration.between(ZonedDateTime.now(next.getZone()), next).toMillis();
            if (millis <= 0) {
                return true;
            }
            long chunk = Math.min(30_000L, millis);
            try {
                Thread.sleep(chunk);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
    }
}
