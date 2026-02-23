package com.stockbot.app;

import com.stockbot.app.properties.EmailProperties;
import com.stockbot.app.properties.MailProperties;
import com.stockbot.app.properties.ScanProperties;
import com.stockbot.core.ModuleResult;
import com.stockbot.core.RunTelemetry;
import com.stockbot.jp.backtest.BacktestRunner;
import com.stockbot.jp.config.Config;
import com.stockbot.jp.db.BarDailyDao;
import com.stockbot.jp.db.Database;
import com.stockbot.jp.db.MetadataDao;
import com.stockbot.jp.db.RunDao;
import com.stockbot.jp.db.ScanResultDao;
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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.io.IoBuilder;
import org.quartz.CronScheduleBuilder;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerContext;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.beans.factory.ObjectProvider;

import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@SpringBootApplication
public final class StockBotApplication implements ApplicationRunner {
    private enum ExecutionMode {
        ONCE,
        DAEMON
    }

    private static final String RUN_MODE_ONCE = "ONCE";
    private static final String RUN_MODE_DAEMON = "DAEMON";
    private static final String TRIGGER_MANUAL = "manual";
    private static final String TRIGGER_CRON = "cron";
    private static final String RUN_SUMMARY_START = "<!-- RUN_SUMMARY_START -->";
    private static final String RUN_SUMMARY_END = "<!-- RUN_SUMMARY_END -->";
    private static final Pattern REPORT_TS_PATTERN = Pattern.compile("jp_daily_(\\d{8}_\\d{6})\\.html", Pattern.CASE_INSENSITIVE);
    private static final Pattern INDICATOR_COVERAGE_PCT_PATTERN = Pattern.compile("INDICATOR_COVERAGE[^\\n%]*\\(([0-9]+(?:\\.[0-9]+)?)%\\)", Pattern.CASE_INSENSITIVE);
    private static volatile boolean LOG_ROUTE_INSTALLED = false;
    private final HtmlPostProcessor htmlPostProcessor = new HtmlPostProcessor();
    private final Config config;
    private final ScanProperties scanProperties;
    private final EmailProperties emailProperties;
    private final MailProperties mailProperties;
    private final ObjectProvider<Database> databaseProvider;
    private final ObjectProvider<UniverseDao> universeDaoProvider;
    private final ObjectProvider<MetadataDao> metadataDaoProvider;
    private final ObjectProvider<BarDailyDao> barDailyDaoProvider;
    private final ObjectProvider<RunDao> runDaoProvider;
    private final ObjectProvider<ScanResultDao> scanResultDaoProvider;
    private final ObjectProvider<EventMemoryService> eventMemoryServiceProvider;
    private EventMemoryService eventMemoryService;
    private boolean runtimeSummaryLogged = false;

    public StockBotApplication(
            Config config,
            ScanProperties scanProperties,
            EmailProperties emailProperties,
            MailProperties mailProperties,
            ObjectProvider<Database> databaseProvider,
            ObjectProvider<UniverseDao> universeDaoProvider,
            ObjectProvider<MetadataDao> metadataDaoProvider,
            ObjectProvider<BarDailyDao> barDailyDaoProvider,
            ObjectProvider<RunDao> runDaoProvider,
            ObjectProvider<ScanResultDao> scanResultDaoProvider,
            ObjectProvider<EventMemoryService> eventMemoryServiceProvider
    ) {
        this.config = config;
        this.scanProperties = scanProperties;
        this.emailProperties = emailProperties;
        this.mailProperties = mailProperties;
        this.databaseProvider = databaseProvider;
        this.universeDaoProvider = universeDaoProvider;
        this.metadataDaoProvider = metadataDaoProvider;
        this.barDailyDaoProvider = barDailyDaoProvider;
        this.runDaoProvider = runDaoProvider;
        this.scanResultDaoProvider = scanResultDaoProvider;
        this.eventMemoryServiceProvider = eventMemoryServiceProvider;
    }

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(StockBotApplication.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        Map<String, Object> defaults = new HashMap<>();
        defaults.put("spring.config.import", "optional:classpath:config.properties,optional:file:./config.properties");
        app.setDefaultProperties(defaults);
        app.run(args);
    }

    @Override
    public void run(ApplicationArguments arguments) {
        int exit = run(arguments.getSourceArgs());
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
            installLogRoutingIfNeeded(config);
            String mode = resolveMode(config);
            if (!mode.equals("DAILY") && !mode.equals("BACKTEST")) {
                System.err.println("ERROR: mode must be DAILY or BACKTEST (config app.mode).");
                return 2;
            }
            if (cmd.hasOption("once") && cmd.hasOption("daemon")) {
                System.err.println("ERROR: --once and --daemon cannot be used together.");
                return 2;
            }
            ExecutionMode executionMode = resolveExecutionMode(cmd);
            String trigger = resolveTrigger(cmd);
            int maxRuns = parseOptionalPositiveInt(cmd, "max-runs");
            int maxRuntimeMin = parseOptionalPositiveInt(cmd, "max-runtime-min");
            if (executionMode == ExecutionMode.DAEMON && !mode.equals("DAILY")) {
                System.err.println("ERROR: --daemon requires DAILY mode (config app.mode=DAILY).");
                return 2;
            }
            if (executionMode == ExecutionMode.ONCE && (maxRuns > 0 || maxRuntimeMin > 0)) {
                System.err.println("WARN: --max-runs/--max-runtime-min only apply in --daemon mode.");
            }

            Database database = databaseProvider.getObject();
            UniverseDao universeDao = universeDaoProvider.getObject();
            MetadataDao metadataDao = metadataDaoProvider.getObject();
            BarDailyDao barDailyDao = barDailyDaoProvider.getObject();
            RunDao runDao = runDaoProvider.getObject();
            ScanResultDao scanResultDao = scanResultDaoProvider.getObject();
            this.eventMemoryService = eventMemoryServiceProvider.getObject();

            System.out.println("DB type=" + database.dbType()
                    + ", url=" + database.maskedJdbcUrl()
                    + ", schema=" + database.schema());
            runDao.recoverDanglingRuns();

            if (executionMode == ExecutionMode.DAEMON) {
                return runSchedule(
                        cmd,
                        config,
                        universeDao,
                        metadataDao,
                        barDailyDao,
                        runDao,
                        scanResultDao,
                        trigger,
                        maxRuns,
                        maxRuntimeMin
                );
            }

            return runOnce(cmd, mode, config, universeDao, metadataDao, barDailyDao, runDao, scanResultDao, trigger);
        } catch (IllegalArgumentException e) {
            System.err.println("ERROR: " + e.getMessage());
            return 2;
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
            ScanResultDao scanResultDao,
            String trigger,
            int maxRuns,
            int maxRuntimeMin
    ) throws Exception {
        ZoneId zoneId = ZoneId.of("Asia/Tokyo");
        Instant daemonStartedAt = Instant.now();
        AtomicInteger completedRuns = new AtomicInteger(0);
        Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
        SchedulerContext schedulerContext = scheduler.getContext();
        schedulerContext.put(
                DailyReportJob.CONTEXT_KEY,
                new DailyReportJobContext(
                        this,
                        cmd,
                        config,
                        universeDao,
                        metadataDao,
                        barDailyDao,
                        runDao,
                        scanResultDao,
                        zoneId,
                        RUN_MODE_DAEMON,
                        trigger,
                        completedRuns
                )
        );

        JobDetail job = JobBuilder.newJob(DailyReportJob.class)
                .withIdentity("dailyReportJob", "stockbot")
                .build();
        Trigger trigger1130 = TriggerBuilder.newTrigger()
                .withIdentity("dailyReport-1130", "stockbot")
                .forJob(job)
                .withSchedule(CronScheduleBuilder.dailyAtHourAndMinute(11, 30)
                        .inTimeZone(TimeZone.getTimeZone(zoneId))
                        .withMisfireHandlingInstructionDoNothing())
                .build();
        Trigger trigger1500 = TriggerBuilder.newTrigger()
                .withIdentity("dailyReport-1500", "stockbot")
                .forJob(job)
                .withSchedule(CronScheduleBuilder.dailyAtHourAndMinute(15, 0)
                        .inTimeZone(TimeZone.getTimeZone(zoneId))
                        .withMisfireHandlingInstructionDoNothing())
                .build();
        Set<Trigger> triggers = new LinkedHashSet<>();
        triggers.add(trigger1130);
        triggers.add(trigger1500);
        scheduler.scheduleJob(job, triggers, true);

        scheduler.start();
        System.out.println(String.format(
                Locale.US,
                "DAEMON 模式已启动。zone=Asia/Tokyo, triggers=11:30,15:00, trigger=%s, max_runs=%s, max_runtime_min=%s",
                safe(trigger),
                maxRuns > 0 ? Integer.toString(maxRuns) : "unlimited",
                maxRuntimeMin > 0 ? Integer.toString(maxRuntimeMin) : "unlimited"
        ));

        Thread shutdownHook = new Thread(() -> {
            try {
                if (!scheduler.isShutdown()) {
                    scheduler.shutdown(true);
                }
            } catch (Exception e) {
                System.err.println("WARN: quartz shutdown failed: " + e.getMessage());
            }
        }, "stockbot-quartz-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdownHook);

        try {
            while (true) {
                if (scheduler.isShutdown()) {
                    return 0;
                }
                int finished = completedRuns.get();
                if (maxRuns > 0 && finished >= maxRuns) {
                    System.out.println(String.format(
                            Locale.US,
                            "DAEMON safety valve hit: max_runs=%d, completed_runs=%d",
                            maxRuns,
                            finished
                    ));
                    scheduler.shutdown(true);
                    return 0;
                }
                if (maxRuntimeMin > 0) {
                    long elapsedMin = Duration.between(daemonStartedAt, Instant.now()).toMinutes();
                    if (elapsedMin >= maxRuntimeMin) {
                        System.out.println(String.format(
                                Locale.US,
                                "DAEMON safety valve hit: max_runtime_min=%d, elapsed_min=%d",
                                maxRuntimeMin,
                                elapsedMin
                        ));
                        scheduler.shutdown(true);
                        return 0;
                    }
                }
                Thread.sleep(1000L);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return 130;
        } finally {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
                // JVM is already shutting down.
            }
            if (!scheduler.isShutdown()) {
                scheduler.shutdown(true);
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
            ScanResultDao scanResultDao,
            String trigger
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
                        true,
                        RUN_MODE_ONCE,
                        trigger
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
                    zoneId,
                    RUN_MODE_ONCE,
                    trigger
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
            ZoneId zoneId,
            String runMode,
            String trigger
    ) throws Exception {
        RunTelemetry telemetry = new RunTelemetry(0L, safe(runMode), normalizeTrigger(trigger), Instant.now());
        Map<String, ModuleResult> moduleResults = new LinkedHashMap<>();
        DailyRunOutcome reportOutcome = null;
        int exitCode = 0;
        try {
            logRuntimeConfigSummary(config);
            DailyRunner dailyRunner = new DailyRunner(
                    config,
                    universeDao,
                    metadataDao,
                    barDailyDao,
                    runDao,
                    scanResultDao,
                    eventMemoryService,
                    telemetry
            );
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
            reportOutcome = dailyRunner.runWatchlistReportFromLatestMarket(watchlist);
            telemetry.setRunId(reportOutcome.runId);
            moduleResults.putAll(buildModuleResults(reportOutcome));

            MailDispatchResult mailResult = sendDailyMailIfNeeded(cmd, config, reportOutcome, telemetry);
            moduleResults.put("mail", mailResult.moduleResult);
            exitCode = mailResult.exitCode;

            System.out.println("SCHEDULED_REPORT completed. run_id=" + reportOutcome.runId
                    + ", watchlist=" + reportOutcome.watchlistCandidates.size()
                    + ", market_ref=" + reportOutcome.marketReferenceCandidates.size());
            logDailySections(reportOutcome, config);
            if (reportOutcome.reportPath != null) {
                System.out.println("report=" + reportOutcome.reportPath.toAbsolutePath());
            }
            return exitCode;
        } catch (Exception e) {
            telemetry.incrementErrors(1);
            moduleResults.putIfAbsent(
                    "mail",
                    ModuleResult.error(
                            "mail_not_attempted_due_to_run_failure",
                            Map.of("error_class", e.getClass().getSimpleName())
                    )
            );
            throw e;
        } finally {
            telemetry.finish();
            if (reportOutcome != null) {
                telemetry.setRunId(reportOutcome.runId);
            }
            String summary = telemetry.getSummary();
            if (reportOutcome != null && reportOutcome.reportPath != null) {
                try {
                    appendRunSummaryToReport(reportOutcome.reportPath, summary);
                } catch (Exception e) {
                    System.err.println("WARN: failed to append run summary to report: " + e.getMessage());
                    telemetry.incrementErrors(1);
                }
            }
            logRunSummary(telemetry, moduleResults);
        }
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
        ScanProperties.Batch batch = scanProperties == null ? null : scanProperties.getBatch();
        boolean batchEnabled = batch == null || batch.isEnabled();
        boolean resumeEnabled = batchEnabled && (batch == null || batch.isResumeEnabled());
        String checkpointKey = "daily.scan.batch.checkpoint.v1";
        if (batch != null) {
            String candidate = batch.getCheckpointKey();
            if (candidate != null && !candidate.trim().isEmpty()) {
                checkpointKey = candidate.trim();
            }
        }
        if (resumeEnabled) {
            metadataDao.delete(checkpointKey);
            System.out.println("Batch checkpoint reset. key=" + checkpointKey);
        } else {
            System.out.println("Batch checkpoint reset skipped. resume is disabled.");
        }
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
            boolean forceSend,
            String runMode,
            String trigger
    ) throws Exception {
        RunTelemetry telemetry = new RunTelemetry(0L, safe(runMode), normalizeTrigger(trigger), Instant.now());
        Map<String, ModuleResult> moduleResults = new LinkedHashMap<>();
        DailyRunOutcome outcome = null;
        try {
            logRuntimeConfigSummary(config);
            boolean sendEmail = emailProperties == null || emailProperties.isEnabled();
            if (forceSend) {
                sendEmail = true;
            }
            if (!sendEmail) {
                moduleResults.put("mail", ModuleResult.disabled("email.enabled=false", Map.of("enabled", false)));
                return 0;
            }

            DailyRunner dailyRunner = new DailyRunner(
                    config,
                    universeDao,
                    metadataDao,
                    barDailyDao,
                    runDao,
                    scanResultDao,
                    eventMemoryService,
                    telemetry
            );
            List<String> watchlist = loadWatchlist(config);
            try {
                outcome = dailyRunner.runWatchlistReportFromLatestMarket(watchlist);
            } catch (Exception e) {
                telemetry.incrementErrors(1);
                System.err.println("WARN: failed to rebuild test report from existing market scan: " + e.getMessage());
                return 2;
            }
            telemetry.setRunId(outcome.runId);
            moduleResults.putAll(buildModuleResults(outcome));
            logDailySections(outcome, config);

            Path reportPath = outcome.reportPath == null ? null : outcome.reportPath.toAbsolutePath().normalize();
            if (reportPath == null || !Files.exists(reportPath)) {
                telemetry.incrementErrors(1);
                System.err.println("WARN: report file missing: " + reportPath);
                return 2;
            }

            Mailer mailer = new Mailer();
            Mailer.Settings settings = mailer.loadSettings(config, emailProperties, mailProperties);
            settings.enabled = true;
            ZoneId zoneId = ZoneId.of(config.getString("app.zone", "Asia/Tokyo"));
            Instant runAt = outcome.startedAt == null ? Instant.now() : outcome.startedAt;
            String subject = buildTestDailyReportSubject(
                    settings.subjectPrefix,
                    runAt,
                    zoneId,
                    outcome.marketReferenceCandidates.size(),
                    outcome.runId
            );
            String text = "";
            String rawHtml = Files.readString(reportPath, StandardCharsets.UTF_8);
            List<Path> attachments = collectReportAttachments(rawHtml, reportPath);
            String html = htmlPostProcessor.cleanDocument(rawHtml, true);
            if (telemetry != null) {
                telemetry.startStep(RunTelemetry.STEP_MAIL_SEND);
            }
            String htmlWithSummary = appendRunSummaryBlock(html, telemetry.getSummary());
            boolean sent;
            try {
                sent = mailer.send(settings, subject, text, htmlWithSummary, attachments);
                telemetry.endStep(
                        RunTelemetry.STEP_MAIL_SEND,
                        1,
                        sent ? 1 : 0,
                        sent ? 0 : 1,
                        settings.dryRun ? "dry_run=true" : ""
                );
            } catch (Exception e) {
                telemetry.endStep(RunTelemetry.STEP_MAIL_SEND, 1, 0, 1, e.getClass().getSimpleName());
                throw e;
            }
            moduleResults.put(
                    "mail",
                    sent
                            ? ModuleResult.ok("mail_sent")
                            : ModuleResult.error("mail_send_failed", Map.of("fail_fast", settings.failFast))
            );
            if (sent) {
                String modeLabel = settings.dryRun ? "Mail dry-run completed for " : "Email sent to ";
                System.out.println(modeLabel + String.join(",", settings.to) + " from run_id=" + outcome.runId);
            } else {
                System.out.println("Report generated, but email failed (mail.fail_fast=false). run_id=" + outcome.runId);
            }
            return 0;
        } finally {
            telemetry.finish();
            if (outcome != null && outcome.reportPath != null) {
                try {
                    appendRunSummaryToReport(outcome.reportPath, telemetry.getSummary());
                } catch (Exception e) {
                    System.err.println("WARN: failed to append run summary to report: " + e.getMessage());
                }
            }
            logRunSummary(telemetry, moduleResults);
        }
    }

    static String buildTestDailyReportSubject(
            String subjectPrefix,
            Instant runAt,
            ZoneId zoneId,
            int topCount,
            long runId
    ) {
        String date = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(runAt.atZone(zoneId));
        return String.format(
                Locale.US,
                "%s 日股日报 %s Top%d [run_id=%d]",
                subjectPrefix,
                date,
                topCount,
                runId
        );
    }

    private MailDispatchResult sendDailyMailIfNeeded(
            CommandLine cmd,
            Config config,
            DailyRunOutcome outcome,
            RunTelemetry telemetry
    ) throws Exception {
        boolean sendEmail = emailProperties == null || emailProperties.isEnabled();
        if (!sendEmail) {
            if (telemetry != null) {
                telemetry.startStep(RunTelemetry.STEP_MAIL_SEND);
                telemetry.endStep(RunTelemetry.STEP_MAIL_SEND, 1, 0, 0, "mail_disabled=true");
            }
            return new MailDispatchResult(
                    0,
                    ModuleResult.disabled("email.enabled=false", Map.of("enabled", false))
            );
        }

        Mailer mailer = new Mailer();
        Mailer.Settings settings = mailer.loadSettings(config, emailProperties, mailProperties);
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
        if (telemetry != null) {
            telemetry.startStep(RunTelemetry.STEP_MAIL_SEND);
        }
        String summaryForMail = telemetry == null ? "" : telemetry.getSummary();
        String htmlWithSummary = noviceMode ? "" : appendRunSummaryBlock(html, summaryForMail);
        String textWithSummary = noviceMode
                ? (text + "\n\nRun Summary\n" + summaryForMail)
                : text;

        String subject = String.format(
                Locale.US,
                "%s DAILY %s top %d",
                settings.subjectPrefix,
                DateTimeFormatter.ofPattern("yyyy-MM-dd").format(outcome.startedAt.atZone(zoneId)),
                outcome.marketReferenceCandidates.size()
        );
        boolean sent;
        try {
            sent = mailer.send(settings, subject, textWithSummary, htmlWithSummary, attachments);
            if (telemetry != null) {
                telemetry.endStep(
                        RunTelemetry.STEP_MAIL_SEND,
                        1,
                        sent ? 1 : 0,
                        sent ? 0 : 1,
                        settings.dryRun ? "dry_run=true" : ""
                );
            }
        } catch (Exception e) {
            if (telemetry != null) {
                telemetry.endStep(RunTelemetry.STEP_MAIL_SEND, 1, 0, 1, e.getClass().getSimpleName());
            }
            throw e;
        }
        if (sent) {
            String modeLabel = settings.dryRun ? "Mail dry-run completed for " : "Email sent to ";
            System.out.println(modeLabel + String.join(",", settings.to) + (noviceMode ? " [novice]" : "") + " run_id=" + outcome.runId);
        } else {
            System.out.println("Report generated, but email failed (mail.fail_fast=false). run_id=" + outcome.runId);
        }
        ModuleResult mailModule = sent
                ? ModuleResult.ok("mail_sent")
                : ModuleResult.error("mail_send_failed", Map.of("fail_fast", settings.failFast));
        return new MailDispatchResult(0, mailModule);
    }

    private void logRuntimeConfigSummary(Config config) {
        if (runtimeSummaryLogged) {
            return;
        }
        runtimeSummaryLogged = true;
        int cpu = Runtime.getRuntime().availableProcessors();
        long maxMemGb = Runtime.getRuntime().maxMemory() / (1024L * 1024L * 1024L);
        int scanThreads = scanProperties == null ? 8 : Math.max(1, scanProperties.getThreads());
        int fetchConcurrent = config.getInt("fetch.concurrent", scanThreads);
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
        int referenceTop = scanProperties == null ? 5 : Math.max(1, scanProperties.getMarketReferenceTopN());
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

    private ExecutionMode resolveExecutionMode(CommandLine cmd) {
        if (cmd != null && cmd.hasOption("daemon")) {
            return ExecutionMode.DAEMON;
        }
        return ExecutionMode.ONCE;
    }

    private String resolveTrigger(CommandLine cmd) {
        String raw = cmd == null ? TRIGGER_MANUAL : cmd.getOptionValue("trigger", TRIGGER_MANUAL);
        String normalized = normalizeTrigger(raw);
        if (!TRIGGER_MANUAL.equals(normalized) && !TRIGGER_CRON.equals(normalized)) {
            throw new IllegalArgumentException("trigger must be manual or cron");
        }
        return normalized;
    }

    private String normalizeTrigger(String value) {
        String normalized = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
        if (normalized.isEmpty()) {
            return TRIGGER_MANUAL;
        }
        return normalized;
    }

    private int parseOptionalPositiveInt(CommandLine cmd, String option) {
        if (cmd == null || option == null || !cmd.hasOption(option)) {
            return 0;
        }
        String raw = cmd.getOptionValue(option, "").trim();
        if (raw.isEmpty()) {
            return 0;
        }
        try {
            int parsed = Integer.parseInt(raw);
            if (parsed <= 0) {
                throw new IllegalArgumentException("--" + option + " must be a positive integer");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + option + " must be a positive integer");
        }
    }

    private Map<String, ModuleResult> buildModuleResults(DailyRunOutcome outcome) {
        Map<String, ModuleResult> results = new LinkedHashMap<>();
        if (outcome == null) {
            results.put("indicators", ModuleResult.error("missing outcome", Map.of("outcome_present", false)));
            results.put("top5", ModuleResult.error("missing outcome", Map.of("outcome_present", false)));
            results.put("news", ModuleResult.error("missing outcome", Map.of("outcome_present", false)));
            results.put("ai", ModuleResult.error("missing outcome", Map.of("outcome_present", false)));
            return results;
        }

        List<WatchlistAnalysis> watchRows = outcome.watchlistCandidates == null ? List.of() : outcome.watchlistCandidates;
        int needBars = Math.max(60, config.getInt("scan.min_history_bars", 180));
        int readyCount = 0;
        int maxBars = 0;
        String sampleTicker = "";
        int newsTotal = 0;
        int aiTriggered = 0;
        for (WatchlistAnalysis row : watchRows) {
            if (row == null) {
                continue;
            }
            if (row.indicatorReady) {
                readyCount++;
            }
            if (row.barsCount > maxBars) {
                maxBars = row.barsCount;
                sampleTicker = safe(row.ticker);
            }
            newsTotal += Math.max(0, row.newsCount);
            if (row.aiTriggered) {
                aiTriggered++;
            }
        }

        if (watchRows.isEmpty()) {
            results.put("indicators", ModuleResult.insufficient(
                    "need " + needBars + " bars but got 0",
                    Map.of("need_bars", needBars, "got_bars", 0, "watchlist_size", 0)
            ));
        } else if (readyCount == watchRows.size()) {
            results.put("indicators", ModuleResult.ok("indicator_ready=" + readyCount + "/" + watchRows.size()));
        } else {
            Map<String, Object> evidence = new LinkedHashMap<>();
            evidence.put("need_bars", needBars);
            evidence.put("got_bars", maxBars);
            evidence.put("ready_count", readyCount);
            evidence.put("watchlist_size", watchRows.size());
            if (!sampleTicker.isEmpty()) {
                evidence.put("symbol", sampleTicker);
            }
            results.put("indicators", ModuleResult.insufficient(
                    "need " + needBars + " bars but got " + maxBars,
                    evidence
            ));
        }

        int topCount = outcome.marketReferenceCandidates == null ? 0 : outcome.marketReferenceCandidates.size();
        if (topCount > 0) {
            results.put("top5", ModuleResult.ok("candidate_count=" + topCount));
        } else {
            results.put("top5", ModuleResult.insufficient(
                    "no market reference candidates",
                    Map.of("candidate_count", 0)
            ));
        }

        if (newsTotal > 0) {
            results.put("news", ModuleResult.ok("news_items=" + newsTotal));
        } else {
            results.put("news", ModuleResult.insufficient(
                    "no relevant news matched",
                    Map.of("watchlist_size", watchRows.size(), "news_items", 0)
            ));
        }

        if (!config.getBoolean("ai.enabled", true)) {
            results.put("ai", ModuleResult.disabled("ai.enabled=false", Map.of("ai_enabled", false)));
        } else if (aiTriggered > 0) {
            results.put("ai", ModuleResult.ok("triggered_count=" + aiTriggered));
        } else {
            results.put("ai", ModuleResult.insufficient(
                    "no watchlist item passed AI gate",
                    Map.of("watchlist_size", watchRows.size(), "triggered_count", 0)
            ));
        }
        return results;
    }

    private void appendRunSummaryToReport(Path reportPath, String summary) throws Exception {
        if (reportPath == null || !Files.exists(reportPath)) {
            return;
        }
        String rawHtml = Files.readString(reportPath, StandardCharsets.UTF_8);
        String merged = appendRunSummaryBlock(rawHtml, summary);
        Files.writeString(reportPath, merged, StandardCharsets.UTF_8);
    }

    private String appendRunSummaryBlock(String html, String summary) {
        String base = html == null ? "" : html;
        String safeSummary = summary == null ? "" : summary.trim();
        if (safeSummary.isEmpty()) {
            return base;
        }
        String cleaned = base.replaceAll("(?s)<!-- RUN_SUMMARY_START -->.*?<!-- RUN_SUMMARY_END -->", "");
        String block = RUN_SUMMARY_START
                + "<div class=\"card\"><h3 style=\"margin-top:0\">Run Summary</h3><pre class=\"small\" style=\"white-space:pre-wrap\">"
                + escapeHtml(safeSummary)
                + "</pre></div>"
                + RUN_SUMMARY_END;
        if (cleaned.toLowerCase(Locale.ROOT).contains("</body>")) {
            return cleaned.replaceFirst("(?i)</body>", Matcher.quoteReplacement(block + "</body>"));
        }
        return cleaned + block;
    }

    private String escapeHtml(String value) {
        if (value == null || value.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder(value.length() + 32);
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '&') {
                sb.append("&amp;");
            } else if (c == '<') {
                sb.append("&lt;");
            } else if (c == '>') {
                sb.append("&gt;");
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private void logRunSummary(RunTelemetry telemetry, Map<String, ModuleResult> moduleResults) {
        if (telemetry == null) {
            return;
        }
        String summary = telemetry.getSummary();
        System.out.println("RUN_SUMMARY run_id=" + telemetry.runId());
        System.out.println(summary);
        if (moduleResults != null && !moduleResults.isEmpty()) {
            for (Map.Entry<String, ModuleResult> entry : moduleResults.entrySet()) {
                ModuleResult result = entry.getValue();
                if (result == null) {
                    continue;
                }
                System.out.println(String.format(
                        Locale.US,
                        "MODULE_STATUS run_id=%d module=%s status=%s reason=%s evidence=%s",
                        telemetry.runId(),
                        safe(entry.getKey()),
                        result.status(),
                        safe(result.reason()),
                        safe(new LinkedHashMap<>(result.evidence()).toString())
                ));
            }
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
        options.addOption(Option.builder().longOpt("once").desc("run once and exit (default)").build());
        options.addOption(Option.builder().longOpt("daemon").desc("run with Quartz schedule loop").build());
        options.addOption(Option.builder().longOpt("trigger").hasArg().argName("manual|cron").desc("run trigger source (default: manual)").build());
        options.addOption(Option.builder().longOpt("max-runs").hasArg().argName("N").desc("daemon safety valve: stop after N completed runs").build());
        options.addOption(Option.builder().longOpt("max-runtime-min").hasArg().argName("M").desc("daemon safety valve: stop after M minutes").build());
        options.addOption(Option.builder().longOpt("reset-batch").desc("clear batch checkpoint, then exit").build());
        options.addOption(Option.builder().longOpt("test").desc("rebuild DAILY report from latest market scan data and send test email (no full-market rescan)").build());
        options.addOption(Option.builder().longOpt("novice").desc("send novice action-only mail body (max 10 lines)").build());
        options.addOption(Option.builder().longOpt("help").desc("show help").build());
        return options;
    }

private String normalizeMode(String value) {
        return value == null ? "DAILY" : value.trim().toUpperCase(Locale.ROOT);
    }

private String resolveMode(Config config) {
        return normalizeMode(config.getString("app.mode", "DAILY"));
    }

    private static final class MailDispatchResult {
        final int exitCode;
        final ModuleResult moduleResult;

        private MailDispatchResult(int exitCode, ModuleResult moduleResult) {
            this.exitCode = exitCode;
            this.moduleResult = moduleResult == null
                    ? ModuleResult.error("mail_result_missing", Map.of())
                    : moduleResult;
        }
    }

    private static final class DailyReportJobContext {
        final StockBotApplication app;
        final CommandLine cmd;
        final Config config;
        final UniverseDao universeDao;
        final MetadataDao metadataDao;
        final BarDailyDao barDailyDao;
        final RunDao runDao;
        final ScanResultDao scanResultDao;
        final ZoneId zoneId;
        final String runMode;
        final String trigger;
        final AtomicInteger completedRuns;

        private DailyReportJobContext(
                StockBotApplication app,
                CommandLine cmd,
                Config config,
                UniverseDao universeDao,
                MetadataDao metadataDao,
                BarDailyDao barDailyDao,
                RunDao runDao,
                ScanResultDao scanResultDao,
                ZoneId zoneId,
                String runMode,
                String trigger,
                AtomicInteger completedRuns
        ) {
            this.app = app;
            this.cmd = cmd;
            this.config = config;
            this.universeDao = universeDao;
            this.metadataDao = metadataDao;
            this.barDailyDao = barDailyDao;
            this.runDao = runDao;
            this.scanResultDao = scanResultDao;
            this.zoneId = zoneId;
            this.runMode = runMode;
            this.trigger = trigger;
            this.completedRuns = completedRuns == null ? new AtomicInteger(0) : completedRuns;
        }
    }

    @DisallowConcurrentExecution
    public static final class DailyReportJob implements Job {
        static final String CONTEXT_KEY = "stockbot.dailyReportJobContext";
        private static final ReentrantLock RUN_LOCK = new ReentrantLock();

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            if (!RUN_LOCK.tryLock()) {
                System.out.println("Quartz trigger skipped: DailyReportJob is already running.");
                return;
            }
            try {
                SchedulerContext schedulerContext = context.getScheduler().getContext();
                Object raw = schedulerContext.get(CONTEXT_KEY);
                if (!(raw instanceof DailyReportJobContext)) {
                    throw new JobExecutionException("missing DailyReportJobContext");
                }
                DailyReportJobContext jobContext = (DailyReportJobContext) raw;
                int exit;
                try {
                    exit = jobContext.app.runScheduledMergeReport(
                            jobContext.cmd,
                            jobContext.config,
                            jobContext.universeDao,
                            jobContext.metadataDao,
                            jobContext.barDailyDao,
                            jobContext.runDao,
                            jobContext.scanResultDao,
                            jobContext.zoneId,
                            jobContext.runMode,
                            jobContext.trigger
                    );
                } finally {
                    jobContext.completedRuns.incrementAndGet();
                }
                if (exit != 0) {
                    throw new JobExecutionException("scheduled report failed, exit=" + exit, false);
                }
            } catch (JobExecutionException e) {
                throw e;
            } catch (Exception e) {
                throw new JobExecutionException(e, false);
            } finally {
                RUN_LOCK.unlock();
            }
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
}
