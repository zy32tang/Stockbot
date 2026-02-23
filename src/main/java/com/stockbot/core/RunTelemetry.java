package com.stockbot.core;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Captures a single run's structured telemetry and summary.
 */
public final class RunTelemetry {
    public static final String STEP_NEWS_FETCH = "NEWS_FETCH";
    public static final String STEP_TEXT_CLEAN = "TEXT_CLEAN";
    public static final String STEP_EMBED = "EMBED";
    public static final String STEP_VECTOR_SEARCH = "VECTOR_SEARCH";
    public static final String STEP_AI_SUMMARY = "AI_SUMMARY";
    public static final String STEP_HTML_RENDER = "HTML_RENDER";
    public static final String STEP_MAIL_SEND = "MAIL_SEND";
    public static final String STEP_MARKET_FETCH = "MARKET_FETCH";
    public static final String STEP_INDICATORS = "INDICATORS";

    private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_INSTANT;

    private long runId;
    private final String runMode;
    private final String trigger;
    private final Instant startedAt;
    private Instant finishedAt;

    private boolean aiUsed;
    private String aiReason;
    private int newsItemsRaw;
    private int newsItemsDedup;
    private int clusters;
    private int errorsTotal;

    private final Map<String, StepStat> steps = new LinkedHashMap<>();
    private final Map<String, Deque<Long>> stepStartsNanos = new HashMap<>();

    public RunTelemetry(long runId, String runMode, String trigger, Instant startedAt) {
        this.runId = runId;
        this.runMode = blankTo(runMode, "ONCE");
        this.trigger = blankTo(trigger, "manual");
        this.startedAt = startedAt == null ? Instant.now() : startedAt;
        this.finishedAt = null;
        this.aiUsed = false;
        this.aiReason = "unknown";
        this.newsItemsRaw = 0;
        this.newsItemsDedup = 0;
        this.clusters = 0;
        this.errorsTotal = 0;
    }

    public synchronized long runId() {
        return runId;
    }

    public synchronized void setRunId(long runId) {
        if (runId > 0L) {
            this.runId = runId;
        }
    }

    public synchronized String runMode() {
        return runMode;
    }

    public synchronized String trigger() {
        return trigger;
    }

    public synchronized Instant startedAt() {
        return startedAt;
    }

    public synchronized Instant finishedAt() {
        return finishedAt;
    }

    public synchronized void startStep(String name) {
        String key = sanitizeStepName(name);
        steps.putIfAbsent(key, new StepStat(key));
        stepStartsNanos.computeIfAbsent(key, ignored -> new ArrayDeque<>()).push(System.nanoTime());
    }

    public synchronized void endStep(String name, long itemsIn, long itemsOut, long errorCount) {
        endStep(name, itemsIn, itemsOut, errorCount, "");
    }

    public synchronized void endStep(
            String name,
            long itemsIn,
            long itemsOut,
            long errorCount,
            String optionalNote
    ) {
        String key = sanitizeStepName(name);
        StepStat stat = steps.computeIfAbsent(key, StepStat::new);
        long startedNanos = 0L;
        Deque<Long> stack = stepStartsNanos.get(key);
        if (stack != null && !stack.isEmpty()) {
            startedNanos = stack.pop();
        }
        long elapsedMs = startedNanos <= 0L
                ? 0L
                : Math.max(0L, (System.nanoTime() - startedNanos) / 1_000_000L);
        stat.elapsedMs += elapsedMs;
        stat.itemsIn += Math.max(0L, itemsIn);
        stat.itemsOut += Math.max(0L, itemsOut);
        stat.errorCount += Math.max(0L, errorCount);
        if (optionalNote != null && !optionalNote.trim().isEmpty()) {
            if (stat.optionalNote.isEmpty()) {
                stat.optionalNote = optionalNote.trim();
            } else if (!stat.optionalNote.contains(optionalNote.trim())) {
                stat.optionalNote = stat.optionalNote + "; " + optionalNote.trim();
            }
        }
        if (errorCount > 0L) {
            errorsTotal += (int) Math.max(0L, errorCount);
        }
    }

    public synchronized void setStepNote(String name, String optionalNote) {
        if (optionalNote == null || optionalNote.trim().isEmpty()) {
            return;
        }
        String key = sanitizeStepName(name);
        StepStat stat = steps.computeIfAbsent(key, StepStat::new);
        if (stat.optionalNote.isEmpty()) {
            stat.optionalNote = optionalNote.trim();
        } else if (!stat.optionalNote.contains(optionalNote.trim())) {
            stat.optionalNote = stat.optionalNote + "; " + optionalNote.trim();
        }
    }

    public synchronized void setAiUsage(boolean used, String reason) {
        this.aiUsed = used;
        String normalizedReason = reason == null ? "" : reason.trim();
        if (used) {
            this.aiReason = normalizedReason.isEmpty() ? "used" : normalizedReason;
        } else {
            this.aiReason = normalizedReason.isEmpty() ? "unknown" : normalizedReason;
        }
    }

    public synchronized void incrementNewsStats(int rawInc, int dedupInc, int clusterInc) {
        this.newsItemsRaw += Math.max(0, rawInc);
        this.newsItemsDedup += Math.max(0, dedupInc);
        this.clusters += Math.max(0, clusterInc);
    }

    public synchronized void setNewsStats(int raw, int dedup, int clusterCount) {
        this.newsItemsRaw = Math.max(0, raw);
        this.newsItemsDedup = Math.max(0, dedup);
        this.clusters = Math.max(0, clusterCount);
    }

    public synchronized void incrementErrors(int count) {
        if (count <= 0) {
            return;
        }
        this.errorsTotal += count;
    }

    public synchronized void finish() {
        if (finishedAt == null) {
            finishedAt = Instant.now();
        }
    }

    public synchronized long totalElapsedMs() {
        Instant end = finishedAt == null ? Instant.now() : finishedAt;
        return Math.max(0L, Duration.between(startedAt, end).toMillis());
    }

    public synchronized List<StepRecord> stepRecords() {
        List<StepRecord> out = new ArrayList<>();
        for (StepStat stat : steps.values()) {
            out.add(new StepRecord(
                    stat.name,
                    stat.elapsedMs,
                    stat.itemsIn,
                    stat.itemsOut,
                    stat.errorCount,
                    stat.optionalNote
            ));
        }
        return out;
    }

    public synchronized String getSummary() {
        Instant end = finishedAt == null ? Instant.now() : finishedAt;
        StringBuilder sb = new StringBuilder();
        sb.append("run_id=").append(runId).append('\n');
        sb.append("run_mode=").append(runMode).append('\n');
        sb.append("trigger=").append(trigger).append('\n');
        sb.append("started_at=").append(ISO.format(startedAt)).append('\n');
        sb.append("finished_at=").append(ISO.format(end)).append('\n');
        sb.append("total_elapsed_ms=").append(Math.max(0L, Duration.between(startedAt, end).toMillis())).append('\n');
        sb.append("ai_used=").append(aiUsed);
        if (!aiUsed) {
            sb.append(" ai_reason=").append(blankTo(aiReason, "unknown"));
        } else if (aiReason != null && !aiReason.trim().isEmpty()) {
            sb.append(" ai_reason=").append(aiReason.trim());
        }
        sb.append('\n');
        sb.append("news_items_raw=").append(newsItemsRaw).append('\n');
        sb.append("news_items_dedup=").append(newsItemsDedup).append('\n');
        sb.append("clusters=").append(clusters).append('\n');
        sb.append("errors_total=").append(errorsTotal).append('\n');
        sb.append("steps:\n");
        for (StepStat stat : steps.values()) {
            sb.append(String.format(
                    Locale.US,
                    "  %s elapsed_ms=%d in=%d out=%d err=%d",
                    stat.name,
                    stat.elapsedMs,
                    stat.itemsIn,
                    stat.itemsOut,
                    stat.errorCount
            ));
            if (stat.optionalNote != null && !stat.optionalNote.isBlank()) {
                sb.append(" note=").append(stat.optionalNote.trim());
            }
            sb.append('\n');
        }
        return sb.toString().trim();
    }

    private String sanitizeStepName(String name) {
        String step = name == null ? "" : name.trim();
        return step.isEmpty() ? "UNKNOWN_STEP" : step.toUpperCase(Locale.ROOT);
    }

    private String blankTo(String value, String fallback) {
        String text = value == null ? "" : value.trim();
        return text.isEmpty() ? fallback : text;
    }

    private static final class StepStat {
        private final String name;
        private long elapsedMs;
        private long itemsIn;
        private long itemsOut;
        private long errorCount;
        private String optionalNote;

        private StepStat(String name) {
            this.name = name;
            this.elapsedMs = 0L;
            this.itemsIn = 0L;
            this.itemsOut = 0L;
            this.errorCount = 0L;
            this.optionalNote = "";
        }
    }

    public record StepRecord(
            String name,
            long elapsedMs,
            long itemsIn,
            long itemsOut,
            long errorCount,
            String optionalNote
    ) {
    }
}
