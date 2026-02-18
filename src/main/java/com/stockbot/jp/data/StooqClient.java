package com.stockbot.jp.data;

import com.stockbot.jp.config.Config;
import com.stockbot.jp.model.BarDaily;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

public final class StooqClient {
    private final String baseUrl;
    private final int timeoutSec;
    private final int retryCount;
    private final long retrySleepMs;
    private final long requestPauseMs;
    private final int maxBars;
    private final int timeoutStreakThreshold;
    private final long circuitCooldownMs;
    private final HttpClient httpClient;
    private final AtomicLong lastRequestAtNanos = new AtomicLong(0L);
    private final AtomicInteger timeoutStreak = new AtomicInteger(0);
    private final AtomicLong circuitOpenUntilNanos = new AtomicLong(0L);

    public StooqClient(Config config) {
        this.baseUrl = config.getString("stooq.base_url", "https://stooq.com/q/d/l/?s=%s&i=d");
        this.timeoutSec = Math.max(3, config.getInt("stooq.request_timeout_sec", 20));
        this.retryCount = Math.max(0, config.getInt("stooq.retry_count", 2));
        this.retrySleepMs = Math.max(100L, config.getLong("stooq.retry_sleep_ms", 700L));
        this.requestPauseMs = Math.max(0L, config.getLong("stooq.request_pause_ms", 0L));
        this.maxBars = Math.max(60, config.getInt("stooq.max_bars_per_ticker", 420));
        this.timeoutStreakThreshold = Math.max(1, config.getInt("stooq.circuit_breaker.timeout_streak", 10));
        this.circuitCooldownMs = Math.max(0L, config.getLong("stooq.circuit_breaker.cooldown_sec", 60L) * 1000L);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(timeoutSec))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    public List<BarDaily> fetchDaily(String ticker) throws Exception {
        FetchDailyResult result = fetchDailyProfiled(ticker);
        if (result.success) {
            return result.bars;
        }
        throw new IllegalStateException(result.error.isEmpty() ? "stooq_fetch_failed" : result.error);
    }

    public FetchDailyResult fetchDailyProfiled(String ticker) {
        return fetchDailyProfiled(ticker, retryCount);
    }

    public FetchDailyResult fetchDailyProfiled(String ticker, int retryLimitOverride) {
        String normalizedTicker = ticker.toLowerCase(Locale.ROOT).trim();
        if (normalizedTicker.isEmpty()) {
            return FetchDailyResult.success(List.of(), 0L, 0L, 0);
        }
        int retryLimit = Math.max(0, retryLimitOverride);
        String lastError = "";
        String lastCategory = "other";
        long downloadNanosTotal = 0L;
        long parseNanosTotal = 0L;
        for (int attempt = 0; attempt <= retryLimit; attempt++) {
            long attemptDownloadStarted = System.nanoTime();
            boolean downloadCounted = false;
            try {
                String url = String.format(baseUrl, normalizedTicker);
                waitIfCircuitOpen();
                throttleRequest(requestPauseMs);
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("User-Agent", "stockbot-jp/1.0")
                        .timeout(Duration.ofSeconds(timeoutSec))
                        .GET()
                        .build();
                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                downloadNanosTotal += Math.max(0L, System.nanoTime() - attemptDownloadStarted);
                downloadCounted = true;
                if (response.statusCode() / 100 != 2) {
                    throw new IllegalStateException("stooq http status=" + response.statusCode() + " ticker=" + ticker);
                }
                long parseStarted = System.nanoTime();
                try {
                    List<BarDaily> bars = parseCsv(normalizedTicker, response.body());
                    parseNanosTotal += Math.max(0L, System.nanoTime() - parseStarted);
                    onRequestResult(true, "");
                    return FetchDailyResult.success(bars, downloadNanosTotal, parseNanosTotal, attempt + 1);
                } catch (Exception parseError) {
                    parseNanosTotal += Math.max(0L, System.nanoTime() - parseStarted);
                    throw parseError;
                }
            } catch (Exception e) {
                if (!downloadCounted) {
                    downloadNanosTotal += Math.max(0L, System.nanoTime() - attemptDownloadStarted);
                }
                lastError = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
                lastCategory = classifyFailureMessage(lastError);
                onRequestResult(false, lastCategory);
                if (attempt >= retryLimit || !isRetryable(e)) {
                    break;
                }
                try {
                    Thread.sleep(retrySleepMs * (attempt + 1));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    String interrupted = "stooq_fetch_interrupted";
                    return FetchDailyResult.failed(
                            interrupted,
                            classifyFailureMessage(interrupted),
                            downloadNanosTotal,
                            parseNanosTotal,
                            attempt + 1
                    );
                }
            }
        }
        if (lastError == null || lastError.trim().isEmpty()) {
            lastError = "stooq_fetch_failed";
        }
        return FetchDailyResult.failed(lastError, lastCategory, downloadNanosTotal, parseNanosTotal, retryLimit + 1);
    }

    public static final class FetchDailyResult {
        public final List<BarDaily> bars;
        public final long downloadNanos;
        public final long parseNanos;
        public final int attempts;
        public final boolean success;
        public final String error;
        public final String errorCategory;

        private FetchDailyResult(
                List<BarDaily> bars,
                long downloadNanos,
                long parseNanos,
                int attempts,
                boolean success,
                String error,
                String errorCategory
        ) {
            this.bars = bars == null ? List.of() : bars;
            this.downloadNanos = Math.max(0L, downloadNanos);
            this.parseNanos = Math.max(0L, parseNanos);
            this.attempts = Math.max(0, attempts);
            this.success = success;
            this.error = error == null ? "" : error;
            this.errorCategory = errorCategory == null ? "other" : errorCategory;
        }

        public static FetchDailyResult success(List<BarDaily> bars, long downloadNanos, long parseNanos, int attempts) {
            return new FetchDailyResult(bars, downloadNanos, parseNanos, attempts, true, "", "");
        }

        public static FetchDailyResult failed(
                String error,
                String errorCategory,
                long downloadNanos,
                long parseNanos,
                int attempts
        ) {
            return new FetchDailyResult(List.of(), downloadNanos, parseNanos, attempts, false, error, errorCategory);
        }
    }

    private void onRequestResult(boolean success, String failureCategory) {
        if (success) {
            timeoutStreak.set(0);
            return;
        }
        String category = failureCategory == null ? "" : failureCategory.trim().toLowerCase(Locale.ROOT);
        if (!"timeout".equals(category)) {
            timeoutStreak.set(0);
            return;
        }
        int streak = timeoutStreak.incrementAndGet();
        if (streak < timeoutStreakThreshold || circuitCooldownMs <= 0L) {
            return;
        }
        timeoutStreak.set(0);
        openCircuitCooldown();
    }

    private void openCircuitCooldown() {
        if (circuitCooldownMs <= 0L) {
            return;
        }
        long now = System.nanoTime();
        long openUntil = now + TimeUnit.MILLISECONDS.toNanos(circuitCooldownMs);
        while (true) {
            long prev = circuitOpenUntilNanos.get();
            long next = Math.max(prev, openUntil);
            if (circuitOpenUntilNanos.compareAndSet(prev, next)) {
                if (next > prev) {
                    long pauseSec = Math.max(1L, circuitCooldownMs / 1000L);
                    System.err.println(String.format(
                            Locale.US,
                            "Stooq circuit breaker open: timeout_streak=%d cooldown=%ds",
                            timeoutStreakThreshold,
                            pauseSec
                    ));
                }
                return;
            }
        }
    }

    private void waitIfCircuitOpen() throws InterruptedException {
        while (true) {
            long until = circuitOpenUntilNanos.get();
            long now = System.nanoTime();
            if (until <= now) {
                return;
            }
            TimeUnit.NANOSECONDS.sleep(until - now);
        }
    }

    public static String classifyFailureMessage(String message) {
        String msg = message == null ? "" : message.toLowerCase(Locale.ROOT);
        if (msg.contains("timed out") || msg.contains("timeout") || msg.contains("connection reset")) {
            return "timeout";
        }
        if (msg.contains("stooq_rate_limit")
                || msg.contains("daily hits limit")
                || msg.contains("http status=429")) {
            return "rate_limit";
        }
        if (msg.contains("no data") || msg.contains("no_data")) {
            return "no_data";
        }
        return "other";
    }

    private void throttleRequest(long pauseMs) throws InterruptedException {
        if (pauseMs <= 0L) {
            return;
        }
        long pauseNanos = pauseMs * 1_000_000L;
        while (true) {
            long prev = lastRequestAtNanos.get();
            long now = System.nanoTime();
            long nextAllowed = prev + pauseNanos;
            if (now < nextAllowed) {
                long waitNanos = nextAllowed - now;
                TimeUnit.NANOSECONDS.sleep(waitNanos);
                continue;
            }
            if (lastRequestAtNanos.compareAndSet(prev, now)) {
                return;
            }
        }
    }

    private boolean isRetryable(Exception e) {
        String msg = e.getMessage() == null ? "" : e.getMessage().toLowerCase(Locale.ROOT);
        if (msg.contains("timed out") || msg.contains("timeout") || msg.contains("connection reset")) {
            return true;
        }
        if (msg.contains("http status=429") || msg.contains("http status=500") || msg.contains("http status=502")
                || msg.contains("http status=503") || msg.contains("http status=504")) {
            return true;
        }
        return false;
    }

    private List<BarDaily> parseCsv(String ticker, String body) {
        if (body == null) {
            return List.of();
        }
        String text = body.trim();
        if (text.isEmpty() || text.equalsIgnoreCase("No data")) {
            return List.of();
        }
        if (text.toLowerCase(Locale.ROOT).contains("exceeded the daily hits limit")) {
            throw new IllegalStateException("stooq_rate_limit");
        }

        String[] lines = text.split("\\r?\\n");
        if (lines.length == 0) {
            return List.of();
        }
        String header = lines[0].trim().toLowerCase(Locale.ROOT);
        if (!header.startsWith("date,open,high,low,close,volume")) {
            String sample = text.length() > 120 ? text.substring(0, 120) : text;
            throw new IllegalStateException("unexpected_stooq_payload:" + sample);
        }

        List<BarDaily> all = new ArrayList<>(Math.max(64, lines.length));
        for (int i = 1; i < lines.length; i++) {
            String line = lines[i].trim();
            if (line.isEmpty()) {
                continue;
            }
            String[] cols = line.split(",");
            if (cols.length < 6) {
                continue;
            }
            try {
                LocalDate date = LocalDate.parse(cols[0].trim());
                double open = parseDouble(cols[1]);
                double high = parseDouble(cols[2]);
                double low = parseDouble(cols[3]);
                double close = parseDouble(cols[4]);
                double volume = cols.length >= 6 ? parseDouble(cols[5]) : 0.0;
                if (close <= 0) {
                    continue;
                }
                all.add(new BarDaily(ticker, date, open, high, low, close, volume));
            } catch (Exception ignored) {
                // Skip malformed line.
            }
        }

        all.sort(Comparator.comparing(a -> a.tradeDate));
        if (all.size() <= maxBars) {
            return all;
        }
        return new ArrayList<>(all.subList(all.size() - maxBars, all.size()));
    }

    private double parseDouble(String input) {
        String v = input == null ? "" : input.trim();
        if (v.isEmpty() || v.equalsIgnoreCase("null")) {
            return 0.0;
        }
        return Double.parseDouble(v);
    }
}
