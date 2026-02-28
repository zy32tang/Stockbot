package com.stockbot.jp.config;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 模块说明：Config（class）。
 * 主要职责：承载 config 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class Config {

    private static final Map<String, String> DEFAULTS = buildDefaults();

    private final Properties props = new Properties();
    private final Properties resourceProps = new Properties();
    private final Properties overrideProps = new Properties();
    private final Path workingDir;

/**
 * 方法说明：Config，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private Config(Path workingDir) {
        this.workingDir = workingDir;
    }

/**
 * 方法说明：load，负责加载配置或数据。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public static Config load(Path workingDir) {
        Config config = new Config(workingDir);

        try (InputStream in = Config.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (in != null) {
                config.resourceProps.load(in);
                config.props.putAll(config.resourceProps);
            }
        } catch (IOException ignored) {
            // Ignore broken classpath config and continue with defaults/local file.
        }

        Path local = workingDir.resolve("config.properties");
        if (Files.exists(local)) {
            try (InputStream in = Files.newInputStream(local)) {
                config.overrideProps.load(in);
                config.props.putAll(config.overrideProps);
            } catch (IOException e) {
                System.err.println("WARN: failed to read config.properties: " + e.getMessage());
            }
        }

        return config;
    }

    /**
     * Build Config from Spring-bound configuration properties.
     */
    public static Config fromConfigurationProperties(Path workingDir, Map<String, ?> rawProperties) {
        Config config = new Config(workingDir);
        flattenInto(config, "", rawProperties);
        return config;
    }

/**
 * 方法说明：workingDir，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public Path workingDir() {
        return workingDir;
    }

/**
 * 方法说明：getString，负责获取数据并返回结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public String getString(String key) {
        String raw = props.getProperty(key);
        if (raw != null) {
            String trimmed = raw.trim();
            if (!trimmed.isEmpty()) {
                return trimmed;
            }
        }
        return DEFAULTS.getOrDefault(key, "");
    }

/**
 * 方法说明：getString，负责获取数据并返回结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public String getString(String key, String fallback) {
        String value = getString(key);
        if (value.isEmpty()) {
            return fallback;
        }
        return value;
    }

/**
 * 方法说明：getBoolean，负责获取数据并返回结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public boolean getBoolean(String key) {
        String value = getString(key);
        if (value.isEmpty()) {
            return false;
        }
        return "true".equalsIgnoreCase(value)
                || "1".equals(value)
                || "yes".equalsIgnoreCase(value)
                || "y".equalsIgnoreCase(value);
    }

/**
 * 方法说明：getBoolean，负责获取数据并返回结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public boolean getBoolean(String key, boolean fallback) {
        String raw = props.getProperty(key);
        if (raw == null || raw.trim().isEmpty()) {
            return fallback;
        }
        return getBoolean(key);
    }

/**
 * 方法说明：getInt，负责获取数据并返回结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public int getInt(String key) {
        return getInt(key, parseInt(DEFAULTS.get(key), 0));
    }

/**
 * 方法说明：getInt，负责获取数据并返回结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public int getInt(String key, int fallback) {
        String value = getString(key);
        return parseInt(value, fallback);
    }

/**
 * 方法说明：getLong，负责获取数据并返回结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public long getLong(String key, long fallback) {
        String value = getString(key);
        try {
            return Long.parseLong(value);
        } catch (Exception ignored) {
            return fallback;
        }
    }

/**
 * 方法说明：getDouble，负责获取数据并返回结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public double getDouble(String key) {
        return getDouble(key, parseDouble(DEFAULTS.get(key), 0.0));
    }

/**
 * 方法说明：getDouble，负责获取数据并返回结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public double getDouble(String key, double fallback) {
        String value = getString(key);
        return parseDouble(value, fallback);
    }

/**
 * 方法说明：getPath，负责获取数据并返回结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public Path getPath(String key) {
        String value = getString(key);
        if (value.isEmpty()) {
            return workingDir;
        }
        return workingDir.resolve(value).normalize();
    }

/**
 * 方法说明：getList，负责获取数据并返回结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public List<String> getList(String key) {
        String value = getString(key);
        if (value.isEmpty()) {
            return List.of();
        }
        List<String> out = new ArrayList<>();
        String[] tokens = value.split("[,;]");
        for (String token : tokens) {
            String trimmed = token.trim();
            if (!trimmed.isEmpty()) {
                out.add(trimmed);
            }
        }
        return out;
    }

/**
 * 方法说明：requireString，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public String requireString(String key) {
        String value = getString(key);
        if (value.isEmpty()) {
            throw new IllegalArgumentException("missing required config: " + key);
        }
        return value;
    }

/**
 * 方法说明：defaults，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public Map<String, String> defaults() {
        return DEFAULTS;
    }

/**
 * 方法说明：resolve，负责解析规则并确定最终结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public ResolvedValue resolve(String key) {
        return new ResolvedValue(
                key == null ? "" : key,
                getString(key),
                sourceOf(key)
        );
    }

/**
 * 方法说明：sourceOf，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public String sourceOf(String key) {
        if (key == null || key.trim().isEmpty()) {
            return "default";
        }
        String local = nonBlank(overrideProps.getProperty(key));
        if (!local.isEmpty()) {
            return "override";
        }
        String resource = nonBlank(resourceProps.getProperty(key));
        if (!resource.isEmpty()) {
            return "resource";
        }
        if (DEFAULTS.containsKey(key)) {
            return "default";
        }
        return "default";
    }

/**
 * 方法说明：nonBlank，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private String nonBlank(String raw) {
        if (raw == null) {
            return "";
        }
        String t = raw.trim();
        return t.isEmpty() ? "" : t;
    }

    private static void flattenInto(Config config, String prefix, Object value) {
        if (config == null || value == null) {
            return;
        }
        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String key = entry.getKey() == null ? "" : entry.getKey().toString().trim();
                if (key.isEmpty()) {
                    continue;
                }
                String fullKey = prefix.isEmpty() ? key : prefix + "." + key;
                flattenInto(config, fullKey, entry.getValue());
            }
            return;
        }
        if (value instanceof List<?> list) {
            List<String> parts = new ArrayList<>();
            for (Object item : list) {
                parts.add(stringify(item));
            }
            putBoundValue(config, prefix, String.join(",", parts));
            return;
        }
        if (value.getClass().isArray()) {
            int len = Array.getLength(value);
            List<String> parts = new ArrayList<>(len);
            for (int i = 0; i < len; i++) {
                parts.add(stringify(Array.get(value, i)));
            }
            putBoundValue(config, prefix, String.join(",", parts));
            return;
        }
        putBoundValue(config, prefix, stringify(value));
    }

    private static void putBoundValue(Config config, String key, String value) {
        if (config == null || key == null || key.trim().isEmpty()) {
            return;
        }
        String normalizedKey = key.trim();
        String normalizedValue = value == null ? "" : value;
        config.overrideProps.setProperty(normalizedKey, normalizedValue);
        config.props.setProperty(normalizedKey, normalizedValue);
    }

    private static String stringify(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

/**
 * 方法说明：parseInt，负责解析输入内容并转换结构。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static int parseInt(String value, int fallback) {
        try {
            return Integer.parseInt(value.trim());
        } catch (Exception ignored) {
            return fallback;
        }
    }

/**
 * 方法说明：parseDouble，负责解析输入内容并转换结构。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static double parseDouble(String value, double fallback) {
        try {
            return Double.parseDouble(value.trim());
        } catch (Exception ignored) {
            return fallback;
        }
    }

/**
 * 方法说明：buildDefaults，负责构建目标对象或输出内容。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private static Map<String, String> buildDefaults() {
        Map<String, String> defaults = new HashMap<>();

        defaults.put("outputs.dir", "outputs");
        defaults.put("db.path", "outputs/stockbot.db");
        defaults.put("db.url", "jdbc:postgresql://localhost:5432/stockbot");
        defaults.put("db.user", "stockbot");
        defaults.put("db.pass", "stockbot");
        defaults.put("db.schema", "stockbot");
        defaults.put("db.sql_log.enabled", "true");
        defaults.put("report.dir", "outputs/reports");
        defaults.put("report.mode.intraday.hideEntry", "true");
        defaults.put("watchlist.path", "watchlist.txt");
        defaults.put("app.zone", "Asia/Tokyo");
        defaults.put("app.mode", "DAILY");
        defaults.put("app.schedule.enabled", "false");
        defaults.put("app.reset_batch", "false");
        defaults.put("app.top_n_override", "0");
        defaults.put("app.background_scan.enabled", "false");
        defaults.put("app.background_scan.interval_sec", "86400");
        defaults.put("app.background_scan.force_universe_update", "false");
        defaults.put("app.background_scan.top_n_override", "0");

        defaults.put("jpx.universe.url",
                "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls");
        defaults.put("jpx.universe.refresh_days", "7");
        defaults.put("jpx.universe.force_update", "false");
        defaults.put("jpx.universe.request_timeout_sec", "20");
        defaults.put("yahoo.max_bars_per_ticker", "300");

        defaults.put("scan.threads", "3");
        defaults.put("scan.top_n", "15");
        defaults.put("scan.market_reference_top_n", "5");
        defaults.put("scan.max_universe_size", "0");
        defaults.put("scan.min_history_bars", "180");
        defaults.put("scan.cache.prefer_enabled", "true");
        defaults.put("scan.cache.fresh_days", "2");
        defaults.put("scan.network.retry_when_cache_exists", "false");
        defaults.put("scan.upsert.initial_days", "300");
        defaults.put("scan.upsert.incremental_recent_days", "10");
        defaults.put("scan.upsert.incremental_overlap_days", "2");
        defaults.put("scan.tradable.min_avg_volume_20", "50000");
        defaults.put("scan.tradable.min_price", "100");
        defaults.put("scan.tradable.max_zero_volume_days_20", "3");
        defaults.put("scan.tradable.flat_lookback_days", "5");
        defaults.put("scan.tradable.max_flat_days", "3");
        defaults.put("scan.min_price", "100");
        defaults.put("scan.max_price", "100000");
        defaults.put("scan.min_avg_volume", "50000");
        defaults.put("scan.min_score", "55");
        defaults.put("scan.batch.enabled", "true");
        defaults.put("scan.batch.segment_by_market", "true");
        defaults.put("scan.batch.market_chunk_size", "0");
        defaults.put("scan.batch.resume_enabled", "true");
        defaults.put("scan.batch.max_segments_per_run", "0");
        defaults.put("scan.batch.checkpoint_key", "daily.scan.batch.checkpoint.v1");
        defaults.put("scan.progress.log_every", "100");
        defaults.put("watchlist.price.duplicate_min_count", "2");

        defaults.put("filter.min_signals", "3");
        defaults.put("filter.hard.max_drop_3d_pct", "-8");
        defaults.put("filter.pullback_threshold_pct", "-8");
        defaults.put("filter.max_drawdown_pct", "-45");
        defaults.put("filter.rsi_floor", "20");
        defaults.put("filter.rsi_ceiling", "55");
        defaults.put("filter.max_pct_from_sma20", "2");
        defaults.put("filter.max_pct_from_sma60", "6");
        defaults.put("filter.band_proximity_pct", "3");

        defaults.put("risk.max_atr_pct", "9");
        defaults.put("risk.max_volatility_pct", "80");
        defaults.put("risk.max_drawdown_pct", "60");
        defaults.put("risk.min_volume_ratio", "0.3");
        defaults.put("risk.fail_atr_multiplier", "1.5");
        defaults.put("risk.fail_volatility_multiplier", "1.4");
        defaults.put("risk.penalty.atr_scale", "1.7");
        defaults.put("risk.penalty.atr_cap", "18");
        defaults.put("risk.penalty.volatility_scale", "0.45");
        defaults.put("risk.penalty.volatility_cap", "18");
        defaults.put("risk.penalty.drawdown_scale", "1.1");
        defaults.put("risk.penalty.drawdown_cap", "22");
        defaults.put("risk.penalty.liquidity", "12");
        defaults.put("risk.minVolume", "50000");
        defaults.put("risk.volMax", "0.06");

        defaults.put("position.single.maxPct", "0.05");
        defaults.put("position.total.maxPct", "0.50");
        defaults.put("report.position.max_single_pct", "5.0");
        defaults.put("report.position.max_total_pct", "50.0");
        defaults.put("report.run_type.close_time", "15:00");
        defaults.put("report.action.buy_score_threshold", "80");
        defaults.put("report.topcards.max_items", "0");
        defaults.put("rr.min", "1.5");
        defaults.put("plan.rr.min_floor", "1.1");
        defaults.put("plan.entry.buffer_pct", "0.5");
        defaults.put("plan.stop.atr_mult", "1.5");
        defaults.put("plan.target.high_lookback_mult", "0.98");
        defaults.put("stop.loss.lookbackDays", "20");
        defaults.put("stop.loss.bufferPct", "0.02");

        defaults.put("score.weight_pullback", "0.22");
        defaults.put("score.weight_rsi", "0.23");
        defaults.put("score.weight_sma_gap", "0.16");
        defaults.put("score.weight_band", "0.14");
        defaults.put("score.weight_rebound", "0.12");
        defaults.put("score.weight_volume", "0.13");

        defaults.put("email.enabled", "true");
        defaults.put("email.smtp_host", "smtp.gmail.com");
        defaults.put("email.smtp_port", "587");
        defaults.put("email.smtp_user", "");
        defaults.put("email.smtp_pass", "");
        defaults.put("email.from", "");
        defaults.put("email.to", "");
        defaults.put("email.subject_prefix", "[StockBot JP]");

        defaults.put("report.top5.min_fetch_coverage_pct", "80");
        defaults.put("report.top5.min_indicator_coverage_pct", "80");
        defaults.put("report.top5.skip_on_partial", "true");
        defaults.put("report.top5.allow_partial_when_coverage_ge", "101");
        defaults.put("report.coverage.use_tradable_denominator", "true");
        defaults.put("report.coverage.show_scope", "true");
        defaults.put("report.metrics.top5_perf.enabled", "false");
        defaults.put("report.metrics.top5_perf.win_rate_30d", "0");
        defaults.put("report.metrics.top5_perf.max_drawdown_30d", "0");
        defaults.put("report.advice.fetch_low_pct", "50");
        defaults.put("report.advice.indicator_low_pct", "50");
        defaults.put("report.advice.fetch_warn_pct", "80");
        defaults.put("report.advice.candidate_try_max", "4");
        defaults.put("report.advice.avg_score_try_threshold", "72");
        defaults.put("report.score.tier.focus_threshold", "80");
        defaults.put("report.score.tier.observe_threshold", "65");

        defaults.put("watchlist.news.lang", "ja");
        defaults.put("watchlist.news.region", "JP");
        defaults.put("watchlist.news.max_items", "20");
        defaults.put("watchlist.news.sources", "google,bing,yahoo,cnbc,marketwatch,wsj,nytimes,yahoonews,investing,ft,guardian,seekingalpha");
        defaults.put("watchlist.news.query_variants", "10");
        defaults.put("news.query.max_variants", "6");
        defaults.put("news.query.max_results_per_variant", "8");
        defaults.put("news.performance.auto_tune", "true");
        defaults.put("news.performance.profile", "accuracy");
        defaults.put("news.fetch.log_keywords", "true");
        defaults.put("news.vector.query_expand.enabled", "true");
        defaults.put("news.vector.query_expand.top_k", "8");
        defaults.put("news.vector.query_expand.max_extra_queries", "2");
        defaults.put("news.vector.query_expand.rounds", "2");
        defaults.put("news.vector.query_expand.seed_count", "3");
        defaults.put("news.source.google_rss", "true");
        defaults.put("news.source.bing", "true");
        defaults.put("news.source.yahoo_finance", "true");
        defaults.put("watchlist.news.query_topics", "株価,決算,業績,見通し,受注,設備投資,提携,規制,為替,金利,guidance,earnings,outlook,supply chain");
        defaults.put("watchlist.news.digest_items", "8");
        defaults.put("watchlist.non_jp_handling", "PROCESS_SEPARATELY");
        defaults.put("watchlist.default_market_for_alpha", "US");
        defaults.put("watchlist.ai.base_url", "http://127.0.0.1:11434");
        defaults.put("watchlist.ai.model", "llama3.1:latest");
        defaults.put("watchlist.ai.timeout_sec", "180");
        defaults.put("watchlist.ai.max_tokens", "220");
        defaults.put("watchlist.ai.max_chars", "1800");
        defaults.put("watchlist.ai.score_threshold", "-2.0");
        defaults.put("watchlist.ai.news_min", "8");
        defaults.put("watchlist.ai.drop_pct_threshold", "-2.0");
        defaults.put("ai.enabled", "true");
        defaults.put("ai.watchlist.mode", "ALL");
        defaults.put("ai.timeout_sec", "600");
        defaults.put("ai.max_tokens", "1200");
        defaults.put("ai.temperature", "0.2");
        defaults.put("fetch.concurrent", "12");
        defaults.put("news.concurrent", "10");
        defaults.put("fetch.bars", "520");
        defaults.put("fetch.bars.market", "520");
        defaults.put("fetch.bars.watchlist", "520");
        defaults.put("fetch.interval.market", "1d");
        defaults.put("fetch.retry.max", "2");
        defaults.put("fetch.retry.backoff_ms", "400");
        defaults.put("indicator.core", "sma20,sma60,rsi14,atr14");
        defaults.put("indicator.allow_partial", "true");
        defaults.put("vector.memory.enabled", "true");
        defaults.put("vector.memory.news.max_items", "3");
        defaults.put("vector.memory.news.top_k", "12");
        defaults.put("vector.memory.news.max_cases", "5");
        defaults.put("vector.memory.signal.top_k", "10");
        defaults.put("vector.memory.signal.max_cases", "20");
        defaults.put("ticker.name.cache.ttl_hours", "168");
        defaults.put("ticker.name.cache.path", "");
        defaults.put("mail.dry_run", "false");
        defaults.put("mail.fail_fast", "false");
        defaults.put("mail.dry_run.dir", "outputs/mail_dry_run");

        defaults.put("schedule.zone", "Asia/Tokyo");
        defaults.put("schedule.times", "11:30,15:00");
        defaults.put("schedule.time", "16:30");

        defaults.put("backtest.lookback_runs", "30");
        defaults.put("backtest.top_k", "5");
        defaults.put("backtest.hold_days", "10");

        return Collections.unmodifiableMap(defaults);
    }

    public static final class ResolvedValue {
        public final String key;
        public final String value;
        public final String source;

        public ResolvedValue(String key, String value, String source) {
            this.key = key == null ? "" : key;
            this.value = value == null ? "" : value;
            this.source = source == null ? "default" : source;
        }
    }
}
