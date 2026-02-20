package com.stockbot.jp.watch;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 模块说明：TickerResolver（class）。
 * 主要职责：承载 watch 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class TickerResolver {
    private static final Pattern JP_CODE_OR_SUFFIX = Pattern.compile("^(\\d{4})(?:\\.T)?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern US_ALPHA = Pattern.compile("^[A-Z]{1,5}$");
    private static final Pattern US_WITH_SUFFIX = Pattern.compile("^[A-Z]{1,5}\\.(US|NQ|N)$", Pattern.CASE_INSENSITIVE);

    private final TickerSpec.Market defaultMarketForAlpha;

/**
 * 方法说明：TickerResolver，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public TickerResolver(String defaultMarketForAlpha) {
        this.defaultMarketForAlpha = parseMarket(defaultMarketForAlpha);
    }

/**
 * 方法说明：resolve，负责解析规则并确定最终结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public TickerSpec resolve(String rawInput) {
        String raw = rawInput == null ? "" : rawInput.trim();
        if (raw.isEmpty()) {
            return new TickerSpec("", TickerSpec.Market.UNKNOWN, "", TickerSpec.ResolveStatus.INVALID);
        }

        String upper = raw.toUpperCase(Locale.ROOT);

        Matcher jpMatcher = JP_CODE_OR_SUFFIX.matcher(upper);
        if (jpMatcher.matches()) {
            String code = jpMatcher.group(1);
            return new TickerSpec(raw, TickerSpec.Market.JP, code + ".T", TickerSpec.ResolveStatus.OK);
        }

        if (US_WITH_SUFFIX.matcher(upper).matches()) {
            return new TickerSpec(raw, TickerSpec.Market.US, upper, TickerSpec.ResolveStatus.OK);
        }

        if (US_ALPHA.matcher(upper).matches()) {
            if (defaultMarketForAlpha == TickerSpec.Market.US) {
                return new TickerSpec(raw, TickerSpec.Market.US, upper, TickerSpec.ResolveStatus.OK);
            }
            return new TickerSpec(raw, TickerSpec.Market.UNKNOWN, upper, TickerSpec.ResolveStatus.NEED_MARKET_HINT);
        }

        return new TickerSpec(raw, TickerSpec.Market.UNKNOWN, upper, TickerSpec.ResolveStatus.INVALID);
    }

/**
 * 方法说明：parseMarket，负责解析输入内容并转换结构。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private TickerSpec.Market parseMarket(String raw) {
        String token = raw == null ? "" : raw.trim().toUpperCase(Locale.ROOT);
        if ("JP".equals(token)) {
            return TickerSpec.Market.JP;
        }
        if ("US".equals(token)) {
            return TickerSpec.Market.US;
        }
        return TickerSpec.Market.UNKNOWN;
    }
}
