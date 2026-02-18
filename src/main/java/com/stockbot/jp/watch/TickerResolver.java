package com.stockbot.jp.watch;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TickerResolver {
    private static final Pattern JP_CODE_OR_SUFFIX = Pattern.compile("^(\\d{4})(?:\\.T)?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern US_ALPHA = Pattern.compile("^[A-Z]{1,5}$");
    private static final Pattern US_WITH_SUFFIX = Pattern.compile("^[A-Z]{1,5}\\.(US|NQ|N)$", Pattern.CASE_INSENSITIVE);

    private final TickerSpec.Market defaultMarketForAlpha;

    public TickerResolver(String defaultMarketForAlpha) {
        this.defaultMarketForAlpha = parseMarket(defaultMarketForAlpha);
    }

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
