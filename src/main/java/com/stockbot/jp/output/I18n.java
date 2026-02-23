package com.stockbot.jp.output;

import com.stockbot.jp.config.Config;

import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

public final class I18n {
    private static final String BASE_NAME = "i18n.messages";
    private static final Locale DEFAULT_LOCALE = Locale.forLanguageTag("zh-CN");

    private final ResourceBundle primaryBundle;
    private final ResourceBundle fallbackBundle;

    public I18n(Config config) {
        Locale locale = parseLocale(config == null ? null : config.getString("report.lang", "zh_CN"));
        this.primaryBundle = loadBundle(locale);
        this.fallbackBundle = loadBundle(DEFAULT_LOCALE);
    }

    public String t(String key, String fallback) {
        if (key == null || key.trim().isEmpty()) {
            return fallback == null ? "" : fallback;
        }
        String value = lookup(primaryBundle, key);
        if (value != null) {
            return value;
        }
        value = lookup(fallbackBundle, key);
        if (value != null) {
            return value;
        }
        return fallback == null ? "" : fallback;
    }

    private static String lookup(ResourceBundle bundle, String key) {
        if (bundle == null || key == null || key.trim().isEmpty()) {
            return null;
        }
        try {
            if (bundle.containsKey(key)) {
                return bundle.getString(key);
            }
        } catch (MissingResourceException ignored) {
            return null;
        }
        return null;
    }

    private static ResourceBundle loadBundle(Locale locale) {
        try {
            return ResourceBundle.getBundle(BASE_NAME, locale == null ? DEFAULT_LOCALE : locale);
        } catch (MissingResourceException ignored) {
            return null;
        }
    }

    private static Locale parseLocale(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return DEFAULT_LOCALE;
        }
        String normalized = raw.trim().replace('-', '_');
        String[] parts = normalized.split("_");
        if (parts.length >= 2) {
            return new Locale(parts[0], parts[1]);
        }
        Locale parsed = Locale.forLanguageTag(parts[0]);
        return parsed.getLanguage().isEmpty() ? DEFAULT_LOCALE : parsed;
    }
}
