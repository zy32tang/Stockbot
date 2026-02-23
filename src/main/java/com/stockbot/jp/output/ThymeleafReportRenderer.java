package com.stockbot.jp.output;

import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;
import org.thymeleaf.templatemode.TemplateMode;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;

import java.util.Locale;
import java.util.Map;

/**
 * Renders report HTML using Thymeleaf templates from classpath.
 */
public final class ThymeleafReportRenderer {
    private final TemplateEngine templateEngine;

    public ThymeleafReportRenderer() {
        ClassLoaderTemplateResolver resolver = new ClassLoaderTemplateResolver();
        resolver.setPrefix("templates/");
        resolver.setSuffix(".html");
        resolver.setTemplateMode(TemplateMode.HTML);
        resolver.setCharacterEncoding("UTF-8");
        resolver.setCacheable(false);

        this.templateEngine = new TemplateEngine();
        this.templateEngine.setTemplateResolver(resolver);
    }

    public String renderDailyReport(Map<String, Object> variables) {
        Context context = new Context(Locale.ROOT);
        if (variables != null && !variables.isEmpty()) {
            variables.forEach(context::setVariable);
        }
        return templateEngine.process("jp/daily_report", context);
    }
}
