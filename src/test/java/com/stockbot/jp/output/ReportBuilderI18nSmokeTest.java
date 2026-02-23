package com.stockbot.jp.output;

import com.stockbot.jp.config.Config;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportBuilderI18nSmokeTest {

    @Test
    void buildHtml_shouldRenderChineseLabelsWhenReportLangIsZhCn() {
        Config config = Config.fromConfigurationProperties(
                Path.of(".").toAbsolutePath().normalize(),
                Map.of("report", Map.of("lang", "zh_CN"))
        );
        ReportBuilder builder = new ReportBuilder(config);

        String html = builder.buildHtml(
                Instant.parse("2026-02-23T01:02:03Z"),
                ZoneId.of("Asia/Tokyo"),
                100,
                80,
                10,
                5,
                List.of(),
                List.of(),
                ReportBuilder.RunType.CLOSE,
                null,
                false,
                "SUCCESS",
                null
        );

        assertTrue(html.contains("StockBot \u65E5\u80A1\u6BCF\u65E5\u62A5\u544A")
                || html.contains("StockBot \u65E5\u62A5"));
        assertTrue(html.contains("\u8FD0\u884C\u65F6\u95F4"));
    }
}
