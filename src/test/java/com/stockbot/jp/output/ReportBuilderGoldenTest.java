package com.stockbot.jp.output;

import com.stockbot.core.ModuleResult;
import com.stockbot.core.RunTelemetry;
import com.stockbot.jp.config.Config;
import com.stockbot.jp.model.ScoredCandidate;
import com.stockbot.jp.model.WatchlistAnalysis;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportBuilderGoldenTest {

    @Test
    void buildHtmlShouldContainGoldenSections() {
        Config config = Config.fromConfigurationProperties(
                Path.of(".").toAbsolutePath().normalize(),
                Map.of("report", Map.of("lang", "en"))
        );
        ReportBuilder builder = new ReportBuilder(config);

        WatchlistAnalysis watchRow = new WatchlistAnalysis(
                "7974",
                "7974",
                "7974.T",
                "Nintendo",
                "Nintendo",
                "Technology",
                "Technology",
                "JP",
                "OK",
                "7974.T",
                8000.0,
                7900.0,
                1.2,
                "yahoo",
                "2026-02-23",
                18,
                false,
                120L,
                true,
                false,
                false,
                65.0,
                "B",
                "MID",
                false,
                "gate",
                1,
                "rss->pgvector",
                "No major cluster",
                List.of("digest"),
                62.0,
                "OBSERVE",
                "{}",
                "{}",
                "{}",
                "",
                List.of()
        );

        ScoredCandidate candidate = new ScoredCandidate(
                "7974.T",
                "7974",
                "Nintendo",
                "JP",
                88.3,
                8000.0,
                "{}",
                "{}"
        );

        Map<String, ModuleResult> moduleResults = new LinkedHashMap<>();
        moduleResults.put("indicators", ModuleResult.insufficient(
                "need 60 bars but got 18",
                Map.of("need_bars", 60, "got_bars", 18, "symbol", "7974.T")
        ));
        moduleResults.put("top5", ModuleResult.ok("candidate_count=1"));
        moduleResults.put("news", ModuleResult.ok("news_items=1"));
        moduleResults.put("ai", ModuleResult.disabled("ai.enabled=false", Map.of("ai_enabled", false)));

        RunTelemetry telemetry = new RunTelemetry(456L, "ONCE", "manual", Instant.parse("2026-02-23T01:02:03Z"));
        telemetry.startStep(RunTelemetry.STEP_HTML_RENDER);
        telemetry.endStep(RunTelemetry.STEP_HTML_RENDER, 1, 1, 0);
        telemetry.finish();

        String html = builder.buildHtml(
                Instant.parse("2026-02-23T01:02:03Z"),
                ZoneId.of("Asia/Tokyo"),
                100,
                80,
                10,
                5,
                List.of(watchRow),
                List.of(candidate),
                ReportBuilder.RunType.CLOSE,
                null,
                false,
                "SUCCESS",
                null,
                moduleResults,
                telemetry
        );

        assertTrue(html.contains("StockBot"));
        assertTrue(html.contains("Run Summary"));
        assertTrue(html.contains("模块状态"));
        assertTrue(html.contains("Name"));
        assertTrue(html.contains("Last"));
        assertTrue(html.contains("Action"));
    }
}
