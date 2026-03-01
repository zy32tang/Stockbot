package com.stockbot.jp.output;

import com.stockbot.core.ModuleResult;
import com.stockbot.core.RunTelemetry;
import com.stockbot.core.diagnostics.Diagnostics;
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
                "NEAR",
                "{\"tech\":{\"checklist\":[{\"status\":\"WATCH\",\"label\":\"Trend\",\"value\":\"MA5>MA10\",\"rule\":\"structure\"}]}}",
                "{\"last_close\":7900.0,\"ma5\":7880.0,\"ma10\":7820.0,\"ma20\":7700.0,\"bias\":0.0102,\"vol_ratio\":1.23,\"stop_line\":7600.0,\"stop_pct\":0.038,\"trend_strength\":62,\"signal_status\":\"NEUTRAL\",\"risk_level\":\"NEAR\",\"data_status\":\"OK\",\"subscores\":{\"trend_structure\":28,\"bias_risk\":8,\"volume_confirm\":14,\"execution_quality\":12}}",
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
                "{\"filter_reasons\":[\"bias too high\"],\"risk_reasons\":[\"stop too wide\"],\"score_reasons\":[\"low exec\"]}",
                "{\"last_close\":8000.0,\"ma5\":7900.0,\"ma10\":7800.0,\"ma20\":7700.0,\"bias\":0.0125,\"vol_ratio\":1.4,\"stop_line\":7600.0,\"stop_pct\":0.0500,\"trend_strength\":88,\"signal_status\":\"BULL\",\"risk_level\":\"IN\",\"data_status\":\"OK\",\"subscores\":{\"trend_structure\":45,\"bias_risk\":8,\"volume_confirm\":14,\"execution_quality\":8}}"
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

        Diagnostics diagnostics = new Diagnostics(456L, "DAILY_REPORT");

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
                diagnostics,
                moduleResults,
                telemetry
        );

        assertTrue(html.contains("StockBot"));
        assertTrue(html.contains("Run Summary"));
        assertTrue(html.contains("Module status (expand)"));
        assertTrue(html.contains("execution_mode=ONCE"));
        assertTrue(html.contains("run_mode=ONCE"));
        assertTrue(html.contains("business_run_mode=DAILY_REPORT"));
        assertTrue(html.contains("data_granularity=1d"));
        assertTrue(html.contains("fetch_missing=20"));
        assertTrue(html.contains("Tech 62/100"));
        assertTrue(html.contains("Price"));
        assertTrue(html.contains("StopLine"));
        assertTrue(html.contains("Risk Top3") || html.contains("#1 7974 Nintendo"));
    }
}
