package com.stockbot.jp.output;

import com.stockbot.core.ModuleResult;
import com.stockbot.jp.config.Config;
import com.stockbot.jp.model.WatchlistAnalysis;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportBuilderWatchlistPlanTest {

    @Test
    void watchlistCardShouldRenderChecklistAndRiskLine() {
        Config config = Config.fromConfigurationProperties(
                Path.of(".").toAbsolutePath().normalize(),
                Map.of("report", Map.of("lang", "en"))
        );
        ReportBuilder builder = new ReportBuilder(config);

        String technicalReasonsJson = "{"
                + "\"filter_reasons\":[\"pullback_detected\"],"
                + "\"risk_reasons\":[\"vol_ok\"],"
                + "\"score_reasons\":[\"score_ok\"],"
                + "\"cause_code\":\"NONE\","
                + "\"details\":{}"
                + "}";
        String technicalIndicatorsJson = "{"
                + "\"last_close\":100.0,"
                + "\"ma5\":99.5,"
                + "\"ma10\":98.8,"
                + "\"ma20\":97.5,"
                + "\"bias\":0.005,"
                + "\"vol_ratio\":1.2,"
                + "\"stop_line\":96.0,"
                + "\"stop_pct\":0.0400,"
                + "\"trend_strength\":70,"
                + "\"signal_status\":\"BULL\","
                + "\"risk_level\":\"IN\","
                + "\"data_status\":\"OK\","
                + "\"subscores\":{\"trend_structure\":45,\"bias_risk\":8,\"volume_confirm\":14,\"execution_quality\":13}"
                + "}";

        WatchlistAnalysis watchRow = new WatchlistAnalysis(
                "1234",
                "1234",
                "1234.T",
                "Demo Corp",
                "Demo Corp",
                "Tech",
                "Tech",
                "JP",
                "OK",
                "1234.T",
                100.0,
                99.0,
                1.0,
                "yahoo",
                "2026-02-23",
                250,
                false,
                100L,
                true,
                true,
                false,
                70.0,
                "B",
                "MID",
                true,
                "",
                3,
                "rss->pgvector",
                "summary",
                List.of("digest"),
                70.0,
                "IN",
                technicalReasonsJson,
                technicalIndicatorsJson,
                "{}",
                "",
                List.of()
        );

        Map<String, ModuleResult> moduleResults = new LinkedHashMap<>();
        moduleResults.put("indicators", ModuleResult.ok("indicator_ready=1/1"));
        moduleResults.put("top5", ModuleResult.ok("candidate_count=0"));
        moduleResults.put("news", ModuleResult.ok("news_items=1"));
        moduleResults.put("ai", ModuleResult.ok("triggered_count=1"));

        String html = builder.buildHtml(
                Instant.parse("2026-02-23T01:02:03Z"),
                ZoneId.of("Asia/Tokyo"),
                100,
                80,
                10,
                5,
                List.of(watchRow),
                List.of(),
                ReportBuilder.RunType.CLOSE,
                null,
                false,
                "SUCCESS",
                null,
                moduleResults,
                null
        );

        assertTrue(html.contains("Demo Corp (1234.T)"));
        assertTrue(html.contains("Tech 70/100"));
        assertTrue(html.contains("pullback_detected"));
        assertTrue(html.contains("96.00"));
        assertTrue(html.contains("4.00%"));
    }

    @Test
    void watchlistCardShouldFallbackWhenChecklistMissing() {
        Config config = Config.fromConfigurationProperties(
                Path.of(".").toAbsolutePath().normalize(),
                Map.of("report", Map.of("lang", "en"))
        );
        ReportBuilder builder = new ReportBuilder(config);

        WatchlistAnalysis watchRow = new WatchlistAnalysis(
                "AAPL",
                "AAPL",
                "AAPL.US",
                "Apple",
                "Apple",
                "Tech",
                "Tech",
                "US",
                "OK",
                "AAPL.US",
                200.0,
                199.0,
                0.5,
                "yahoo",
                "2026-02-27",
                250,
                false,
                80L,
                true,
                true,
                false,
                40.0,
                "OBSERVE",
                "MID",
                false,
                "",
                0,
                "rss->pgvector",
                "",
                List.of(),
                40.0,
                "NEAR",
                "{}",
                "{}",
                "{}",
                "",
                List.of()
        );

        String html = builder.buildHtml(
                Instant.parse("2026-02-27T06:00:00Z"),
                ZoneId.of("Asia/Tokyo"),
                100,
                80,
                10,
                5,
                List.of(watchRow),
                List.of(),
                ReportBuilder.RunType.CLOSE,
                null,
                false,
                "SUCCESS",
                null,
                Map.of(),
                null
        );

        assertTrue(html.contains("Checklist unavailable, fallback to data status"));
        assertTrue(html.contains("MISSING"));
    }
}
