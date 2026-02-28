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
    void watchlistTableShouldRenderRealReasonsAndTradePlan() {
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
                + "\"sma20\":100.0,"
                + "\"low_lookback\":95.0,"
                + "\"high_lookback\":120.0,"
                + "\"atr14\":2.0,"
                + "\"volatility20_pct\":20.0,"
                + "\"volume_ratio20\":1.2"
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
                "CANDIDATE",
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

        assertTrue(html.contains("pullback_detected"));
        assertTrue(html.contains("99.50 ~ 100.50"));
        assertTrue(html.contains("93.10"));
        assertTrue(html.contains("117.60"));
    }

    @Test
    void watchlistTableShouldRenderPlanFailureReasonInPlanCells() {
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
                "OBSERVE",
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

        assertTrue(html.contains("-(missing_watchlist_inputs)"));
    }
}
