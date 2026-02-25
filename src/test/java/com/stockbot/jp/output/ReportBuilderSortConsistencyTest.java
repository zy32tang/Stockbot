package com.stockbot.jp.output;

import com.stockbot.jp.config.Config;
import com.stockbot.jp.model.WatchlistAnalysis;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportBuilderSortConsistencyTest {

    @Test
    void watchlistRowsShouldSortByTechnicalScore() {
        Config config = Config.fromConfigurationProperties(
                Path.of(".").toAbsolutePath().normalize(),
                Map.of("report", Map.of("lang", "en"))
        );
        ReportBuilder builder = new ReportBuilder(config);

        WatchlistAnalysis highTotalLowTechnical = watchRow(
                "HIGH_TOTAL_LOW_TECH",
                95.0,
                10.0,
                "OBSERVE"
        );
        WatchlistAnalysis lowTotalHighTechnical = watchRow(
                "LOW_TOTAL_HIGH_TECH",
                5.0,
                90.0,
                "CANDIDATE"
        );

        String html = builder.buildHtml(
                Instant.parse("2026-02-23T01:02:03Z"),
                ZoneId.of("Asia/Tokyo"),
                100,
                80,
                10,
                5,
                List.of(highTotalLowTechnical, lowTotalHighTechnical),
                List.of(),
                ReportBuilder.RunType.CLOSE,
                null,
                false,
                "SUCCESS",
                null
        );

        int highTechIndex = html.indexOf("LOW_TOTAL_HIGH_TECH");
        int lowTechIndex = html.indexOf("HIGH_TOTAL_LOW_TECH");
        assertTrue(highTechIndex >= 0, "LOW_TOTAL_HIGH_TECH should exist in html");
        assertTrue(lowTechIndex >= 0, "HIGH_TOTAL_LOW_TECH should exist in html");
        assertTrue(highTechIndex < lowTechIndex, "watchlist rows should be ordered by technical score");
    }

    private WatchlistAnalysis watchRow(String name, double totalScore, double technicalScore, String technicalStatus) {
        return new WatchlistAnalysis(
                name,
                "1234",
                "1234.T",
                name,
                name,
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
                totalScore,
                "OBSERVE",
                "NONE",
                false,
                "not triggered",
                1,
                "rss->pgvector",
                "summary",
                List.of("digest"),
                technicalScore,
                technicalStatus,
                "{}",
                "{}",
                "{}",
                "",
                List.of()
        );
    }
}

