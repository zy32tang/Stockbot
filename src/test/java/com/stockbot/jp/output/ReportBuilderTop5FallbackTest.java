package com.stockbot.jp.output;

import com.stockbot.jp.config.Config;
import com.stockbot.jp.model.ScoredCandidate;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportBuilderTop5FallbackTest {

    @Test
    void top5ShouldShowExplanationAndRiskTop3WhenAllCandidatesAreRisk() {
        Config config = Config.fromConfigurationProperties(
                Path.of(".").toAbsolutePath().normalize(),
                Map.of(
                        "report", Map.of("lang", "en"),
                        "top5", Map.of("filter_risk", true, "count", 5)
                )
        );
        ReportBuilder builder = new ReportBuilder(config);

        String reasons = "{"
                + "\"tech\":{\"checklist\":["
                + "{\"status\":\"FAIL\",\"label\":\"Bias risk\",\"value\":\"bias=10.2%\",\"rule\":\"bias>8%\"},"
                + "{\"status\":\"FAIL\",\"label\":\"Execution stop\",\"value\":\"stop_pct=8.1%\",\"rule\":\">6%\"}"
                + "]}"
                + "}";

        ScoredCandidate c1 = new ScoredCandidate(
                "1111.T",
                "1111",
                "Alpha",
                "JP",
                91.0,
                100.0,
                reasons,
                "{\"last_close\":100.0,\"ma5\":98.0,\"ma10\":96.0,\"ma20\":95.0,\"bias\":0.102,\"vol_ratio\":1.0,\"stop_line\":91.9,\"stop_pct\":0.081,\"trend_strength\":91,\"signal_status\":\"DEFEND\",\"risk_level\":\"RISK\",\"data_status\":\"OK\",\"subscores\":{\"trend_structure\":45,\"bias_risk\":-18,\"volume_confirm\":6,\"execution_quality\":3}}"
        );
        ScoredCandidate c2 = new ScoredCandidate(
                "2222.T",
                "2222",
                "Beta",
                "JP",
                88.0,
                80.0,
                reasons,
                "{\"last_close\":80.0,\"ma5\":79.0,\"ma10\":78.0,\"ma20\":77.0,\"bias\":0.090,\"vol_ratio\":0.9,\"stop_line\":74.8,\"stop_pct\":0.065,\"trend_strength\":88,\"signal_status\":\"DEFEND\",\"risk_level\":\"RISK\",\"data_status\":\"OK\",\"subscores\":{\"trend_structure\":40,\"bias_risk\":-18,\"volume_confirm\":6,\"execution_quality\":3}}"
        );

        String html = builder.buildHtml(
                Instant.parse("2026-02-23T01:02:03Z"),
                ZoneId.of("Asia/Tokyo"),
                100,
                80,
                2,
                5,
                List.of(),
                List.of(c1, c2),
                ReportBuilder.RunType.CLOSE,
                null,
                false,
                "SUCCESS",
                null,
                Map.of(),
                null
        );

        assertTrue(html.contains("No qualified opportunities today"));
        assertTrue(html.contains("Risk Top3"));
        assertTrue(html.contains("1111 Alpha"));
        assertTrue(html.contains("Bias risk: bias=10.2%"));
    }
}
