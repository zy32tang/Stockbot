package com.stockbot.jp.runner;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DailyRunnerScoreMappingTest {

    @Test
    void mapJpScoreToLegacyGateScore_shouldMatchExpectedValues() {
        assertEquals(-2.0, DailyRunner.mapJpScoreToLegacyGateScore(40.0), 1e-9);
        assertEquals(1.0, DailyRunner.mapJpScoreToLegacyGateScore(55.0), 1e-9);
    }

    @Test
    void mapJpScoreToLegacyGateScore_shouldClampBounds() {
        assertEquals(-10.0, DailyRunner.mapJpScoreToLegacyGateScore(0.0), 1e-9);
        assertEquals(10.0, DailyRunner.mapJpScoreToLegacyGateScore(100.0), 1e-9);
        assertEquals(-10.0, DailyRunner.mapJpScoreToLegacyGateScore(-200.0), 1e-9);
        assertEquals(10.0, DailyRunner.mapJpScoreToLegacyGateScore(999.0), 1e-9);
    }

    @Test
    void mapRatingFromTechnicalStatus_shouldMapToNormalizedStatus() {
        assertEquals("CANDIDATE", DailyRunner.mapRatingFromTechnicalStatus("candidate"));
        assertEquals("IN", DailyRunner.mapRatingFromTechnicalStatus(" in "));
        assertEquals("ERROR", DailyRunner.mapRatingFromTechnicalStatus(" ERROR "));
        assertEquals("NEAR", DailyRunner.mapRatingFromTechnicalStatus(""));
    }

    @Test
    void mapRiskFromTechnicalStatus_shouldMapToStatefulRisk() {
        assertEquals("RISK", DailyRunner.mapRiskFromTechnicalStatus("RISK"));
        assertEquals("ERROR", DailyRunner.mapRiskFromTechnicalStatus("error"));
        assertEquals("SKIPPED", DailyRunner.mapRiskFromTechnicalStatus(" skipped "));
        assertEquals("NONE", DailyRunner.mapRiskFromTechnicalStatus("CANDIDATE"));
        assertEquals("NONE", DailyRunner.mapRiskFromTechnicalStatus("OBSERVE"));
    }
}
