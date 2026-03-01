package com.stockbot.jp.tech;

import com.stockbot.jp.model.BarDaily;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TechScoreEngineTest {

    private final TechScoreEngine engine = new TechScoreEngine();

    @Test
    void evaluate_shouldReturnOkWithFullWindow() {
        List<BarDaily> bars = buildBars(25, 100.0, 1.2, 1_200_000.0, true);
        TechScoreResult result = engine.evaluate("7203.jp", "Toyota", bars);

        assertEquals(DataStatus.OK, result.dataStatus);
        assertTrue(result.trendStrength >= 0 && result.trendStrength <= 100);
        assertTrue(result.ma20 > 0.0);
        assertTrue(result.volRatio > 0.0);
        assertTrue(result.stopLine > 0.0);
        assertTrue(result.stopPct >= 0.0);
        assertNotNull(result.subscores);
        assertNotNull(result.signalStatus);
        assertNotNull(result.riskLevel);
        assertFalse(result.checklist.isEmpty());
    }

    @Test
    void evaluate_shouldReturnDegradedWhenWindowInsufficient() {
        List<BarDaily> bars = buildBars(8, 100.0, 0.8, 800_000.0, true);
        TechScoreResult result = engine.evaluate("6758.jp", "Sony", bars);

        assertEquals(DataStatus.DEGRADED, result.dataStatus);
        assertTrue(result.trendStrength >= 0 && result.trendStrength <= 100);
        assertFalse(result.checklist.isEmpty());
        assertTrue(result.checklist.stream().anyMatch(i -> i.label.contains("Data window")));
    }

    @Test
    void evaluate_shouldReturnMissingWhenBarsTooShort() {
        List<BarDaily> bars = buildBars(4, 100.0, 0.5, 700_000.0, true);
        TechScoreResult result = engine.evaluate("9984.jp", "SoftBank", bars);

        assertEquals(DataStatus.MISSING, result.dataStatus);
        assertEquals(0, result.trendStrength);
        assertEquals(RiskLevel.RISK, result.riskLevel);
        assertEquals(SignalStatus.DEFEND, result.signalStatus);
    }

    @Test
    void evaluate_shouldReturnMissingWhenVolumeMissing() {
        List<BarDaily> bars = buildBars(20, 100.0, 0.6, 0.0, false);
        TechScoreResult result = engine.evaluate("9432.jp", "NTT", bars);

        assertEquals(DataStatus.MISSING, result.dataStatus);
        assertEquals(0, result.trendStrength);
        assertEquals(RiskLevel.RISK, result.riskLevel);
        assertEquals(SignalStatus.DEFEND, result.signalStatus);
    }

    @Test
    void evaluate_shouldExposeStableSchemaAndEnums() {
        List<BarDaily> bars = buildBars(20, 100.0, 0.6, 900_000.0, true);
        TechScoreResult result = engine.evaluate("8306.jp", "MUFG", bars);

        assertNotNull(result.ticker);
        assertNotNull(result.companyName);
        assertNotNull(result.subscores);
        assertNotNull(result.checklist);
        assertTrue(result.subscores.trendStructure >= 0 && result.subscores.trendStructure <= 50);
        assertTrue(result.subscores.biasRisk >= -20 && result.subscores.biasRisk <= 10);
        assertTrue(result.subscores.volumeConfirm >= 0 && result.subscores.volumeConfirm <= 25);
        assertTrue(result.subscores.executionQuality >= 0 && result.subscores.executionQuality <= 15);
        assertTrue(Set.of("IN", "NEAR", "RISK").contains(result.riskLevel.name()));
        assertTrue(Set.of("BULL", "NEUTRAL", "BEAR", "DEFEND").contains(result.signalStatus.name()));
        assertTrue(Set.of("OK", "DEGRADED", "MISSING").contains(result.dataStatus.name()));
    }

    private List<BarDaily> buildBars(int count, double startClose, double step, double volume, boolean withVolume) {
        List<BarDaily> bars = new ArrayList<>();
        LocalDate start = LocalDate.of(2026, 1, 1);
        double close = startClose;
        for (int i = 0; i < count; i++) {
            close += step;
            double low = close - 1.5;
            double high = close + 1.5;
            double vol = withVolume ? volume + i * 1000 : volume;
            bars.add(new BarDaily(
                    "t",
                    start.plusDays(i),
                    close - 0.5,
                    high,
                    low,
                    close,
                    vol
            ));
        }
        return bars;
    }
}
