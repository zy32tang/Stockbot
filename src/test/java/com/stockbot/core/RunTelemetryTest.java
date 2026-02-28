package com.stockbot.core;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RunTelemetryTest {

    @Test
    void summaryShouldContainRequiredFields() {
        RunTelemetry telemetry = new RunTelemetry(123L, "ONCE", "manual", Instant.parse("2026-02-23T00:00:00Z"));
        telemetry.startStep(RunTelemetry.STEP_NEWS_FETCH);
        telemetry.endStep(RunTelemetry.STEP_NEWS_FETCH, 10, 7, 1);
        telemetry.startStep(RunTelemetry.STEP_HTML_RENDER);
        telemetry.endStep(RunTelemetry.STEP_HTML_RENDER, 1, 1, 0);
        telemetry.finish();

        String summary = telemetry.getSummary();

        assertTrue(summary.contains("run_id=123"));
        assertTrue(summary.contains("execution_mode=ONCE"));
        assertTrue(summary.contains("run_mode=ONCE"));
        assertTrue(summary.contains("trigger=manual"));
        assertTrue(summary.contains("total_elapsed_ms="));
        assertTrue(summary.contains("steps:"));
        assertTrue(summary.contains(RunTelemetry.STEP_NEWS_FETCH));
    }
}
