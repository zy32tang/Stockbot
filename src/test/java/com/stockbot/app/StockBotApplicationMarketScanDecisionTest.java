package com.stockbot.app;

import com.stockbot.jp.model.RunRow;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StockBotApplicationMarketScanDecisionTest {

    @Test
    void evaluateMarketScanDecision_shouldRequireScanWhenNoPreviousRun() {
        ZoneId zone = ZoneId.of("Asia/Tokyo");
        StockBotApplication.MarketScanDecision decision =
                StockBotApplication.evaluateMarketScanDecision(Optional.empty(), zone);

        assertTrue(decision.needScan);
        assertEquals("NO_PREVIOUS_MARKET_SCAN", decision.reasonCode);
        assertEquals(zone, decision.zone);
        assertNull(decision.latestRunId);
        assertNull(decision.latestRunStartedAt);
        assertNull(decision.latestRunLocalDate);
    }

    @Test
    void evaluateMarketScanDecision_shouldRequireScanWhenStartedAtMissing() {
        ZoneId zone = ZoneId.of("Asia/Tokyo");
        RunRow run = RunRow.builder()
                .id(11L)
                .mode("MARKET_SCAN")
                .startedAt(null)
                .status("SUCCESS")
                .build();

        StockBotApplication.MarketScanDecision decision =
                StockBotApplication.evaluateMarketScanDecision(Optional.of(run), zone);

        assertTrue(decision.needScan);
        assertEquals("LATEST_RUN_MISSING_STARTED_AT", decision.reasonCode);
        assertEquals(11L, decision.latestRunId);
        assertNull(decision.latestRunStartedAt);
        assertNull(decision.latestRunLocalDate);
    }

    @Test
    void evaluateMarketScanDecision_shouldRequireScanWhenLatestRunNotToday() {
        ZoneId zone = ZoneId.of("Asia/Tokyo");
        Instant yesterdayStart = ZonedDateTime.now(zone)
                .minusDays(1)
                .withHour(10)
                .withMinute(0)
                .withSecond(0)
                .withNano(0)
                .toInstant();
        RunRow run = RunRow.builder()
                .id(12L)
                .mode("MARKET_SCAN")
                .startedAt(yesterdayStart)
                .status("SUCCESS")
                .build();

        StockBotApplication.MarketScanDecision decision =
                StockBotApplication.evaluateMarketScanDecision(Optional.of(run), zone);

        assertTrue(decision.needScan);
        assertEquals("LATEST_RUN_NOT_TODAY", decision.reasonCode);
        assertEquals(LocalDate.now(zone).minusDays(1), decision.latestRunLocalDate);
        assertTrue(decision.toLogLine().contains("MARKET_SCAN_DECISION"));
    }

    @Test
    void evaluateMarketScanDecision_shouldSkipScanWhenLatestRunAlreadyToday() {
        ZoneId zone = ZoneId.of("Asia/Tokyo");
        LocalDate today = LocalDate.now(zone);
        Instant todayNoon = today.atStartOfDay(zone).plusHours(12).toInstant();
        RunRow run = RunRow.builder()
                .id(13L)
                .mode("MARKET_SCAN")
                .startedAt(todayNoon)
                .status("SUCCESS")
                .build();

        StockBotApplication.MarketScanDecision decision =
                StockBotApplication.evaluateMarketScanDecision(Optional.of(run), zone);

        assertFalse(decision.needScan);
        assertEquals("LATEST_RUN_ALREADY_TODAY", decision.reasonCode);
        assertEquals(today, decision.latestRunLocalDate);
        assertTrue(decision.toLogLine().contains("reason=LATEST_RUN_ALREADY_TODAY"));
    }
}

