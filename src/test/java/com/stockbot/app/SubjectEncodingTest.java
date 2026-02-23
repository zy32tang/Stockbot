package com.stockbot.app;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertTrue;

class SubjectEncodingTest {

    @Test
    void buildTestDailyReportSubject_shouldContainChineseKeyword() {
        String subject = StockBotApplication.buildTestDailyReportSubject(
                "[StockBot JP]",
                Instant.parse("2026-02-23T01:02:03Z"),
                ZoneId.of("Asia/Tokyo"),
                5,
                123L
        );

        assertTrue(subject.contains("\u65E5\u80A1\u65E5\u62A5"));
    }
}
