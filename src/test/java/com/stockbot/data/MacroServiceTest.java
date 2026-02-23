package com.stockbot.data;

import com.stockbot.model.DailyPrice;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MacroServiceTest {

    @Test
    void scoreMacroJPShouldFallbackAndCacheWhenMarketDataFails() {
        ThrowingMarketDataService market = new ThrowingMarketDataService();
        MacroService service = new MacroService(market);

        double first = service.scoreMacroJP();
        double second = service.scoreMacroJP();

        assertEquals(50.0, first, 1e-9);
        assertEquals(50.0, second, 1e-9);
        assertEquals(1, market.fetchLastTwoCalls);
        assertEquals(0, market.fetchHistoryCalls);
    }

    private static final class ThrowingMarketDataService extends MarketDataService {
        int fetchLastTwoCalls;
        int fetchHistoryCalls;

        private ThrowingMarketDataService() {
            super(null);
        }

        @Override
        public PricePair fetchLastTwoCloses(String ticker) {
            fetchLastTwoCalls++;
            throw new RuntimeException("HTTP 429");
        }

        @Override
        public List<DailyPrice> fetchDailyHistory(String ticker, String range, String interval) {
            fetchHistoryCalls++;
            throw new RuntimeException("HTTP 429");
        }
    }
}
