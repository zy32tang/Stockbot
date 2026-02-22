package com.stockbot.jp.polymarket;

import com.stockbot.jp.model.WatchItem;

import java.time.Instant;
import java.util.List;

public interface PolymarketSignalProvider {
    PolymarketSignalReport collectSignals(List<WatchItem> watchlist, Instant now);
}
