package com.stockbot.backtest;

import com.stockbot.storage.Db;

public class BacktestRunner {
    private final Db db;

    public BacktestRunner(Db db) {
        this.db = db;
    }

    public String runSummary(int daysLookback) {
        if (db == null) {
            return "Backtest unavailable: DB is not initialized.";
        }
        try {
            return db.simpleBacktestSummary(daysLookback);
        } catch (Exception e) {
            return "Backtest failed: " + e.getMessage();
        }
    }
}