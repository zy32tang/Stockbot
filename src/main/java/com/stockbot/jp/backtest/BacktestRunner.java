package com.stockbot.jp.backtest;

import com.stockbot.jp.config.Config;
import com.stockbot.jp.db.BarDailyDao;
import com.stockbot.jp.db.RunDao;
import com.stockbot.jp.model.BacktestReport;
import com.stockbot.jp.model.CandidateRow;
import com.stockbot.jp.model.RunRow;

import java.sql.SQLException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.OptionalDouble;

public final class BacktestRunner {
    private final Config config;
    private final RunDao runDao;
    private final BarDailyDao barDailyDao;

    public BacktestRunner(Config config, RunDao runDao, BarDailyDao barDailyDao) {
        this.config = config;
        this.runDao = runDao;
        this.barDailyDao = barDailyDao;
    }

    public BacktestReport run() throws SQLException {
        int lookbackRuns = Math.max(1, config.getInt("backtest.lookback_runs", 30));
        int topK = Math.max(1, config.getInt("backtest.top_k", 5));
        int holdDays = Math.max(1, config.getInt("backtest.hold_days", 10));
        ZoneId zoneId = ZoneId.of(config.getString("app.zone", "Asia/Tokyo"));

        List<RunRow> runs = runDao.listSuccessfulDailyRuns(lookbackRuns);
        List<Double> returns = new ArrayList<>();
        int usedRuns = 0;

        for (RunRow run : runs) {
            if (run.startedAt == null) {
                continue;
            }
            LocalDate runDate = LocalDate.ofInstant(run.startedAt, zoneId);
            List<CandidateRow> picks = runDao.listCandidates(run.id, topK);
            if (picks.isEmpty()) {
                continue;
            }
            usedRuns++;
            for (CandidateRow pick : picks) {
                OptionalDouble entry = barDailyDao.closeOnOrAfterWithOffset(pick.ticker, runDate, 0);
                OptionalDouble exit = barDailyDao.closeOnOrAfterWithOffset(pick.ticker, runDate, holdDays);
                if (entry.isPresent() && exit.isPresent() && entry.getAsDouble() > 0.0) {
                    double ret = (exit.getAsDouble() - entry.getAsDouble()) / entry.getAsDouble() * 100.0;
                    returns.add(ret);
                }
            }
        }

        if (returns.isEmpty()) {
            return new BacktestReport(usedRuns, 0, 0.0, 0.0, 0.0);
        }

        Collections.sort(returns);
        double sum = 0.0;
        int wins = 0;
        for (double value : returns) {
            sum += value;
            if (value > 0.0) {
                wins++;
            }
        }
        double avg = sum / returns.size();
        double median;
        if (returns.size() % 2 == 0) {
            int right = returns.size() / 2;
            int left = right - 1;
            median = (returns.get(left) + returns.get(right)) / 2.0;
        } else {
            median = returns.get(returns.size() / 2);
        }
        double winRate = wins * 100.0 / returns.size();
        return new BacktestReport(usedRuns, returns.size(), round2(avg), round2(median), round2(winRate));
    }

    public String toSummaryText(BacktestReport report) {
        return String.format(
                Locale.US,
                "BACKTEST runs=%d samples=%d avg=%.2f%% median=%.2f%% win_rate=%.2f%%",
                report.runCount,
                report.sampleCount,
                report.avgReturnPct,
                report.medianReturnPct,
                report.winRatePct
        );
    }

    private double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
