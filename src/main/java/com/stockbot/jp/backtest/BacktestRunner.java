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

/**
 * 模块说明：BacktestRunner（class）。
 * 主要职责：承载 backtest 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class BacktestRunner {
    private final Config config;
    private final RunDao runDao;
    private final BarDailyDao barDailyDao;

/**
 * 方法说明：BacktestRunner，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public BacktestRunner(Config config, RunDao runDao, BarDailyDao barDailyDao) {
        this.config = config;
        this.runDao = runDao;
        this.barDailyDao = barDailyDao;
    }

/**
 * 方法说明：run，负责执行核心流程并返回执行结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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

/**
 * 方法说明：toSummaryText，负责转换数据结构用于后续处理。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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

/**
 * 方法说明：round2，负责执行业务逻辑并产出结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    private double round2(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
}
