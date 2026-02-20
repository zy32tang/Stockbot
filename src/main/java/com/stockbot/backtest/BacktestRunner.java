package com.stockbot.backtest;

import com.stockbot.storage.Db;

/**
 * 模块说明：BacktestRunner（class）。
 * 主要职责：承载 backtest 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public class BacktestRunner {
    private final Db db;

/**
 * 方法说明：BacktestRunner，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public BacktestRunner(Db db) {
        this.db = db;
    }

/**
 * 方法说明：runSummary，负责执行核心流程并返回执行结果。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
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
