package com.stockbot.jp.model;

import java.time.LocalDate;

/**
 * 模块说明：BarDaily（class）。
 * 主要职责：承载 model 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class BarDaily {
    public final String ticker;
    public final LocalDate tradeDate;
    public final double open;
    public final double high;
    public final double low;
    public final double close;
    public final double volume;

/**
 * 方法说明：BarDaily，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public BarDaily(String ticker, LocalDate tradeDate, double open, double high, double low, double close, double volume) {
        this.ticker = ticker;
        this.tradeDate = tradeDate;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
    }
}
