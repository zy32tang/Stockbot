package com.stockbot.jp.model;

/**
 * 模块说明：UniverseRecord（class）。
 * 主要职责：承载 model 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public final class UniverseRecord {
    public final String ticker;
    public final String code;
    public final String name;
    public final String market;

/**
 * 方法说明：UniverseRecord，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public UniverseRecord(String ticker, String code, String name, String market) {
        this.ticker = ticker;
        this.code = code;
        this.name = name;
        this.market = market;
    }
}
