package com.stockbot.model;

import java.time.ZonedDateTime;

/**
 * 模块说明：NewsItem（class）。
 * 主要职责：承载 model 模块 的关键逻辑，对外提供可复用的调用入口。
 * 使用建议：修改该类型时应同步关注上下游调用，避免影响整体流程稳定性。
 */
public class NewsItem {
    public final String title;
    public final String description;
    public final String link;
    public final String source;
    public final ZonedDateTime publishedAt;

/**
 * 方法说明：NewsItem，负责初始化对象并装配依赖参数。
 * 处理流程：会结合入参与当前上下文执行业务逻辑，并返回结果或更新内部状态。
 * 维护提示：调整此方法时建议同步检查调用方、异常分支与日志输出。
 */
    public NewsItem(String title, String link, String source, ZonedDateTime publishedAt) {
        this(title, "", link, source, publishedAt);
    }

    public NewsItem(String title, String description, String link, String source, ZonedDateTime publishedAt) {
        this.title = title;
        this.description = description;
        this.link = link;
        this.source = source;
        this.publishedAt = publishedAt;
    }
}
