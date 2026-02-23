# StockBot Business Logic Summary

> Last updated: 2026-02-23  
> Scope: `src/main/java/com/stockbot/app/StockBotApplication.java` + `com.stockbot.jp.*`

## 1. 本次更新结论（重点）

本次是“高收益最小改动”更新，**只增强运行控制、可观测性与测试保障**，不改业务计算逻辑。

### 1.1 明确未改动的业务逻辑

以下逻辑保持不变：

- 评分公式（JP `ScoringEngine`）
- Top5 筛选规则与门控
- action 判定（BUY/WATCH/AVOID/WAIT）
- 指标计算（`IndicatorEngine`）
- AI 触发规则本身（`GatePolicy` 判定条件）

### 1.2 新增能力（不改变业务结果）

- 运行模式：`ONCE` / `DAEMON`
- 结构化运行遥测：`RunTelemetry`
- 模块状态结构化输出：`ModuleStatus` / `ModuleResult`
- 报告与邮件追加 `Run Summary`
- Golden Tests + 最小 CI（仅 `mvn -q test`）

## 2. 运行入口与模式

入口仍为 `StockBotApplication`，新增 CLI 参数：

- `--once`：执行一次后退出（默认行为）
- `--daemon`：按 Quartz 调度持续运行
- `--trigger=manual|cron`：运行触发来源（默认 `manual`）
- `--max-runs=N`：DAEMON 安全阀，最多运行 N 次后退出
- `--max-runtime-min=M`：DAEMON 安全阀，最长运行 M 分钟后退出

约束：

- `--once` 与 `--daemon` 互斥
- `--daemon` 仅允许 `app.mode=DAILY`
- DAEMON 启动会打印明确提示（包含 trigger 与安全阀参数）

## 3. RunTelemetry（运行摘要）

新增：`src/main/java/com/stockbot/core/RunTelemetry.java`

核心接口：

- `startStep(name)`
- `endStep(name, itemsIn, itemsOut, errorCount[, optionalNote])`
- `getSummary()`

固定步骤名：

- `NEWS_FETCH`
- `TEXT_CLEAN`
- `EMBED`
- `VECTOR_SEARCH`
- `AI_SUMMARY`
- `HTML_RENDER`
- `MAIL_SEND`
- `MARKET_FETCH`
- `INDICATORS`

每步记录字段：

- `elapsed_ms`
- `items_in`
- `items_out`
- `error_count`
- `optional_note`

Run Summary 包含：

- `run_id`
- `run_mode=ONCE|DAEMON`
- `trigger=manual|cron`
- `started_at / finished_at / total_elapsed_ms`
- `ai_used + ai_reason`
- `news_items_raw / news_items_dedup / clusters`
- `errors_total`
- `steps`（逐行）

## 4. 模块状态结构化输出

新增：

- `src/main/java/com/stockbot/core/ModuleStatus.java`
- `src/main/java/com/stockbot/core/ModuleResult.java`

结构：

- `ModuleStatus { OK, DISABLED, INSUFFICIENT_DATA, ERROR }`
- `ModuleResult { status, reason, evidence }`

汇总模块（主流程末端生成）：

- `indicators`
- `top5`
- `news`
- `ai`
- `mail`

说明：

- 只在已有分支基础上补充状态与证据
- 不改变原有业务分支判断结果

## 5. 报告与邮件输出

### 5.1 HTML

`ReportBuilder` 与模板新增：

- “模块状态（点击展开）”区块
- “Run Summary”区块（`<pre>`）

watchlist 中原本 `- / n/a` 的展示保持不变；当模块非 OK 时，展示提示：

- `- (原因见模块状态)`

### 5.2 邮件

- 邮件正文 HTML 末尾追加 Run Summary（`<pre>`）
- novice 纯文本邮件也附带简要 Run Summary 文本
- `MAIL_SEND` 步骤纳入遥测，失败不会静默

## 6. 遥测接入点

### 6.1 新闻流水线（`WatchlistNewsPipeline`）

已接入步骤：

- `NEWS_FETCH`
- `EMBED`
- `VECTOR_SEARCH`
- `TEXT_CLEAN`
- `AI_SUMMARY`

并回填新闻统计：

- raw / dedup / clusters

### 6.2 日常运行（`DailyRunner`）

已接入步骤：

- `MARKET_FETCH`
- `INDICATORS`
- `HTML_RENDER`

并回填 AI 使用状态：

- `ai_used`
- `ai_reason`

## 7. 测试与 CI

新增测试：

- `src/test/java/com/stockbot/core/RunTelemetryTest.java`
  - 校验 summary 字段完整性（`run_id/run_mode/trigger/total_elapsed_ms/steps`）
- `src/test/java/com/stockbot/jp/output/ReportBuilderGoldenTest.java`
  - 校验 HTML 关键区块存在（标题、Run Summary、模块状态、watchlist 关键列）

CI：

- `.github/workflows/ci.yml`
- 触发：`push` / `pull_request`
- 执行：`mvn -q test`

## 8. 当前运行契约（与业务层解耦）

### 8.1 ONCE

- 单次执行
- 正常结束返回 `0`
- 失败会在 Run Summary 体现错误计数与步骤错误

### 8.2 DAEMON

- Quartz 调度持续执行
- 启动日志明确显示 DAEMON 模式
- 命中安全阀（`max-runs`/`max-runtime-min`）后安全退出

## 9. 快速验证建议

本地最小验证：

1. `.\mvnw.cmd -q test`
2. `--once` 运行并确认进程退出
3. `--daemon --max-runs=1 --trigger=cron` 验证 DAEMON 启动提示与安全阀退出
4. 检查报告 HTML 与邮件内容末尾是否含 `Run Summary`

