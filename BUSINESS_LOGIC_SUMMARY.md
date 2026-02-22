# StockBot 业务逻辑摘要（当前代码快照）

> 更新时间：2026-02-22  
> 代码范围：`src/main/java/com/stockbot/app/StockBotApplication.java` + `com.stockbot.jp.*`（watchlist 解释链仍调用 `com.stockbot.*` 旧模块）

## 1. 系统定位与主链路

当前主入口是 `com.stockbot.app.StockBotApplication`。默认运行路径：

1. 后台/调度触发 JP 全市场扫描（支持分段、断点恢复、`PARTIAL`）
2. 复用最新市场扫描候选，叠加 watchlist 分析（技术链 + 解释链）
3. 生成 HTML 报告（含 diagnostics/config 快照）
4. 按邮件配置发送（支持 `--novice` 文本模式、dry-run）

另外支持独立 `BACKTEST` 模式和 SQLite -> PostgreSQL 一次性迁移命令。

## 2. 入口分发（StockBotApplication）

### 2.1 CLI 参数

支持：

- `--help`
- `--reset-batch`
- `--test`
- `--novice`
- `--migrate-sqlite-to-postgres`
- `--sqlite-path <path>`

行为：

1. 参数解析失败 -> 打印帮助并返回 `2`
2. `--help` -> 返回 `0`
3. `--migrate-sqlite-to-postgres` -> 必须带 `--sqlite-path`，执行迁移后返回 `0`
4. 其他 -> 进入 `DAILY` / `BACKTEST` 分支

### 2.2 配置加载与 DB 优先级

`Config.load(workingDir)` 合并顺序：

1. classpath `config.properties`
2. 工作目录 `config.properties`（覆盖）
3. `Config.DEFAULTS`（缺省兜底）

DB 配置优先级（仅 url/user/pass）：

1. 环境变量 `STOCKBOT_DB_URL/STOCKBOT_DB_USER/STOCKBOT_DB_PASS`
2. 配置文件键 `db.url/db.user/db.pass`

当前 DB 后端是 PostgreSQL（`Database.dbType() == POSTGRES`），启动会打印脱敏后的 JDBC URL 与 schema。

### 2.3 mode 与 schedule 判定

- `app.mode` 仅允许 `DAILY` / `BACKTEST`
- `scheduleEnabled` 条件：
  - `! --test && !maintenance(reset-batch/migrate) && app.schedule.enabled=true`
  - 或 `! --test && !maintenance && 无参数 && mode=DAILY`

因此：`DAILY` 无参数时默认进入调度循环。

### 2.4 启动固定动作

进入业务分支前固定执行：

1. 安装 Log4j stdout/stderr 路由
2. 执行 `MigrationRunner`（建 schema/table/index，含 pgvector extension）
3. `RunDao.recoverDanglingRuns()` 将遗留 `RUNNING` 标记为 `ABORTED`

## 3. runOnce 分支

### 3.1 DAILY

- `--reset-batch`：清 batch checkpoint + 背景扫描启动 checkpoint 后退出
- `--test`：基于最近市场扫描重建报告并发送测试邮件（不重扫市场）
- 其他：执行一次 `runScheduledMergeReport(...)`

### 3.2 BACKTEST

`runBacktest(...)`：

1. `startRun("BACKTEST")`
2. `jp.backtest.BacktestRunner.run()`
3. 成功 -> `finishRun("SUCCESS", summary)`
4. 失败 -> `finishRun("FAILED", err)` 并抛出异常

### 3.3 SQLite 迁移

`--migrate-sqlite-to-postgres --sqlite-path <file>`：

- 执行 `SqliteToPostgresMigrator.migrate(...)`
- 输出 `watchlist_count/price_daily_count/signals_count/run_logs_count`
- 不进入 DAILY/BACKTEST 主链路

## 4. schedule 分支（runSchedule）

### 4.1 调度时间解析

读取顺序：

1. `schedule.times`（逗号分隔）
2. 若空 -> `schedule.time`
3. 都无效 -> 返回 `2`

### 4.2 并发模型

调度线程与后台扫描线程共用一把 `ReentrantLock`：

- 前台调度：`lockInterruptibly()`，保证报告任务串行
- 后台扫描：`tryLock()`，抢不到锁则 defer

### 4.3 后台扫描触发规则（当前实现）

后台线程始终启动（不读取 `app.background_scan.enabled`）：

1. 启动触发：若距离上次进程启动时间 `>= 8h` 执行一次
2. 运行中触发：之后每 `8h` 尝试一次（固定常量）
3. 每次后台扫描调用 `runMarketScanOnly(..., resetBatchCheckpoint=true)`

## 5. runScheduledMergeReport

流程：

1. 查最新 market scan run（`DAILY_MARKET_SCAN` / `DAILY`，且必须有 candidates）
2. 若不是“今天（按 `schedule.zone`）”的数据，先补跑 `runMarketScanOnly`
3. 读取 watchlist，执行 `runWatchlistReportFromLatestMarket`
4. 若 `email.enabled=true` 发送日报（`--novice` 会改成动作文本模式）

`needsFreshMarketScan=true` 条件：

- 没有可用 market run
- `run.startedAt` 为空
- `run.startedAt` 日期 != 当前日期（按 `schedule.zone`）

## 6. DailyRunner 主流程

### 6.1 run mode

- `DAILY`：全扫描 + watchlist + 报告
- `DAILY_MARKET_SCAN`：仅市场扫描
- `DAILY_REPORT`：复用最新市场扫描 + watchlist + 报告

主入口实际主要使用 `DAILY_MARKET_SCAN` + `DAILY_REPORT`。

### 6.2 runMarketScanOnly

1. `startRun("DAILY_MARKET_SCAN")`
2. `executeMarketScan(...)`
3. `insertCandidates(runId, topCandidates)`
4. `finishRun(status=SUCCESS|PARTIAL)`

### 6.3 runWatchlistReportFromLatestMarket

1. `startRun("DAILY_REPORT")`
2. 获取最新 market run（必须存在 candidates）
3. 读取该 run 的 topN candidates
4. 若 universe 为空，强制做一次 universe 更新
5. 分析 watchlist，生成报告
6. 将 market run candidates 复制写入当前 run
7. `finishRun("SUCCESS")`

## 7. 市场扫描细节

### 7.1 executeMarketScan

1. `JpxUniverseUpdater.updateIfNeeded(force)`
2. `prepareBatchPlan(...)` 生成 segment
3. 从 checkpoint 恢复 `nextSegmentIndex`
4. 按 `scan.batch.max_segments_per_run` 限制本次执行段数
5. 每段 `scanUniverse(...)`，每段后保存 checkpoint
6. 全段完成则清 checkpoint，否则 run 记为 `PARTIAL`

产物：

- `topCandidates`（`scan.top_n`）
- `marketReferenceCandidates`（`scan.market_reference_top_n`）

### 7.2 分段与断点恢复

`prepareBatchPlan(...)`：

1. `scan.batch.enabled=false` -> 单段 `ALL`
2. `scan.batch.segment_by_market=true` -> 按市场分段（可再按 chunk 切分）
3. 否则按固定 chunk（`market_chunk_size<=0` 时默认 500）

checkpoint 自动失效并重扫条件：

- checkpoint JSON 解析失败
- universe 签名变化
- segment 数变化
- topN 变化
- `nextSegmentIndex` 越界/已完成

### 7.3 scanUniverse（并发）

- 线程池：`scan.threads`
- 每 ticker 执行 `TickerTask(scanTicker)`
- 每段结束批量写 `scan_results`
- 统计覆盖率、数据源分布、失败分类、upsert 耗时

### 7.4 scanTicker（单票）

流程（市场扫描）：

1. 先看本地 cache bars
2. 若 `scan.cache.prefer_enabled=true` 且 cache 新鲜且形态可用 -> 直接使用 cache
3. 否则请求 Yahoo
4. Yahoo 失败时按 `scan.network.retry_when_cache_exists` 决定是否优先回退 cache
5. 最终无数据 -> failed

### 7.5 新鲜度与可交易闸门

- `isBarsFreshEnough`：按 `app.zone` 计算日龄，周末自动把 freshDays 下限抬到 2
- `isCacheFreshEnough`：在 fresh 基础上还需 `bars >= scan.min_history_bars`
- `isTradableAndLiquid`：
  - 价格 > 0 且 >= `scan.tradable.min_price`
  - 20日均量 >= `scan.tradable.min_avg_volume_20`
  - 20日零量天数 <= `scan.tradable.max_zero_volume_days_20`
  - 最近 flat 天数 <= `scan.tradable.max_flat_days`

## 8. 指标/过滤/风控/评分

### 8.1 IndicatorEngine

计算：

- SMA20/60/120、SMA60Prev5、SMA60Slope
- RSI14、ATR14、ATR%
- Bollinger 上中下轨
- drawdown120%、volatility20%、avgVolume20、volumeRatio20
- `pct_from_sma20/sma60`
- `return3d/5d/10d`
- `low_lookback/high_lookback`

### 8.2 CandidateFilter

通过条件：`hardPass && signalCount >= filter.min_signals`

hard 规则：

- `history_too_short`
- `price_out_of_range`
- `avg_volume_too_low`
- `drawdown_too_deep`
- `too_far_above_sma60`
- `short_term_drop_too_fast`（阈值来自 `filter.hard.max_drop_3d_pct`）

signal 规则：

- `pullback_detected`
- `rsi_rebound_zone`
- `price_near_or_below_sma20`
- `near_lower_bollinger`
- `short_term_rebound`
- `volume_support`

### 8.3 RiskFilter

基于 ATR%、波动率、回撤、量比：

- 超阈值累积 penalty
- 达到 fail multiplier 时直接 `pass=false`

### 8.4 JP ScoringEngine

6 因子加权后扣 risk penalty，clamp 到 `[0,100]`：

- `pullback/rsi/sma_gap/bollinger/rebound/volume`

候选门槛：`score >= scan.min_score`

## 9. Watchlist 合并分析

每个 watchlist 项合并两条链：

1. 技术链（JP 新引擎）：价格抓取 -> 指标 -> filter/risk/score
2. 解释链（旧模块）：`FactorEngine + legacy Scoring + GatePolicy + Ollama`

### 9.1 ticker 解析规则（TickerResolver）

- JP：`1234`、`1234.T`
- US：`NVDA.US`、`NVDA.NQ`、`NVDA.N`
- 纯字母（如 `NVDA`）只有在 `watchlist.default_market_for_alpha=US` 时自动按 US 处理
- 非法格式 -> `SYMBOL_ERROR`

`watchlist.non_jp_handling`：

- `PROCESS_SEPARATELY`：继续处理非 JP
- 其他值：非 JP 直接标记 `SKIPPED`

### 9.2 watchlist 技术状态映射

- `ERROR`：抓取/指标等失败
- `OBSERVE`：filter 不过或分数低于阈值
- `RISK`：risk 不过
- `CANDIDATE`：全部通过

### 9.3 watchlist 价格抓取路径

`fetchWatchPriceTrace(...)` 固定顺序：

1. Yahoo（watchlist bars）
2. 失败后回退本地 cache
3. 仍失败 -> `fetch_failed`

### 9.4 旧链路 AI 触发

前提：`ai.enabled=true`

- 若 `ai.watchlist.mode=ALL`：全量触发 AI
- 否则走 `GatePolicy.shouldRunAi`，满足任一：
  - `totalScore <= watchlist.ai.score_threshold`
  - `newsCount >= watchlist.ai.news_min`
  - `pctChange <= watchlist.ai.drop_pct_threshold`

AI 输出只用于解释文案，不参与技术分数。

### 9.5 价格异常标记

同一 `trade_date + lastClose` 若跨 ticker 重复数量 `>= watchlist.price.duplicate_min_count`：

- 打 `WARN` 日志
- 相关 watch 行标记 `priceSuspect=true`

## 10. Polymarket 信号链

### 10.1 总开关与模式

- `polymarket.enabled=false` -> 返回 disabled
- `polymarket.impact.mode`：
  - `rule` -> `PolymarketSignalProviderRule`
  - 其他 -> `PolymarketSignalProviderVector`

### 10.2 PolymarketClient

- 请求 `GET {baseUrl}/markets?active=true&closed=false&limit=...`
- 支持 `polymarket.apiKey` header
- 内存缓存 TTL：`polymarket.refresh.ttl_hours`
- 最多保留 `polymarket.refresh.max_markets`

### 10.3 rule provider（关键词）

- 关键词重叠 + 流动性打分
- `topK` 来自 `polymarket.vector.top_k`（fallback `polymarket.top_n`）
- 只输出分数 > 0 且能映射 watchlist impact 的市场

### 10.4 vector provider（当前默认）

优先使用持久化 docs 检索：

1. 将市场 upsert 为 `docs`（`doc_type=POLYMARKET_MARKET`）
2. 对每个 watch item 做向量检索
3. 计算综合分：`wSim*similarity + wRecency + wLiquidity + wConfidence`
4. similarity 需 >= `polymarket.vector.min_similarity`
5. 取 `polymarket.vector.top_k`

异常或 docs backend 不可用时回退内存相似度模式（同一套打分参数）。

## 11. 报告生成（ReportBuilder）

输出文件：`jp_daily_yyyyMMdd_HHmmss.html`

`RunType`：

- `startedAt < 15:00 (JST)` -> `INTRADAY`
- 否则 `CLOSE`

页面内容（当前实现）：

- 顶部指标 + 系统状态
- novice 决策卡 + 行动建议
- watchlist 表与 AI 摘要
- Top 卡片（候选+交易计划）
- Polymarket
- 免责声明
- Config snapshot（折叠）
- Diagnostics（折叠）

### 11.1 Top5 选择闸门

前置闸门：

1. `report.top5.skip_on_partial=true` 且 `marketScanPartial=true` 时，若覆盖率 `< report.top5.allow_partial_when_coverage_ge` 则跳过
2. `fetchCoveragePct < report.top5.min_fetch_coverage_pct` 则跳过

逐条筛选：

1. `score >= scan.min_score`
2. 排除衍生/杠杆类关键词（ETF/REIT/INVERSE/LEVERAGE...）
3. 必须有风险输入指标
4. `CLOSE` 模式必须有有效 `TradePlan`（`INTRADAY` 不强制）

`report.top5.min_indicator_coverage_pct` 在 `DailyRunner` 中主要用于 Top5 可用性提示/日志，不直接作为 `buildTopSelection` 的硬跳过条件。

### 11.2 风险评级与交易计划

风险评级依据：`risk.minVolume`、`risk.volMax`、趋势关系（`lastClose/sma60/sma60Prev5`）。

交易计划由 `TradePlanBuilder` 生成：

- 入场区间：`lastClose ± plan.entry.buffer_pct`
- 止损：`low_lookback*(1-stop.loss.bufferPct)` 与 `entry-ATR*plan.stop.atr_mult` 取可用值
- 止盈：`max(sma20, high_lookback*plan.target.high_lookback_mult, rr.min)` 约束
- 必须满足价位顺序与 `rr >= max(plan.rr.min_floor, rr.min)`

## 12. 邮件发送

### 12.1 正式日报

`sendDailyMailIfNeeded(...)` 仅在 `email.enabled=true` 时发送。

- 正常模式：发送 HTML（会去掉趋势图 img），趋势图作为附件
- `--novice`：只发文本动作摘要，不带 HTML 与附件

### 12.2 测试邮件（`--test`）

- 强制 `settings.enabled=true`
- 基于最新 market scan 重建报告；失败返回 `2`
- 主题含 `run_id`

### 12.3 附件与 dry-run/fail-fast

发送前处理：

1. 从报告 HTML 收集本地 `<img src>`
2. 附加同时间戳 `trends/*_<ts>.png`
3. 从正文移除趋势图标签

`Mailer` 额外行为：

- `mail.dry_run=true`：写 `.eml/.html/.txt` 到 `mail.dry_run.dir`，不真正投递
- `mail.fail_fast=true`：SMTP 异常直接抛出；否则仅 WARN 并继续

SMTP 使用 STARTTLS。

## 13. 数据库与持久化

`MigrationRunner` 在 PostgreSQL `stockbot` schema 下维护：

- `metadata`
- `watchlist`
- `price_daily`
- `indicators_daily`
- `signals`
- `run_logs`
- `docs`（`VECTOR(1536)`）
- `universe`
- `runs`
- `candidates`
- `scan_results`

run 状态：

- `RUNNING`
- `SUCCESS`
- `PARTIAL`
- `FAILED`
- `ABORTED`（启动恢复）

扫描覆盖率优先从 `scan_results` 汇总；无数据时回退 `in-memory stats` 或 `runs/candidates` 推导。

## 14. 回测逻辑（jp.backtest.BacktestRunner）

输入：

- 最近 `backtest.lookback_runs` 个 `mode=DAILY && status=SUCCESS` 的 run

每个 run 取 TopK：

- 入场：`runDate` 后第 `0` 个交易日 close
- 出场：`runDate` 后第 `hold_days` 个交易日 close

输出：

- `runCount`
- `sampleCount`
- `avgReturnPct`
- `medianReturnPct`
- `winRatePct`

## 15. 当前关键配置（以 `src/main/resources/config.properties` 为准）

说明：工作目录 `config.properties` 会覆盖以下值。

- `app.mode=DAILY`
- `app.schedule.enabled=false`（但 DAILY 无参数仍会进入 schedule）
- `schedule.times=11:30,15:00`
- `scan.top_n=15`
- `scan.market_reference_top_n=5`
- `scan.batch.enabled=true`
- `scan.batch.segment_by_market=false`
- `scan.batch.market_chunk_size=150`
- `scan.batch.max_segments_per_run=0`
- `scan.progress.log_every=25`
- `report.top5.skip_on_partial=true`
- `report.top5.min_fetch_coverage_pct=80`
- `report.top5.min_indicator_coverage_pct=80`
- `report.top5.allow_partial_when_coverage_ge=101`
- `report.coverage.use_tradable_denominator=true`
- `watchlist.default_market_for_alpha=US`
- `watchlist.non_jp_handling`（默认在 defaults 为 `PROCESS_SEPARATELY`）
- `vector.memory.enabled=true`
- `vector.memory.signal.top_k=10`
- `polymarket.enabled=true`
- `polymarket.impact.mode=vector`
- `polymarket.vector.top_k=5`
- `polymarket.vector.min_similarity=0.20`
- `mail.dry_run=false`
- `mail.fail_fast=false`

## 16. 当前注意点（按现代码）

1. `app.background_scan.enabled` 与 `app.background_scan.interval_sec` 当前未参与后台扫描调度逻辑（后台固定 8h 节奏）。
2. `DAILY` 且无参数会进入无限 schedule 循环，不是单次执行。
3. `--test` 依赖“可用 market scan + candidates + 报告可重建”，否则返回 `2`。
4. `scan.upsert.incremental_overlap_days` 未进入当前 upsert 调用链（实际使用 `scan.upsert.initial_days` / `scan.upsert.incremental_recent_days`）。
5. `app.top_n_override`、`app.reset_batch` 仅在配置定义，主入口未读取。
6. `RunDao.findLatestDailyRunWithReport()` 与 `ReportBuilder.buildMailText()` 当前未被入口链路调用。
7. 部分 polymarket 配置键目前未被业务链使用（如 `polymarket.data_base_url`、`polymarket.keywords`、`polymarket.search_limit`、`polymarket.topic_map_path`、`polymarket.watch_impact_limit`）。

## 17. Docker 常用命令

1. 启动数据库：
   - `docker compose up -d postgres`
2. 改了 Java 代码后跑测试（重建镜像）：
   - `docker compose run --rm --build app --test`
3. 只改 `watchlist.txt` / `config.properties`（无需重建）：
   - `docker compose run --rm app --test`
4. 常驻运行 app（新代码）：
   - `docker compose up -d --build app`
5. 停止：
   - `docker compose down`

PowerShell 常用一行：

- `docker compose up -d postgres; docker compose run --rm --build app --test`

---

如后续改动入口分发、后台扫描调度、Top5 筛选、polymarket provider、邮件发送或 DB schema，建议优先同步本文件第 2/4/10/11/12/13/16 节。
