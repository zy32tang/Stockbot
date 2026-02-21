# StockBot 业务逻辑摘要（当前代码快照）

> 更新时间：2026-02-18  
> 代码范围：`src/main/java/com/stockbot/app/StockBotApplication.java` + `com.stockbot.jp.*`（watchlist 解释链会调用 `com.stockbot.*` 旧模块）

## 1. 系统定位与主链路

当前主入口为 `com.stockbot.app.StockBotApplication`，核心运行链路是：

1. JP 全市场扫描（支持分段、断点恢复、部分完成）
2. 复用最近一次市场扫描结果，叠加 watchlist 分析
3. 生成 HTML 日报，按配置发送邮件
4. 支持 `BACKTEST` 独立模式

说明：仓库中旧包（如 `com.stockbot.output.*`、`com.stockbot.backtest.*`）仍存在，但当前主入口不直接调用。

## 2. 入口分发（StockBotApplication）

### 2.1 CLI 参数

支持参数：

- `--help`
- `--reset-batch`
- `--test`

分支：

1. 参数解析失败 -> 打印帮助并返回 `2`
2. `--help` -> 打印帮助并返回 `0`
3. 其他 -> 进入配置加载与业务分发

### 2.2 配置加载优先级

`Config.load(workingDir)` 顺序：

1. 类路径 `src/main/resources/config.properties`
2. 工作目录 `config.properties`（本地覆盖）
3. `Config.DEFAULTS`（键缺失或空值时兜底）

本地运行请在工作目录放置 `config.properties` 覆盖资源配置；该文件不提交 Git。

当前仓库根目录 `config.properties` 为空覆盖文件，实际主要由 resources 配置生效。

### 2.3 mode 与 schedule 判定

- `app.mode` 仅允许 `DAILY` / `BACKTEST`，否则返回 `2`
- `scheduleEnabled` 实际条件：
  - `! --test && app.schedule.enabled=true`
  - 或 `! --test && 无 CLI 参数 && mode=DAILY`

因此：`DAILY` 且无参数启动会默认进入 schedule 循环。

### 2.4 启动固定动作

无论分支如何，都会先做：

1. 初始化 Log4j 路由（stdout/stderr 重定向）
2. 打开 SQLite 并执行 migration
3. `RunDao.recoverDanglingRuns()`：遗留 `RUNNING` run 标记为 `ABORTED`

然后分叉：

- `scheduleEnabled=true` -> `runSchedule(...)`
- 否则 -> `runOnce(...)`

## 3. runOnce 分支

### 3.1 DAILY

- `--reset-batch`：删除 batch checkpoint 与背景扫描 startup trigger checkpoint 后退出
- `--test`：基于最近市场扫描结果重建报告并强制发送测试邮件（不重跑全市场）
- 其他：执行一次 `runScheduledMergeReport(...)`

### 3.2 BACKTEST

`runBacktest(...)`：

1. `startRun("BACKTEST")`
2. `jp.backtest.BacktestRunner.run()`
3. 成功 -> `finishRun(..., "SUCCESS", summary)`
4. 失败 -> `finishRun(..., "FAILED", err)` 并抛出异常

## 4. schedule 分支（runSchedule）

### 4.1 调度时间解析

读取顺序：

1. `schedule.times`（逗号分隔）
2. 若空 -> 回退 `schedule.time`
3. 都无效 -> 返回 `2`

### 4.2 并发模型

调度启动后并行两条链路，使用同一把 `ReentrantLock`：

1. 后台扫描线程：周期跑 `runMarketScanOnly(...)`
2. 前台调度线程：到点跑 `runScheduledMergeReport(...)`

关键点：

- 后台线程 `tryLock()`，拿不到锁直接跳过本轮
- 前台线程 `lockInterruptibly()`，确保触发任务串行
- 后台间隔：`max(30, app.background_scan.interval_sec)`
- sleep 被中断返回 `130`

### 4.3 runScheduledMergeReport

流程：

1. 查最新 market scan run（`DAILY_MARKET_SCAN` 或 `DAILY`，且必须已有 candidates）
2. 若不是“今天（按 `schedule.zone`）”的数据，先补跑 `runMarketScanOnly`
3. 读取 watchlist
4. 执行 `runWatchlistReportFromLatestMarket`
5. 若 `email.enabled=true` 发送正式日报

`needsFreshMarketScan` 为 true 的条件：

- 没有 market run
- run.startedAt 为空
- run.startedAt 对应日期不是今天（按 `schedule.zone`）

## 5. DailyRunner 主流程

`DailyRunner` 定义 3 种 run mode：

- `DAILY`：全扫描 + watchlist + 报告
- `DAILY_MARKET_SCAN`：仅市场扫描
- `DAILY_REPORT`：复用最近市场扫描 + watchlist + 报告

当前主入口主要使用后两者。

### 5.1 runMarketScanOnly

1. `startRun("DAILY_MARKET_SCAN")`
2. `executeMarketScan(...)`
3. `insertCandidates(runId, topCandidates)`
4. `finishRun(status=SUCCESS|PARTIAL)`

### 5.2 runWatchlistReportFromLatestMarket

1. `startRun("DAILY_REPORT")`
2. 查最近 market run（必须有 candidates）
3. 取该 run 的 topN 与 market reference topN
4. 若 universe 为空，强制触发一次 JPX 更新
5. 分析 watchlist
6. 生成报告
7. 将 market run 的 candidates 复制写入本次 run
8. `finishRun(status=SUCCESS)`

### 5.3 executeMarketScan（核心）

#### 5.3.1 Universe 更新

`JpxUniverseUpdater.updateIfNeeded(force)`：

1. `force=true` 或 `jpx.universe.force_update=true` -> 强制更新
2. 否则若本地 active universe 存在且 `last_sync` 未超过 `refresh_days` -> 跳过更新
3. 否则下载并解析 JPX（CSV/Excel）

解析后若 0 记录直接抛异常。

#### 5.3.2 分段计划

`prepareBatchPlan(...)`：

1. `scan.batch.enabled=false` -> 单段 `ALL`
2. `scan.batch.enabled=true && scan.batch.segment_by_market=true` -> 按市场分段，可按 `market_chunk_size` 再拆分
3. `scan.batch.enabled=true && segment_by_market=false` -> 固定 chunk（`market_chunk_size<=0` 时使用 500）

#### 5.3.3 断点恢复

checkpoint 存在时，以下情况会清 checkpoint 并从头开始：

- checkpoint JSON 解析失败
- universe 签名不一致
- segment 数不一致
- topN 变化
- `nextSegmentIndex` 越界/已完成

#### 5.3.4 单次执行段数

`scan.batch.max_segments_per_run`：

- `<=0`：本次跑完剩余全部段
- `>0`：本次最多跑 N 段

执行结束：

- 全段完成 -> 清 checkpoint，run 可 `SUCCESS`
- 未完成 -> 保留 checkpoint，run 标记 `PARTIAL`

最终产出：

- `topCandidates`（topN）
- `marketReferenceCandidates`（`scan.market_reference_top_n`）

## 6. 全市场扫描细节

### 6.1 scanUniverse（并发）

- 线程池大小：`scan.threads`
- 每个 ticker 提交 `TickerTask`
- 统计 `scanned / failed / candidateCount`
- 记录 download/parse/upsert 耗时、数据源分布、失败分类
- 每段结束批量写入 `scan_results`（`ScanResultDao.insertBatch`）

关键分支：

1. `result.error != null` -> `failed++`
2. 有 bars 且 `dataSource=yahoo` -> `upsertBarsIncremental(...)`
3. `result.candidate != null` -> 候选计数并维护 topN
4. 进度日志：完成时必打；否则按 `scan.progress.log_every`

### 6.2 scanTicker（单票）

实际顺序：

1. 读取缓存 bars
2. 若 `scan.cache.prefer_enabled=true` 且缓存足够新鲜 -> 直接走 cache
3. 否则先抓 Yahoo（日线 2y）
4. Yahoo 无数据则回退到 cache（标记 requestFailed）
5. 无缓存且抓取失败 -> failed

retry 规则：

- 若已有缓存且 `scan.network.retry_when_cache_exists=false` -> 直接回退 cache
- 若 `scan.network.retry_when_cache_exists=true` -> 仍优先尝试 Yahoo

### 6.3 新鲜度与可交易性闸门

`isBarsFreshEnough` / `isCacheFreshEnough`：

1. bars 数量满足要求（cache 需 `>= scan.min_history_bars`）
2. 最新 trade_date 距今天 <= freshDays
3. 周末会把 freshDays 下限抬到 2

`isTradableAndLiquid`（先于指标）：

- last close 有效且 >0
- close >= `scan.tradable.min_price`
- 20日均量 >= `scan.tradable.min_avg_volume_20`
- 20日零成交天数 <= `scan.tradable.max_zero_volume_days_20`
- 最近 `flat_lookback_days` 平线K 数 <= `scan.tradable.max_flat_days`

## 7. 指标、筛选、风控、评分

### 7.1 IndicatorEngine

计算项包含：

- SMA20/60/120、SMA60 前5日、SMA60 slope
- RSI14、ATR14、ATR%
- Bollinger 上/中/下轨
- 120日回撤、20日年化波动、20日均量、量比
- 距 SMA20/SMA60 偏离
- 3/5/10 日收益
- stop-loss lookback 高低点

### 7.2 CandidateFilter

通过条件：`hardPass && signalCount >= filter.min_signals`

hard 条件（任一失败即 false）：

- 历史长度不足
- 价格超区间
- 均量不足
- 回撤过深
- 距 SMA60 过远
- 3日跌幅过快（固定 `return3dPct < -8`）

signal 条件（计数）：

- pullback_detected
- rsi_rebound_zone
- price_near_or_below_sma20
- near_lower_bollinger
- short_term_rebound
- volume_support

### 7.3 RiskFilter

检查 ATR%、波动率、回撤绝对值、量比：

- 超阈值会加 penalty
- 严重超阈值会直接 `pass=false`

### 7.4 JP ScoringEngine

6 因子加权后扣 risk penalty，再 clamp 到 `[0,100]`：

- pullback
- rsi
- sma_gap
- bollinger
- rebound
- volume

候选需满足：`score >= scan.min_score`。

## 8. Watchlist 合并分析

每个 watchlist 项会合并两条链路：

1. 技术链（JP 新引擎）：指标 + 过滤 + 风控 + 评分
2. 解释链（旧模块）：基本面/行业/宏观/新闻评分 + AI 摘要

### 8.1 watchlist 解析

支持：`8306.T`、`8306.jp`、`8306`。

解析顺序：

1. ticker 精确匹配 universe
2. code 匹配
3. 规范化 ticker 匹配
4. 仍失败则构造 `WATCHLIST` 临时记录

### 8.2 technicalStatus 映射

- `ERROR`：技术扫描异常
- `OBSERVE`：filter 未通过或分数低于阈值
- `RISK`：risk 未通过
- `CANDIDATE`：全部通过

### 8.3 旧模块评分与 AI 触发

旧链路：

1. `FactorEngine` 计算 `fundamental/industry/macro/news`
2. `com.stockbot.scoring.ScoringEngine` 产出 `totalScore/rating/risk`
3. `GatePolicy.shouldRunAi` 满足任一条件触发 AI：
   - `totalScore <= watchlist.ai.score_threshold`
   - `newsCount >= watchlist.ai.news_min`
   - `pctChange <= watchlist.ai.drop_pct_threshold`
4. 触发后调用 `OllamaClient.summarize(...)`

AI 不参与技术候选分数，仅用于解释文本。

### 8.4 价格异常标记

watchlist 行会做重复价格检测：同一 trade_date + lastClose 出现在多个 ticker 且达到 `watchlist.price.duplicate_min_count` 时，标记 `priceSuspect` 并告警。

## 9. 报告生成（ReportBuilder）

输出文件：`jp_daily_yyyyMMdd_HHmmss.html`

`RunType` 判定：

- `startedAt < 15:00`（JST）-> `INTRADAY`
- 否则 -> `CLOSE`

页面主要区块：A 头部指标、B 系统说明、C 行动建议、D watchlist 表、E Top5、F Polymarket、G 免责声明。

### 9.1 Top5 选择与跳过规则

来源：`marketReferenceCandidates`（先按 score 降序）。

先决跳过条件：

- `report.top5.skip_on_partial=true` 且 market scan 为 `PARTIAL`
- `FETCH_COVERAGE < report.top5.min_fetch_coverage_pct`

逐条过滤：

1. score 必须 `>= scan.min_score`
2. 剔除衍生/杠反类（ETF/REIT/INVERSE/LEVERAGE 等关键词）
3. 必须能解析出风险输入指标
4. `CLOSE` 模式下交易计划必须有效（`INTRADAY` 可不强制）

### 9.2 风险评级与交易计划

风险评级（报告层规则）：

- 依据 `risk.minVolume`、`risk.volMax`、`lastClose/sma60/sma60Prev5`
- 产出 `LOW/MID/HIGH`，缺数据会抬升到 `MID`

交易计划：

- 入场：`lastClose ±0.5%`
- 止损：`lowLookback*(1-buffer)` 与 `entry-1.5*ATR` 取可用值
- 止盈：至少满足 `rr.min`
- 若价位顺序或 RR 校验失败 -> 计划无效

### 9.3 行动建议

`actionAdvice` 根据 `FETCH_COVERAGE`、`INDICATOR_COVERAGE`、候选数、Top5 平均分给出 `观望/谨慎/小仓试错/正常`。

## 10. 邮件发送

### 10.1 正式日报

`sendDailyMailIfNeeded` 仅在 `email.enabled=true` 时发送。

### 10.2 测试邮件（`--test`）

- 强制启用发送（`settings.enabled=true`）
- 尝试基于最新 market scan 重建日报；失败则返回 `2`
- 主题包含 run_id

### 10.3 附件与 HTML 处理

发送前会：

1. 从报告 HTML 收集本地 `<img src>` 文件
2. 追加匹配报告时间戳的 `trends/*_<ts>.png`
3. 移除正文中的趋势图标签（趋势图走附件）

SMTP 使用 STARTTLS（Jakarta Mail）。

## 11. 数据库与持久化

`MigrationRunner` 当前维护表：

- `metadata`：元数据状态（如 universe last_sync、batch checkpoint）
- `universe`：股票池
- `bars_daily`：日线 OHLCV
- `runs`：任务运行记录
- `candidates`：run 级候选明细
- `scan_results`：逐 ticker 扫描结果（覆盖率/失败原因/数据源/延迟等）

run 状态集合：

- `RUNNING`
- `SUCCESS`
- `PARTIAL`
- `FAILED`
- `ABORTED`（启动恢复）

## 12. 回测逻辑（jp.backtest.BacktestRunner）

输入：最近 `backtest.lookback_runs` 个 `mode=DAILY 且 status=SUCCESS` 的 run。

每个 run 取 TopK：

- 入场：`runDate` 当日及之后第 0 个交易日 close
- 出场：`runDate` 当日及之后第 `hold_days` 个交易日 close

输出统计：

- `runCount`
- `sampleCount`
- `avgReturnPct`
- `medianReturnPct`
- `winRatePct`

## 13. 当前生效配置（resources + 空 local override）

关键值（节选）：

- `app.mode=DAILY`
- `app.schedule.enabled=false`（但 DAILY 无参启动仍进入 schedule）
- `app.background_scan.interval_sec=86400`
- `schedule.times=11:30,15:00`
- `scan.threads=3`
- `scan.top_n=15`
- `scan.market_reference_top_n=5`
- `scan.batch.enabled=true`
- `scan.batch.segment_by_market=false`
- `scan.batch.market_chunk_size=150`
- `scan.progress.log_every=25`
- `report.top5.skip_on_partial=true`
- `report.top5.min_fetch_coverage_pct=80`
- `watchlist.news.sources=google,bing,yahoo`
- `watchlist.news.query_variants=2`
- `polymarket.enabled=false`

## 14. 当前注意点（按现代码）

1. `app.background_scan.enabled` 当前未参与分支控制，schedule 下后台扫描线程总会启动。
2. `DAILY` 且无参数会进入无限循环调度，不会只跑一次。
3. `--test` 依赖“可用 market scan + candidates + 报告可重建”，否则返回错误。
4. `scan.upsert.incremental_overlap_days` 当前未进入 upsert 调用链。
5. `app.top_n_override`、`app.reset_batch` 仅在配置里，主入口未读取。
6. `RunDao.findLatestDailyRunWithReport()` 与 `ReportBuilder.buildMailText()` 当前未被入口链路调用。

## 15. Docker 快捷操作（改代码后）

目标：尽量避免每次都 `docker compose down`。

1. 首次或需要数据库时：
   - `docker compose up -d postgres`
2. 改了 Java 代码后，跑一次测试（自动重建 app 镜像）：
   - `docker compose run --rm --build --no-deps app --test`
3. 只改 `watchlist.txt` / `config.properties`（已挂载，不必重建）：
   - `docker compose run --rm --no-deps app --test`
4. 需要让 app 常驻并使用新代码：
   - `docker compose up -d --build app`
5. 需要彻底停止时：
   - `docker compose down`

PowerShell 一行（常用）：
- `docker compose up -d postgres; docker compose run --rm --build --no-deps app --test`

### 15.1 按参数快捷指令

先确保数据库已启动：

- `docker compose up -d postgres`

改了 Java 代码后（需要重建）：

- `docker compose run --rm --build app --help`
- `docker compose run --rm --build app --test`
- `docker compose run --rm --build app --reset-batch`
- `docker compose run --rm --build app --migrate-sqlite-to-postgres --sqlite-path outputs/stockbot.db`

只改 `watchlist.txt` / `config.properties`（无需重建）：

- `docker compose run --rm  app --help`
- `docker compose run --rm  app --test`
- `docker compose run --rm ps app --reset-batch`
- `docker compose run --rm app --migrate-sqlite-to-postgres --sqlite-path outputs/stockbot.db`

---

如后续改动入口分发、分段恢复、Top5 选择、邮件附件处理或 DB 表结构，建议优先同步本文件第 2/5/9/10/11/14 节。

本地运行请在工作目录放置 `config.properties` 覆盖资源配置；该文件不提交 Git。
