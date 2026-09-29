# 指数温度计盘中快照自动化设计

## 目标与范围

新增统一入口 `intraday_temperature.py`，同时支持一次性测试和未来正式盘中运行：

```text
python intraday_temperature.py --time 13:30 --mode test
python intraday_temperature.py --time 14:15 --mode scheduled
```

本轮只实现并执行一次即时测试。由于实际请求晚于严格的 13:30，测试记录使用：

- `requested_time = 13:30`
- `actual_snapshot_time =` 实际请求开始时间
- `fetched_at =` 同花顺响应时间或实际完成时间
- `formula_version = V1.1-INTRADAY-TEST`
- 输出目录 `output/intraday/test/`

本轮不创建 Windows Scheduled Task，不发送邮件、不推送消息、不自动打开浏览器。

## 数据边界

1. 运行时只从 `data/market.duckdb` 读取正式历史数据、注册表和当前成分历史。
2. 盘中实时输入只使用同花顺公开指数快照、指数成分和 A 股行情快照接口；不切换或迁移盘后历史主源。
3. `index_registry` 是指数范围唯一来源：`status = active` 进入盘中计算；`status = temperature_pending` 生成待启用记录但不计算正式温度、不参与排名；`disabled` 不处理。
4. 同花顺不能可靠取得的港股指数或成分股，保留页面和快照记录，所有依赖实时数据的字段为 `NA`，`temperature_intraday = NA`，`data_status = source_unavailable`。不使用本地收盘、上一交易日收盘或其他数据源替代。
5. 绝不写入 `index_daily`、`stock_daily`，也不更新已有 `indicator_daily` 的 V1.0/V1.1 行。

## 数据库结构

新增独立表 `intraday_snapshot`。表内保留用户要求的盘中指标、比较值、覆盖率、来源和状态，并增加测试审计所需的 `requested_time`、`actual_snapshot_time`、`today_intraday_amount`、`constituent_mode` 和 `failure_reason`。

唯一键为：

```text
market + index_code + snapshot_date + snapshot_time + formula_version
```

其中 `snapshot_time` 是命令请求时间标签，实际采集时间单独保存，避免把延迟采集误标为严格时点。写表前对数据库做备份并记录 SHA-256；写入使用事务和幂等插入。重复成功运行默认不再请求实时数据、不重复写库、不覆盖输出，`--force` 才允许重新执行。

## 盘中计算

盘中计算复用现有 `main.py` 的冻结百分位函数和 `validate_v11.py` 的 V1.1 四维组合逻辑，不修改正式函数和正式结果：

- `RET20_raw`：盘中指数价格除以 20 个交易日前正式收盘价减一。
- `BIAS20_raw`：盘中价格与此前 19 个正式收盘价组成临时 MA20，不把临时价格写入日线历史。
- `RS20_raw`：指数盘中 20 日表现与同一时间获取的沪深 300 盘中 20 日表现相减；沪深 300 实时数据不可用时不使用昨日收盘替代。
- `BreadthMA20_raw`：当前成分盘中价格与各自由当前价格和此前 19 个正式收盘组成的临时 MA20 比较。
- `HLBreadth_raw`：当前成分盘中价格与此前 20 个完整交易日正式收盘的最高、最低值比较。
- `Sync_raw`：此前 19 个完整交易日收益与当前盘中临时收益组成 20 行收益窗口，沿用正式模型的平均两两相关性计算。
- 盘中 raw 值追加到本地历史 raw 序列后，只在内存中调用既有 756 日窗口、252 个最小样本的百分位计算；不持久化为正式指标。
- `PriceMomentum_score = mean(RET20_score, BIAS20_score, RS20_score)`。
- `Volume_score` 直接沿用上一交易日正式 V1.1 的 `VolumeStrength_score`，并保存可取得的 `today_intraday_amount`；本轮不把累计成交额加入历史百分位。
- `Breadth_score = mean(BreadthMA20_score, HLBreadth_score)`，`Sync_score_v11 = Sync_score`。
- 四个 V1.1 维度均有效时计算 `temperature_intraday`，否则保持 `NA`。
- 覆盖率使用有效成分数除以总成分数；低于 0.80 时 BreadthMA20、HLBreadth、Sync 及对应 score 为 `NA`。

比较逻辑分开保存：

- 测试运行不寻找或伪造上一交易日同时间快照；`previous_snapshot_temperature = NA`。
- 正式运行按相同 `snapshot_time` 和正式公式版本查找上一交易日盘中快照。
- `previous_close_temperature` 来自上一交易日正式 V1.1；变化值均四舍五入到 1 位。

## 获取与失败隔离

同花顺调用使用 `HITHINK_FINANCE_API_KEY` 环境变量，不在代码、参数、日志或输出中写入密钥。指数请求按注册表逐指数隔离，A 股成分行情按已取得的当前成分合并后分批请求；任何一个指数、一个批次或整个港股市场失败，都只影响对应记录。

如果所有可参与的 active 指数都没有有效实时输入，运行摘要记录 `market_closed` 或统一源不可用的明确原因，不生成伪温度。单个失败时继续生成总页面。

## 输出与页面

测试输出保存到 `output/intraday/test/`，文件名包含日期和 `intraday_test` 标识；正式模式使用 `output/intraday/1415/` 和 `YYYY-MM-DD_1415_*` 命名。

HTML 从 `build_dashboard.py` 提取并复用现有内嵌 CSS、卡片、表格、温度色阶、响应式布局和交互样式，不引入 CDN、在线字体、服务器或新前端工程。页面只生成一个全部指数页面，包含：

- 标题 `2026-09-21 盘中测试快照` 和实际采集时间 `HH:MM:SS`；
- `盘中估算值，正式温度以收盘后 V1.1 为准` 提示；
- 温度色带、当前指数数量、active 数量、有效温度数量、最高/最低温、升温/降温摘要；
- active 指数按盘中温度从高到低排名；pending 指数单独显示为“待启用”，不进入排名；
- 每个指数的温度、等级、指数涨跌幅、同时间变化、收盘变化、PriceMomentum、Volume、Breadth、Sync、coverage_ratio 和数据状态；
- 首次测试的“暂无上一交易日同时间快照”说明，以及成交维度沿用上一交易日收盘值的说明；
- 页面底部的盘中估算、历史基准、本地数据库、成交维度和 `current_constituents_proxy` 限制说明。

CSV 保存快照明细；JSON 保存请求时间、实际采集时间、注册表统计、逐指数状态、覆盖率、温度、NA 原因、写入行数和正式数据零改动验证结果。

## 验证边界

执行前记录 `index_daily`、`stock_daily`、`indicator_daily` 的行数、最大日期和 V1.0/V1.1 稳定摘要；执行后重新核对。验证包括：

1. 注册指数总数、active 数量、pending 数量；
2. 实时行情成功数、`source_unavailable` 数量和温度成功数；
3. 每个指数的 coverage_ratio、盘中温度、相对上一交易日收盘变化和 NA 字段；
4. `intraday_snapshot` 新增行数和唯一键；
5. 正式日线表、V1.0/V1.1 结果是否零改动；
6. HTML UTF-8 解码、无外部资源、可离线打开和渲染检查；
7. 中文文本无替换字符或乱码。

本轮验证结束后停止，不创建正式 14:15 自动化，等待用户确认。
