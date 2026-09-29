# 指数温度计项目执行规范

本文件是本项目后续 Codex 任务的项目级执行规范。开始任何数据获取、更新、计算或迁移前，先阅读相关脚本、`data/` 缓存与 `data/market.duckdb` 的实际状态。用户本轮的明确要求优先；未经用户授权，不扩大任务范围。

## 核心原则与执行前检查

**已经可靠取得的数据优先复用，不因新增数据源、重新运行任务或重构而重复获取、覆盖或重建。**所有数据任务按 `LOCAL FIRST → PRIMARY SOURCE → FALLBACK SOURCE` 执行：先查正式库，确定缺口；本地有效数据直接复用；仅对缺失日期或字段请求锁定的主源；主源确实不可用且符合下述条件时才考虑备用源。禁止为“保险”“统一来源”或重新验证而默认下载完整历史。

任何外部数据请求前，先在内部确认并能说明四件事：本地是否已有数据；该标的当前锁定的 `primary_source` 是什么；实际缺少哪些交易日或字段；是否确需调用外部 API。未确认标的身份和缺口时，不得开始全量抓取。

`data/market.duckdb` 是正式历史仓库。优先通过只读连接核查表结构、业务键、日期、来源和有效值；`data/` 下 CSV、JSON 是已有缓存或阶段产物，不得因为缓存文件缺失就认定数据库缺失。数据读取、标准化与指标计算应分层：`fetch/update → normalize → persist → calculate`。指标计算只读取本地标准化数据，不在计算函数中临时调用 API。

## 已接入指数注册表与动态任务范围

- `data/market.duckdb` 中的 `index_registry` 是项目“已正式接入指数”名单的唯一事实来源。`AGENTS.md`、Python/JavaScript/HTML 脚本、缓存摘要和报表不得维护固定的指数数量或指数代码名单。
- `index_registry` 至少包含 `index_code`、`display_name`、`market`、`primary_source`、`source_symbol`、`status`、`constituent_mode`、`initialized_at`、`last_data_date`、`temperature_status`。`index_code` 是内部 canonical code；供应商原码只能放入 `source_symbol`。
- `market` 是指数市场组，只允许 `Ashare`、`HK`；股票行情表的 `market` 仍遵守 `SH`、`SZ`、`BJ`、`HK` 规则，不能混用两个字段的语义。
- `status` 只允许 `active`、`temperature_pending`、`disabled`。`active` 表示正式初始化完成且温度已有有效样本；`temperature_pending` 表示指数行情和必要成分数据已正式接入，但温度样本不足，仍属于已接入指数；`disabled` 表示暂不参与默认任务，历史数据不得删除。
- `temperature_status` 用于单独表达温度可用性：已有有效温度时为 `available`，样本不足时为 `pending`，确认不适用或不可计算时为 `na`。不可取得的值保持 `NA`，不得填 0 或其他替代值。
- 页面生成、每日增量更新、成分更新、温度计算、跨指数验证和相关报表必须在运行时读取 `index_registry`。默认处理 `status IN ('active', 'temperature_pending')` 的行；`disabled` 只有在任务明确指定时才处理。任务排序使用注册表字段或查询结果，不得在代码中复制名单。
- 新指数只有在 canonical identity、`primary_source`、`source_symbol`、必要行情和成分能力核验通过并完成正式初始化后，才能登记为 `active`；温度样本不足时登记为 `temperature_pending`。初始化失败、身份不确定、主源不可用或必要数据缺失时不得登记为 `active`。
- 新指数正式接入成功后必须自动登记 `index_registry`；后续增量任务直接读取注册表中的 `primary_source`、`source_symbol` 和 `constituent_mode`，不能每次根据名称、缓存标签或数值重新判断数据源。来源变更仍须写入 `data_source_transition`。
- 仅作为基准、对齐或计算辅助而存在的数据不自动视为正式接入指数；只有完成接入流程并登记进 `index_registry` 后，才可进入页面和跨指数任务的指数集合。

## 标的身份与数据源锁定

- 使用 `index_registry.index_code` 作为内部 canonical code，另保留供应商原码 `index_registry.source_symbol`；带市场前缀或后缀的供应商代码不得直接当作不同内部指数，也不得仅凭数字或名称猜测是同一指数。每个指数独立确定并锁定 `index_registry.primary_source`，不做全项目统一换源。
- 已初始化的指数原则上沿用各自既有主源增量更新。不得因为接入同花顺而把既有历史重抓、整体切换或形成无记录的来源断点。必须换源时先取得用户明确批准，记录 `source_transition_date`、`old_source`、`new_source`、`reason`，再执行。
- 新增指数先确认完全对应的 `EXACT` 标的与 `canonical_code`，再查正式库。优先验证同花顺 `hithink-finance` 能否提供同一标的的历史行情、当前行情、当前成分股与成分股行情；若可靠，首次初始化必要历史时优先以同花顺为该指数主源，之后继续从同一主源增量更新。已有其他可靠主源或同花顺不满足条件时，按证据逐标的决定，不能强行增加指数数量。
- 名称相似、`PUBLIC_EQUIVALENT`、同名 `.TI` 不能当作 `EXACT`。HY/GN 与 `.TI` 不得仅凭名称映射；现有 HY/GN 板块的相近 `.TI` 日收益序列未能证明同一对象，保持 `UNSUPPORTED`，除非用户明确要求重新研究。板块 K 线不能证明身份或缺可核验成分清单时，也保持 `UNSUPPORTED`/`NA`。
- 不同数据类型可使用不同最佳来源：既有官方指数历史沿用编制机构或交易所的可靠来源；已有股票历史优先正式库；新指数的 EXACT 数据可优先同花顺；最新行情可按标的来源策略使用同花顺；当前成分优先指数官方，同花顺自有指数体系可用同花顺；融资融券与 ETF 份额优先交易所；温度指标和历史百分位一律本地计算。股票也应记录逐标的来源，不因加入新指数而自动换源。
- 当前正式库保留了逐行 `source`、`fetched_at`，但不能把这误认为已建成逐标的 `primary_source` 锁定表或来源切换审计。缺少 `source_symbol`、锁定记录或过渡记录时先标明未知或待补，不得从名称或缓存标签推断；涉及补表或迁移须按数据库迁移规则办理。

## 缓存、缺口与增量更新

- 查询本地实际交易日与所需交易日历，计算 `missing_dates = required_dates - local_dates`。`MAX(date)` 只用于判断最新增量，不能证明中间无缺口。只请求缺失日期或必要的小窗口，不因一个缺口重抓整段历史。
- 指数增量范围先从 `index_registry` 动态读取，再从本地最大有效日期之后补新增交易日；股票按 `(market, stock_code)` 各自的最大有效日期和实际缺口补齐，不能用指数的最大日期代替。新增指数先建立成分股并集，查 `stock_daily`：同一股票跨多个指数只保存一套历史，只抓尚缺的股票与日期。
- 缓存目录、元数据等低频变化内容可本地复用；缓存记录 `source`、`fetched_at`，刷新须有明确策略。缓存与正式库不一致时先核实，不能直接用缓存覆盖正式库。批量任务按指数、股票独立记录成功与失败，允许断点续跑；后续接口失败不得删除此前成功结果或强制整批重抓。
- 当主源当天尚未发布，记录 `source_lag`、可用的最后交易日和下次增量检查点；不伪造数据、不使用未来数据、不因短时延迟切换主源。长期失效才评估备用源。
- 备用源仅在主源不可用、长期缺数据或用户明确要求交叉核验时使用。写入前核对代码、标的身份、交易日、价格与复权口径、成交量和成交额单位；记录 `source`、`source_symbol`、`fetched_at`，必要时记录 `fallback_reason`。不能因名称相似、数值较新或请求方便而自动替换。
- 两源同日冲突时，核对 `close`、`pct_change`、`volume`、`amount`、身份、单位与交易日，保留冲突记录。既有主源有效值默认继续使用；重大差异报告用户，不自动取“较新”一方。

## 正式库、键值与写入

- `stock_daily` 的逻辑唯一键是 `(market, stock_code, date)`；`market` 仅用 `SH`、`SZ`、`BJ`、`HK`。无法可靠判断市场时记录异常，不猜测，也不混用 `SSE`、`Shanghai`、`SHSE` 等表示。指数内部统一 `canonical_code`，供应商代码另存 `source_symbol`。
- `index_registry` 的逻辑唯一键是 `index_code`，`source_symbol` 也必须唯一；注册表只记录正式接入指数，不把辅助基准或未核验候选混入。
- `indicator_daily` 按 `(index_code, date, formula_version)` 隔离版本，V1.0 和 V1.1 必须共存、互不覆盖。其他业务表也遵守各自既有主键。已有有效业务键默认 `DO NOTHING`；禁止默认 `UPDATE`、`REPLACE` 或 `INSERT OR REPLACE`。只有原值为空、确认历史错误、用户明确要求刷新或官方修订，才可在审计记录下覆盖。
- 正式行情尽量保留 `source`、`source_symbol`、`fetched_at`。来源切换必须有过渡日期、旧源、新源与原因。不同接口的成交量可能为股或手，成交额可能为元、万元或亿元；确认原始单位并标准化后再写库，不能直接拼接不同单位的序列。
- 无法取得的值保持 `NA` 并记录原因，不填 0、均值、上一日或默认值；0 是有效数值。数据源返回空值、权限不足、无标的、限流和源延迟须区别记录，不把缺证据写成已验证。
- 对正式 DuckDB 修改表结构、主键或批量迁移前，先做备份，记录路径、时间与 SHA-256。迁移使用事务：建立结构、迁移、核对行数、主键和原值，通过后提交，关键检查失败则 `ROLLBACK`。迁移或批量写入前保存旧模型基线，之后核对行数、日期、原始指标、`score`、`temperature`、`temperature_change`、`temperature_level` 与 `formula_version`，确保旧结果不变。

## 成分股、交易日与模型

- 区分当前成分与真实逐日历史成分。用当前成分回算历史时必须标记 `constituent_mode = current_constituents_proxy`，不能称为真实历史成分；只有可靠重建历史调样才可标记 `point_in_time`。不得拼凑、插值、推测调样。
- 成分行情覆盖率为 `coverage_ratio = valid_constituent_count / total_constituent_count`。按现有模型，覆盖率低于 `0.80` 时，`BreadthMA20`、`HLBreadth`、`Sync` 及对应 `score` 记 `NA`；不得为增加样本下调阈值。
- A 股与港股分别使用对应交易日历。港股以沪深 300 为基准时，只能向过去对齐最近有效基准交易日，不用未来数据；建议保留 `benchmark_date` 供审计。
- V1.0 与 V1.1 模型冻结。未经用户明确要求模型迭代，不改指标定义、权重、756 日窗口、沪深 300 基准、温度区间、High/Low 状态定义或覆盖阈值；不因回测结果自行产生 V1.2/V2。

## 凭据、运行边界与交付

- API Key、Token、Secret 只从环境变量或本次进程的安全临时输入读取；代码使用 `os.getenv(...)` 等环境读取方式。真实值不得写入 `.py`、`.md`、`.json`、`.yaml`、`.env.example`、日志、Excel、缓存、项目目录或 Git，也不得打印认证 Header。不要在命令参数和工具日志中传递密钥。
- 对新增指数按以下顺序执行：确认 `canonical_code` 与 EXACT 身份 → 查 `market.duckdb` → 锁定并登记 `primary_source` 与 `source_symbol` → 验证必要行情和成分能力 → 仅初始化缺少的必要历史 → 永久保存 → 自动登记 `index_registry`（按温度样本选择 `active` 或 `temperature_pending`）→ 此后只增量更新 → 在既有冻结口径下计算 V1.0/V1.1。用户只给出指数名称时，先做身份与可行性核验，不直接全量下载。
- 每次数据任务汇报本地复用范围、实际请求的缺口、来源与口径、成功和失败、`NA` 原因、写入行数、输出路径及验证边界。若发现现有代码与本规范冲突，在未获修复授权的任务中只报告，不顺手修改其他功能。
