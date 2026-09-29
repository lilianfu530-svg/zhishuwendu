# 指数温度计 V1.1 验证设计

## 范围

本次只开发并验证 V1.1，不增加指数、不接实时行情、不开发前端、不调整阈值和权重，不产生 V1.2。V1.0 原有记录、字段值和输出文件均不得覆盖。

## 数据与版本隔离

- 数据范围固定为 `399006` 和 `930986`。
- V1.1 复用 `indicator_daily` 中 V1.0 已保存的 7 个 score，不重新定义或重算基础指标。
- V1.1 使用 `formula_version = 'V1.1'`、`constituent_mode = 'current_constituents_proxy'` 写入 `indicator_daily`。
- 写入前保存 V1.0 全字段快照，写入后逐值、逐日期和逐行数核对，确认 V1.0 完全不变。
- V1.0 的 5 日变化和状态只在验证流程中派生，不回写 V1.0 记录。

## V1.1 计算

- `PriceMomentum_score = (RET20_score + BIAS20_score + RS20_score) / 3`
- `Volume_score = VolumeStrength_score`
- `Breadth_score = (BreadthMA20_score + HLBreadth_score) / 2`
- `Sync_score_v11 = Sync_score`
- `Temperature_V1_1 = (PriceMomentum_score + Volume_score + Breadth_score + Sync_score_v11) / 4`
- 综合温度保留 1 位小数。
- 任一所需 score 缺失时，四维温度不做缺失值替代，结果保留为空。

## 变化与状态

- `temperature_change_1d = Temperature(t) - Temperature(t-1)`。
- `temperature_change_5d = Temperature(t) - Temperature(t-5)`。
- `temperature_change_5d = 0` 时优先归为 `Flat`，不再按温度高低分类。
- 非零时依次归类：`High_Rising`、`High_Falling`、`Low_Falling`、`Low_Rising`、`Middle`。
- 因前 5 个有效温度记录而无法计算 5 日变化的记录不进入状态统计。

## 验证统计

按指数、公式版本和状态分别统计状态样本数，以及未来 5、10、20 日收益的有效样本数、平均值、中位数和上涨比例。未来 20 日同时统计最大回撤和最大上涨的有效样本数、平均值和中位数。

采用固定定义：

- `ForwardReturn_N = Close(t+N) / Close(t) - 1`
- `Future20_MaxDrawdown = min(Close[t+1:t+20]) / Close(t) - 1`
- `Future20_MaxUpside = max(Close[t+1:t+20]) / Close(t) - 1`

版本比较只依据实际样本结果，重点比较高温上升与下降、低温下降与回升，以及 V1.1 对极端状态的区分能力。

## 维度相关性

对 V1.1 的 `PriceMomentum`、`Volume`、`Breadth`、`Sync` 分别计算 Pearson 和 Spearman 相关矩阵。绝对相关性不低于 0.80 的组合只报告，不修改模型。

## 输出与验收

生成 `output/temperature_v11_validation.xlsx`，包含：

- `399006_V1.0`
- `399006_V1.1`
- `930986_V1.0`
- `930986_V1.1`
- `V1_vs_V1.1`
- `Dimension_Correlation`

验收包括数据库版本与重复主键检查、V1.0 前后逐值一致性、公式独立抽算、状态边界检查、前瞻收益和极值区间抽算、相关矩阵复算、Excel 关键区域检查、公式错误扫描和全部工作表渲染检查。
