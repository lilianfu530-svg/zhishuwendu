from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

from validate_v11 import add_changes_and_states


ROOT = Path(__file__).resolve().parent
DATABASE = ROOT / "data" / "market.duckdb"
PAYLOAD = ROOT / "data" / "cross_index_validation_payload.json"
SUMMARY = ROOT / "output" / "cross_index_validation_summary.json"
COMPUTE_SUMMARY = ROOT / "data" / "phase2_compute_summary.json"
DATABASE_VERIFICATION = ROOT / "data" / "phase2_database_verification.json"
CODES = ("399006", "930986", "000001", "000688", "899050", "932000", "HSTECH", "931787")
NAMES = {"399006": "创业板指", "930986": "金融科技", "000001": "上证指数", "000688": "科创50",
         "899050": "北证50", "932000": "中证2000", "HSTECH": "恒生科技指数", "931787": "港股创新药"}
ASHARE = set(CODES[:6])
HK = set(CODES[6:])
STATES = ("High_Rising", "High_Falling", "Low_Rising", "Low_Falling", "Middle", "Flat")
HORIZONS = (5, 10, 20)
VERSIONS = ("V1.0", "V1.1")
DIMENSIONS = ("PriceMomentum_score", "Volume_score", "Breadth_score", "Sync_score_v11")


def serializable(value: object) -> object:
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value) if np.isfinite(value) else None
    if pd.isna(value):
        return None
    raise TypeError(type(value).__name__)


def as_float(value: object) -> float | None:
    return None if pd.isna(value) else float(value)


def forward_observations(frame: pd.DataFrame) -> pd.DataFrame:
    result = add_changes_and_states(frame)
    close = result["close"].to_numpy(dtype=float)
    for horizon in HORIZONS:
        result[f"return_{horizon}"] = result["close"].shift(-horizon) / result["close"] - 1
        draw = np.full(len(result), np.nan)
        upside = np.full(len(result), np.nan)
        for position in range(len(result) - horizon):
            window = close[position + 1 : position + horizon + 1]
            if np.isfinite(close[position]) and np.isfinite(window).all():
                draw[position] = window.min() / close[position] - 1
                upside[position] = window.max() / close[position] - 1
        result[f"max_drawdown_{horizon}"] = draw
        result[f"max_upside_{horizon}"] = upside
    return result


def metrics_for_group(frame: pd.DataFrame, horizon: int) -> dict:
    if frame.empty:
        return {"sample_count": 0, "mean_return": None, "median_return": None,
                "up_ratio": None, "mean_max_drawdown": None, "median_max_drawdown": None,
                "mean_max_upside": None, "median_max_upside": None}
    returns = frame[f"return_{horizon}"].dropna()
    draw = frame[f"max_drawdown_{horizon}"].dropna()
    up = frame[f"max_upside_{horizon}"].dropna()
    return {"sample_count": len(returns),
            "mean_return": as_float(returns.mean()), "median_return": as_float(returns.median()),
            "up_ratio": as_float((returns > 0).mean()),
            "mean_max_drawdown": as_float(draw.mean()), "median_max_drawdown": as_float(draw.median()),
            "mean_max_upside": as_float(up.mean()), "median_max_upside": as_float(up.median())}


def group_rows(observations: pd.DataFrame, group_name: str, indices: set[str]) -> list[dict]:
    selected = observations[observations["index_code"].isin(indices)]
    rows = []
    for version in VERSIONS:
        subset = selected[selected["formula_version"] == version]
        for state in STATES:
            state_rows = subset[subset["temperature_state"] == state]
            for horizon in HORIZONS:
                by_index = [metrics_for_group(g, horizon) for _, g in state_rows.groupby("index_code")]
                valid = [r for r in by_index if r["sample_count"] > 0]
                equal = {"sample_count": sum(r["sample_count"] for r in valid),
                         "index_count": len(valid)}
                for field in ("mean_return", "median_return", "up_ratio", "mean_max_drawdown",
                              "median_max_drawdown", "mean_max_upside", "median_max_upside"):
                    values = [r[field] for r in valid if r[field] is not None]
                    equal[field] = float(np.mean(values)) if values else None
                pooled = metrics_for_group(state_rows, horizon)
                pooled["index_count"] = len(valid)
                common = {"group": group_name, "formula_version": version, "state": state, "horizon": horizon}
                rows.append({**common, "method": "Equal_Index_Weight", **equal})
                rows.append({**common, "method": "Pooled", **pooled})
    return rows


def consistency_rows(by_index: list[dict], group_name: str, indices: set[str]) -> list[dict]:
    lookup = {(r["index_code"], r["formula_version"], r["state"], r["horizon"]): r for r in by_index}
    rows = []
    for version in VERSIONS:
        for horizon in HORIZONS:
            for better, worse, label in (("Low_Rising", "Low_Falling", "Low_Rising_vs_Low_Falling"),
                                         ("High_Falling", "High_Rising", "High_Falling_vs_High_Rising")):
                for field in ("mean_return", "mean_max_drawdown"):
                    comparable = []
                    improved = []
                    for code in CODES:
                        if code not in indices:
                            continue
                        left = lookup.get((code, version, better, horizon))
                        right = lookup.get((code, version, worse, horizon))
                        if not left or not right or left["sample_count"] == 0 or right["sample_count"] == 0:
                            continue
                        if left[field] is None or right[field] is None:
                            continue
                        comparable.append(code)
                        if left[field] > right[field]:
                            improved.append(code)
                    rows.append({"group": group_name, "formula_version": version, "comparison": label,
                                 "horizon": horizon, "metric": field, "improved_count": len(improved),
                                 "comparable_count": len(comparable), "improved_indices": improved,
                                 "comparable_indices": comparable})
    return rows


def main() -> None:
    compute = json.loads(COMPUTE_SUMMARY.read_text(encoding="utf-8"))
    verification = json.loads(DATABASE_VERIFICATION.read_text(encoding="utf-8"))
    with duckdb.connect(str(DATABASE), read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT i.*, d.close FROM indicator_daily AS i
            JOIN index_daily AS d ON i.index_code = d.index_code AND i.date = d.date
            WHERE i.index_code IN ('399006','930986','000001','000688','899050','932000','HSTECH','931787')
              AND i.formula_version IN ('V1.0','V1.1')
            ORDER BY i.index_code, i.formula_version, i.date
            """
        ).df()
        histories = connection.execute(
            """
            SELECT index_code, MIN(date) AS start_date, MAX(date) AS end_date,
                   COUNT(*) AS index_days FROM index_daily
            WHERE index_code IN ('399006','930986','000001','000688','899050','932000','HSTECH','931787')
            GROUP BY 1 ORDER BY 1
            """
        ).df()
        duplicates = connection.execute(
            """
            SELECT 'stock_daily' AS table_name, COUNT(*) - COUNT(DISTINCT (market, stock_code, date)) AS duplicate_count FROM stock_daily
            UNION ALL SELECT 'index_daily', COUNT(*) - COUNT(DISTINCT (index_code, date)) FROM index_daily
            UNION ALL SELECT 'indicator_daily', COUNT(*) - COUNT(DISTINCT (index_code, date, formula_version)) FROM indicator_daily
            """
        ).df().to_dict("records")
    if any(r["duplicate_count"] for r in duplicates):
        raise RuntimeError("数据库出现重复主键，停止报告")
    observations = pd.concat([forward_observations(g) for _, g in rows.groupby(["index_code", "formula_version"])], ignore_index=True)
    by_index = []
    sample_counts = []
    for (code, version), group in observations.groupby(["index_code", "formula_version"]):
        sample_counts.append({"index_code": code, "formula_version": version,
                              "valid_temperature_count": int(group["temperature"].notna().sum()),
                              **{state: int((group["temperature_state"] == state).sum()) for state in STATES}})
        for state in STATES:
            subset = group[group["temperature_state"] == state]
            for horizon in HORIZONS:
                by_index.append({"index_code": code, "formula_version": version, "state": state,
                                 "horizon": horizon, **metrics_for_group(subset, horizon)})
    aggregate = group_rows(observations, "All_8", set(CODES))
    aggregate += group_rows(observations, "Ashare", ASHARE)
    aggregate += group_rows(observations, "HK", HK)
    consistency = []
    for name, indices in (("All_8", set(CODES)), ("Ashare", ASHARE), ("HK", HK)):
        consistency += consistency_rows(by_index, name, indices)
    dimensions = []
    for code, group in rows[rows["formula_version"] == "V1.1"].groupby("index_code"):
        good = group.dropna(subset=list(DIMENSIONS))
        for method in ("pearson", "spearman"):
            matrix = good[list(DIMENSIONS)].corr(method=method)
            for i, left in enumerate(DIMENSIONS):
                for right in DIMENSIONS[i + 1 :]:
                    value = as_float(matrix.loc[left, right])
                    dimensions.append({"index_code": code, "method": method.title(),
                                       "dimension_1": left.replace("_score", ""),
                                       "dimension_2": right.replace("_score_v11", "").replace("_score", ""),
                                       "correlation": value, "abs_ge_0_80": value is not None and abs(value) >= .8,
                                       "sample_count": len(good)})
    high_corr_indices = sorted({r["index_code"] for r in dimensions if r["abs_ge_0_80"]})
    availability = {r["index_code"]: r for r in compute["indices"]}
    valid_samples = {(r["index_code"], r["formula_version"]): r["valid_temperature_count"] for r in sample_counts}
    index_list = []
    for code in CODES:
        hist = histories[histories["index_code"] == code]
        entry = availability.get(code, {})
        notes = []
        if entry.get("reason"):
            notes.append(entry["reason"])
        if code == "899050":
            notes.append("2022-04-29至2022-11-18为北交所发布的历史点位；实时发布始于2022-11-21")
        if code == "HSTECH":
            notes.append("恒生指数公司官网当前成分清单的可见日期为2026-09-11")
        if code in HK:
            alignment = entry.get("benchmark_alignment", {})
            notes.append(f"沪深300按当日及以前最近交易日对齐；回溯日期数{alignment.get('asof_days', 0)}")
        if entry.get("frozen_historical_below80_raw_rows"):
            notes.append("冻结旧值中有低覆盖率原值行；对应score和温度均为NA")
        index_list.append({"index_code": code, "display_name": NAMES[code],
                           "market_group": "HK" if code in HK else "Ashare",
                           "start_date": str(hist["start_date"].iloc[0]) if not hist.empty else None,
                           "end_date": str(hist["end_date"].iloc[0]) if not hist.empty else None,
                           "index_days": int(hist["index_days"].iloc[0]) if not hist.empty else 0,
                           "constituent_count": entry.get("constituent_count"),
                           "first_80_date": entry.get("first_80_date"),
                           "coverage_mean": entry.get("coverage_mean"),
                           "coverage_min": entry.get("coverage_min"),
                           "coverage_below_80_days": entry.get("coverage_below_80_days"),
                           "valid_temperature_v10": valid_samples.get((code, "V1.0"), 0),
                           "valid_temperature_v11": valid_samples.get((code, "V1.1"), 0),
                           "compute_status": entry.get("status"),
                           "data_note": "；".join(notes)})
    payload = {"generated_at": datetime.now().isoformat(), "index_list": index_list,
               "state_sample_count": sample_counts, "by_index": by_index, "aggregates": aggregate,
               "cross_index_consistency": consistency, "dimension_correlation": dimensions,
               "high_correlation_indices": high_corr_indices, "database_duplicate_checks": duplicates,
               "compute_summary": compute, "database_verification": verification}
    PAYLOAD.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=serializable), encoding="utf-8")
    low_rising_before = sum(r["Low_Rising"] for r in sample_counts if r["index_code"] in ("399006", "930986") and r["formula_version"] == "V1.1")
    low_rising_all = sum(r["Low_Rising"] for r in sample_counts if r["formula_version"] == "V1.1")
    summary = {"generated_at": payload["generated_at"], "complete_indices": sum(r["compute_status"] in ("success", "frozen_coverage_only") for r in index_list),
               "total_indices": len(index_list), "index_list": index_list,
               "low_rising_v11_original_two_samples": low_rising_before,
               "low_rising_v11_all_eight_samples": low_rising_all,
               "high_dimension_correlation_index_count": len(high_corr_indices),
               "high_dimension_correlation_indices": high_corr_indices,
               "key_consistency": [
                   {key: r[key] for key in ("group", "comparison", "horizon", "metric", "improved_count", "comparable_count")}
                   for r in consistency if r["formula_version"] == "V1.1" and
                   (r["group"] == "All_8" or r["horizon"] == 20)
               ],
               "database_duplicate_checks": duplicates,
               "backup_sha256": verification["backup_sha256"],
               "frozen_original_checks_passed": all(
                   row["rows"] == 1143 and row["valid_temperature"] == 872 and
                   row["changed_rows_forward"] == 0 and row["changed_rows_backward"] == 0
                   for row in verification["frozen_indicator_checks"]),
               "original_stock_rows_preserved": verification["original_stock_rows_missing_or_changed"] == 0}
    summary["data_anomalies"] = [
        "399006与930986在2022-02-07的旧Breadth原值已冻结；覆盖率低于80%，相关score与温度本就为NA",
        "899050的2022-04-29至2022-11-18指数点位为北交所发布的正式上线前历史点位",
        "HSTECH官方当前成分清单可见日期为2026-09-11",
        "港股指数47个交易日的沪深300基准采用当日以前最近交易日，最大滞后10个自然日",
        "689009及港股后续增量源不提供股票成交额；成分指标仅使用股票收盘价",
    ]
    SUMMARY.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=serializable), encoding="utf-8")
    print(json.dumps({"complete_indices": summary["complete_indices"], "low_rising_samples": low_rising_all,
                      "high_corr_indices": high_corr_indices}, ensure_ascii=False))


if __name__ == "__main__":
    main()
