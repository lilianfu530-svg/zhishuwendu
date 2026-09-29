from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

import main as frozen_v10
import validate_v11 as frozen_v11


ROOT = Path(__file__).resolve().parent
DATABASE = ROOT / "data" / "market.duckdb"
CONSTITUENTS = ROOT / "data" / "phase2_constituents"
INDEX_CACHE = ROOT / "data" / "phase2_index_history"
SUMMARY = ROOT / "data" / "phase2_compute_summary.json"
CODES = ("399006", "930986", "000001", "000688", "899050", "932000", "HSTECH", "931787")
NEW_CODES = CODES[2:]
SHARED_COLUMNS = [
    "index_code", "date", "formula_version", "constituent_mode",
    "RET20_raw", "RET20_score", "BIAS20_raw", "BIAS20_score", "RS20_raw", "RS20_score",
    "VolumeStrength_raw", "VolumeStrength_score", "BreadthMA20_raw", "BreadthMA20_score",
    "HLBreadth_raw", "HLBreadth_score", "Sync_raw", "Sync_score", "temperature",
    "temperature_change_1d", "temperature_level", "PriceMomentum_score", "Volume_score",
    "Breadth_score", "Sync_score_v11", "temperature_change_5d", "temperature_state",
    "valid_constituent_count", "total_constituent_count", "coverage_ratio", "calculated_at",
]


def fast_average_pairwise_correlation(window: pd.DataFrame) -> float:
    values = window.to_numpy(dtype=float)
    if values.shape[0] < 20:
        return np.nan
    complete = np.isfinite(values).all(axis=0)
    values = values[:, complete]
    if values.shape[1] < 2:
        return np.nan
    centered = values - values.mean(axis=0)
    scales = np.sqrt(np.sum(centered * centered, axis=0))
    positive = scales > 0
    centered = centered[:, positive]
    scales = scales[positive]
    count = centered.shape[1]
    if count < 2:
        return np.nan
    normalized_sum = (centered / scales).sum(axis=1)
    return float((normalized_sum @ normalized_sum - count) / (count * (count - 1)))


def validate_fast_correlation() -> None:
    rng = np.random.default_rng(20260919)
    for count in (2, 10, 100):
        values = pd.DataFrame(rng.normal(size=(20, count)))
        if count > 2:
            values.iloc[3, 0] = np.nan
            values.iloc[:, 1] = 1.0
        actual = fast_average_pairwise_correlation(values)
        expected = frozen_v10.average_pairwise_correlation(values)
        if not np.isclose(actual, expected, atol=1e-12, rtol=0, equal_nan=True):
            raise RuntimeError(f"同步相关性优化与冻结定义不一致：{count}")


def load_members(code: str) -> pd.DataFrame:
    data = pd.read_csv(CONSTITUENTS / f"{code}.csv", dtype={"index_code": str, "stock_code": str}, encoding="utf-8-sig")
    if data.empty or data.duplicated(["market", "stock_code"]).any():
        raise RuntimeError(f"{code}当前成分股为空或重复")
    return data


def load_index(connection: duckdb.DuckDBPyConnection, code: str) -> pd.DataFrame:
    data = connection.execute(
        "SELECT date AS 日期, close AS 收盘, amount AS 成交额, volume AS 成交量 FROM index_daily WHERE index_code = ? ORDER BY date",
        [code],
    ).df()
    data["日期"] = pd.to_datetime(data["日期"])
    if data.empty or data["收盘"].isna().any():
        raise RuntimeError(f"{code}指数行情为空或缺收盘")
    return data


def load_histories(connection: duckdb.DuckDBPyConnection, members: pd.DataFrame) -> dict[str, pd.DataFrame]:
    connection.register("current_members", members[["market", "stock_code"]])
    try:
        data = connection.execute(
            """
            SELECT d.market, d.stock_code, d.date AS 日期, d.close AS 收盘
            FROM stock_daily AS d JOIN current_members AS m
              ON d.market = m.market AND d.stock_code = m.stock_code
            ORDER BY d.stock_code, d.date
            """
        ).df()
    finally:
        connection.unregister("current_members")
    data["日期"] = pd.to_datetime(data["日期"])
    histories = {}
    for (market, code), group in data.groupby(["market", "stock_code"], sort=False):
        histories[(market, code)] = group[["日期", "收盘"]].reset_index(drop=True)
    return histories


def close_matrix(members: pd.DataFrame, histories: dict, calendar: pd.DatetimeIndex) -> pd.DataFrame:
    series = {}
    for row in members.itertuples(index=False):
        history = histories.get((row.market, row.stock_code))
        if history is not None:
            series[row.stock_code] = history.set_index("日期")["收盘"]
    return pd.DataFrame(series).reindex(calendar)


def coverage(members: pd.DataFrame, histories: dict, calendar: pd.DatetimeIndex) -> pd.DataFrame:
    closes = close_matrix(members, histories, calendar)
    # 三项成分指标共用更严格的 21 个连续有效收盘，覆盖当日和前 20 个收益日。
    valid = closes.notna().rolling(21, min_periods=21).sum().eq(21)
    count = valid.sum(axis=1).astype(int)
    total = len(members)
    return pd.DataFrame({"date": calendar, "valid_constituent_count": count.to_numpy(),
                         "total_constituent_count": total, "coverage_ratio": count.to_numpy() / total})


def ingest_sources(connection: duckdb.DuckDBPyConnection) -> None:
    for code in CODES:
        members = load_members(code)
        incoming = members.copy()
        incoming["weight"] = np.nan
        incoming["exchange"] = incoming["market"].map({
            "SH": "上海证券交易所", "SZ": "深圳证券交易所", "BJ": "北京证券交易所", "HK": "香港交易所",
        })
        incoming["fetched_at"] = datetime.now()
        incoming["effective_date"] = pd.to_datetime(incoming["effective_date"])
        connection.register("incoming_members", incoming)
        try:
            connection.execute(
                """
                INSERT OR IGNORE INTO index_constituents
                (index_code, stock_code, stock_name, weight, exchange, effective_date, source, fetched_at, market)
                SELECT index_code, stock_code, stock_name, weight, exchange, effective_date, source, fetched_at, market
                FROM incoming_members
                """
            )
        finally:
            connection.unregister("incoming_members")
    for code in NEW_CODES:
        data = pd.read_csv(INDEX_CACHE / f"{code}.csv", dtype={"index_code": str}, encoding="utf-8-sig")
        data["date"] = pd.to_datetime(data["date"])
        data["fetched_at"] = pd.to_datetime(data["fetched_at"])
        connection.register("incoming_index", data)
        try:
            connection.execute(
                """
                INSERT OR IGNORE INTO index_daily
                (index_code, date, open, high, low, close, pct_change, volume, amount, source, fetched_at)
                SELECT index_code, date, open, high, low, close, pct_change, volume, amount, source, fetched_at
                FROM incoming_index
                """
            )
        finally:
            connection.unregister("incoming_index")


def update_original_index_dates(connection: duckdb.DuckDBPyConnection) -> dict[str, int]:
    # 原两指数与沪深300只请求数据库最新日期之后的区间。
    today = pd.Timestamp(datetime.now().date())
    target = today if today.weekday() < 5 else today - pd.offsets.BDay(1)
    counts = {}
    for code in ("000300", "399006", "930986"):
        latest = connection.execute("SELECT MAX(date) FROM index_daily WHERE index_code = ?", [code]).fetchone()[0]
        if latest is None:
            raise RuntimeError(f"{code}缺少原有指数历史，禁止重新初始化")
        start = pd.Timestamp(latest) + pd.Timedelta(days=1)
        if start > target:
            counts[code] = 0
            continue
        try:
            locked_source, locked_symbol = frozen_v10.locked_index_daily_source(connection, code)
            if locked_source == "同花顺金融数据服务":
                if not locked_symbol:
                    raise RuntimeError(f"{code} 已锁定同花顺主源但缺少source_symbol")
                previous_close = connection.execute(
                    "SELECT close FROM index_daily WHERE index_code = ? AND date = ?",
                    [code, latest],
                ).fetchone()[0]
                raw = frozen_v10.fetch_hithink_index_range(
                    locked_symbol, start, target, previous_close
                )
            else:
                raw = frozen_v10.fetch_index_range(code, start, target)
        except ValueError as exc:
            if "Length mismatch" not in str(exc):
                raise
            raw = pd.DataFrame()
        if raw.empty:
            counts[code] = 0
            continue
        incoming = pd.DataFrame({
            "index_code": code, "date": raw["日期"], "open": raw["开盘"],
            "high": raw["最高"], "low": raw["最低"], "close": raw["收盘"],
            "pct_change": raw["涨跌幅"], "volume": raw["成交量"], "amount": raw["成交额"],
            "source": locked_source if locked_source == "同花顺金融数据服务" else (
                "深证指数官网" if code == "399006" else "中证指数官网"
            ),
            "source_symbol": locked_symbol if locked_source == "同花顺金融数据服务" else None,
            "fetched_at": datetime.now(),
        })
        connection.register("incoming_original_index", incoming)
        try:
            connection.execute(
                "INSERT OR IGNORE INTO index_daily "
                "(index_code, date, open, high, low, close, pct_change, volume, amount, source, fetched_at, source_symbol) "
                "SELECT index_code, date, open, high, low, close, pct_change, volume, amount, source, fetched_at, source_symbol "
                "FROM incoming_original_index"
            )
        finally:
            connection.unregister("incoming_original_index")
        counts[code] = len(incoming)
    return counts


def asof_benchmark(index: pd.DataFrame, benchmark: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    source = benchmark[["日期", "收盘"]].rename(columns={"日期": "benchmark_date", "收盘": "benchmark_close"})
    aligned = pd.merge_asof(index[["日期"]].sort_values("日期"), source.sort_values("benchmark_date"),
                            left_on="日期", right_on="benchmark_date", direction="backward")
    future = (aligned["benchmark_date"] > aligned["日期"]).fillna(False)
    if future.any():
        raise RuntimeError("沪深300基准向未来对齐")
    lags = (aligned["日期"] - aligned["benchmark_date"]).dt.days
    report = {"asof_days": int((lags > 0).sum()), "max_lag_calendar_days": int(lags.max()) if lags.notna().any() else None}
    return aligned[["日期", "benchmark_close"]].rename(columns={"benchmark_close": "收盘"}), report


def write_new_index(connection: duckdb.DuckDBPyConnection, code: str, benchmark: pd.DataFrame) -> dict:
    existing, last_indicator_date = connection.execute(
        "SELECT COUNT(*), MAX(date) FROM indicator_daily WHERE index_code = ? AND formula_version = 'V1.0'", [code]
    ).fetchone()
    members = load_members(code)
    index = load_index(connection, code)
    if existing and pd.Timestamp(last_indicator_date) >= index["日期"].max():
        return {"index_code": code, "status": "already_initialized", "indicator_rows": existing,
                "constituent_count": len(members),
                "index_start": str(index["日期"].min().date()), "index_end": str(index["日期"].max().date())}
    histories_market = load_histories(connection, members)
    histories = {stock_code: frame for (_, stock_code), frame in histories_market.items()}
    calendar = pd.DatetimeIndex(index["日期"])
    counts = coverage(members, histories_market, calendar)
    if code in ("HSTECH", "931787"):
        comparison_benchmark, alignment = asof_benchmark(index, benchmark)
    else:
        comparison_benchmark, alignment = benchmark, {"asof_days": 0, "max_lag_calendar_days": 0}
    margin = pd.DataFrame({"日期": pd.Series(dtype="datetime64[ns]"), "融资余额合计": pd.Series(dtype=float)})
    metrics, member_counts = frozen_v10.compute_metrics(
        code, index, comparison_benchmark,
        members.rename(columns={"stock_code": "股票代码"}), histories, margin,
        write_legacy_outputs=False, latest_only=False,
    )
    metrics = metrics.rename(columns={"日期": "date"}).merge(counts, on="date", how="left", validate="one_to_one")
    below = metrics["coverage_ratio"] < .80
    for name in ("BreadthMA20", "HLBreadth", "Sync"):
        raw, score = f"{name}_raw", f"{name}_score"
        metrics.loc[below, raw] = np.nan
        metrics[score] = frozen_v10.rolling_percentile(metrics[raw])
    metrics = frozen_v10.add_temperature_columns(metrics)
    metrics["index_code"] = code
    metrics["formula_version"] = "V1.0"
    metrics["constituent_mode"] = "current_constituents_proxy"
    metrics["calculated_at"] = datetime.now()
    metrics = frozen_v11.add_changes_and_states(metrics)
    v11 = frozen_v11.make_v11_rows(metrics)
    v11["temperature_change_1d"] = v11["temperature_change_1d_calc"]
    v11["temperature_level"] = None
    new_metrics = metrics[metrics["date"] > pd.Timestamp(last_indicator_date)] if existing else metrics
    new_v11 = v11[v11["date"] > pd.Timestamp(last_indicator_date)] if existing else v11
    for frame in (new_metrics, new_v11):
        for column in SHARED_COLUMNS:
            if column not in frame:
                frame[column] = np.nan if column.endswith("_score") or column.endswith("_raw") else None
        connection.register("incoming_indicator", frame[SHARED_COLUMNS])
        try:
            connection.execute(
                f"INSERT OR IGNORE INTO indicator_daily ({','.join(SHARED_COLUMNS)}) SELECT {','.join(SHARED_COLUMNS)} FROM incoming_indicator"
            )
        finally:
            connection.unregister("incoming_indicator")
    return {
        "index_code": code,
        "status": ("incremental" if existing else "success") if member_counts["成功行情数量"] == len(members) else "partial_stock_history",
        "constituent_count": len(members),
        "stock_with_history_count": member_counts["成功行情数量"],
        "index_start": str(index["日期"].min().date()), "index_end": str(index["日期"].max().date()),
        "coverage_mean": float(counts["coverage_ratio"].mean()),
        "coverage_min": float(counts["coverage_ratio"].min()),
        "coverage_below_80_days": int((counts["coverage_ratio"] < .8).sum()),
        "first_80_date": str(counts.loc[counts["coverage_ratio"] >= .8, "date"].min().date()) if (counts["coverage_ratio"] >= .8).any() else None,
        "valid_temperature_v10": int(metrics["temperature"].notna().sum()),
        "valid_temperature_v11": int(v11["temperature"].notna().sum()),
        "inserted_indicator_rows": len(new_metrics) + len(new_v11),
        "benchmark_alignment": alignment, "member_counts": member_counts,
    }


def fill_frozen_coverage(connection: duckdb.DuckDBPyConnection, code: str) -> dict:
    members = load_members(code)
    index = load_index(connection, code)
    histories = load_histories(connection, members)
    counts = coverage(members, histories, pd.DatetimeIndex(index["日期"]))
    connection.register("incoming_coverage", counts)
    try:
        conflict = connection.execute(
            """
            SELECT COUNT(*) FROM indicator_daily AS i
            JOIN incoming_coverage AS c ON i.date = c.date
            WHERE i.index_code = ? AND c.coverage_ratio < 0.8
              AND (i.BreadthMA20_raw IS NOT NULL OR i.HLBreadth_raw IS NOT NULL OR i.Sync_raw IS NOT NULL)
            """, [code]
        ).fetchone()[0]
        connection.execute(
            """
            UPDATE indicator_daily AS i
            SET valid_constituent_count = c.valid_constituent_count,
                total_constituent_count = c.total_constituent_count,
                coverage_ratio = c.coverage_ratio
            FROM incoming_coverage AS c
            WHERE i.index_code = ? AND i.date = c.date
            """, [code]
        )
    finally:
        connection.unregister("incoming_coverage")
    return {
        "index_code": code, "status": "frozen_coverage_only", "constituent_count": len(members),
        "index_start": str(index["日期"].min().date()), "index_end": str(index["日期"].max().date()),
        "coverage_mean": float(counts["coverage_ratio"].mean()),
        "coverage_min": float(counts["coverage_ratio"].min()),
        "coverage_below_80_days": int((counts["coverage_ratio"] < .8).sum()),
        "frozen_historical_below80_raw_rows": int(conflict),
        "first_80_date": str(counts.loc[counts["coverage_ratio"] >= .8, "date"].min().date()) if (counts["coverage_ratio"] >= .8).any() else None,
    }


def main() -> None:
    validate_fast_correlation()
    frozen_v10.average_pairwise_correlation = fast_average_pairwise_correlation
    results = []
    with duckdb.connect(str(DATABASE)) as connection:
        index_updates = update_original_index_dates(connection)
        ingest_sources(connection)
        benchmark = load_index(connection, "000300")
        for code in CODES:
            print(f"计算 {code} 开始", flush=True)
            try:
                connection.execute("BEGIN TRANSACTION")
                if code in NEW_CODES:
                    result = write_new_index(connection, code, benchmark)
                else:
                    result = fill_frozen_coverage(connection, code)
                    latest_indicator, latest_index = connection.execute(
                        "SELECT (SELECT MAX(date) FROM indicator_daily WHERE index_code = ? AND formula_version = 'V1.0'), "
                        "(SELECT MAX(date) FROM index_daily WHERE index_code = ?)", [code, code]
                    ).fetchone()
                    if latest_indicator is None or latest_index > latest_indicator:
                        addition = write_new_index(connection, code, benchmark)
                        result["incremental_rows"] = addition.get("inserted_indicator_rows", 0)
                        result["status"] = addition["status"]
                connection.execute("COMMIT")
            except Exception as exc:
                connection.execute("ROLLBACK")
                result = {"index_code": code, "status": "failed", "reason": f"{type(exc).__name__}: {exc}"}
            results.append(result)
            SUMMARY.write_text(json.dumps({"run_at": datetime.now().isoformat(), "original_index_incremental_rows": index_updates,
                                           "indices": results}, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"计算 {code}: {result['status']}", flush=True)


if __name__ == "__main__":
    main()
