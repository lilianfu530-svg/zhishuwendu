from __future__ import annotations

import json
import importlib.util
import os
import shutil
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# Windows下直接执行“python main.py”时，自动使用项目已配置的虚拟环境。
PROJECT_PYTHON = Path(__file__).resolve().parent / ".venv" / "Scripts" / "python.exe"
REQUIRED_MODULES = ("akshare", "duckdb", "numpy", "pandas")
if PROJECT_PYTHON.exists() and Path(sys.executable).resolve() != PROJECT_PYTHON.resolve():
    if any(importlib.util.find_spec(module) is None for module in REQUIRED_MODULES):
        completed = subprocess.run([str(PROJECT_PYTHON), *sys.argv], check=False)
        raise SystemExit(completed.returncode)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import akshare as ak
import duckdb
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "output"
STOCK_CACHE_DIR = DATA_DIR / "stock_history"
MARGIN_CACHE_DIR = DATA_DIR / "margin"
DATABASE_PATH = DATA_DIR / "market.duckdb"
START_DATE = "20220101"
END_DATE = datetime.now().strftime("%Y%m%d")
PERCENTILE_WINDOW = 756
MIN_PERCENTILE_OBS = 252

STATUS_OK = "\u2705 可稳定取得"
STATUS_LIMITED = "\u26a0\ufe0f 可取得但有限制"
STATUS_FAILED = "\u274c 当前公开接口无法可靠取得"


@dataclass(frozen=True)
class IndexSpec:
    code: str
    name: str
    history_source: str


INDEX_SPECS = {
    "399006": IndexSpec("399006", "创业板指数", "国证指数官网（AKShare index_hist_cni）"),
    "930986": IndexSpec("930986", "中证金融科技主题指数", "中证指数官网（AKShare stock_zh_index_hist_csindex）"),
}


CONFIRMED_ETFS = {
    "399006": {
        "159675",
        "159908",
        "159915",
        "159948",
        "159952",
        "159956",
        "159957",
        "159958",
        "159964",
        "159971",
    },
    "930986": {
        "159086",
        "159103",
        "159299",
        "159851",
        "515720",
        "516100",
        "516860",
        "563570",
        "563670",
    },
}


RESULT_COLUMNS = [
    "日期",
    "RET20_raw",
    "RET20_score",
    "BIAS20_raw",
    "BIAS20_score",
    "RS20_raw",
    "RS20_score",
    "VolumeStrength_raw",
    "VolumeStrength_score",
    "BreadthMA20_raw",
    "BreadthMA20_score",
    "HLBreadth_raw",
    "HLBreadth_score",
    "Margin20_raw",
    "Margin20_score",
    "ETFShare20_raw",
    "ETFShare20_score",
    "Sync_raw",
    "Sync_score",
]

TEMPERATURE_RESULT_COLUMNS = [
    "日期",
    "RET20_raw",
    "RET20_score",
    "BIAS20_raw",
    "BIAS20_score",
    "RS20_raw",
    "RS20_score",
    "VolumeStrength_raw",
    "VolumeStrength_score",
    "BreadthMA20_raw",
    "BreadthMA20_score",
    "HLBreadth_raw",
    "HLBreadth_score",
    "Sync_raw",
    "Sync_score",
    "temperature",
    "temperature_change_1d",
    "temperature_level",
]

TEMPERATURE_WEIGHTS = {
    "RET20_score": 0.15,
    "BIAS20_score": 0.15,
    "RS20_score": 0.15,
    "VolumeStrength_score": 0.10,
    "BreadthMA20_score": 0.20,
    "HLBreadth_score": 0.15,
    "Sync_score": 0.10,
}


def ensure_directories() -> None:
    for path in (DATA_DIR, OUTPUT_DIR, STOCK_CACHE_DIR, MARGIN_CACHE_DIR):
        path.mkdir(parents=True, exist_ok=True)


def call_with_retry(func: Callable[[], pd.DataFrame], attempts: int = 3) -> pd.DataFrame:
    """仅对瞬时网络错误做有限重试，不掩盖最终失败。"""
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            return func()
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt + 1 < attempts:
                time.sleep(1.5 * (attempt + 1))
    assert last_error is not None
    raise last_error


def clean_date_index(frame: pd.DataFrame, date_column: str = "日期") -> pd.DataFrame:
    data = frame.copy()
    data[date_column] = pd.to_datetime(data[date_column], errors="coerce")
    data = data.dropna(subset=[date_column])
    data = data[data[date_column].dt.weekday < 5]
    data = data.sort_values(date_column).drop_duplicates(date_column, keep="last")
    return data.reset_index(drop=True)


def fetch_index_history(code: str) -> pd.DataFrame:
    cache_path = DATA_DIR / f"index_{code}.csv"
    if code == "399006":
        raw = call_with_retry(
            lambda: ak.index_hist_cni(symbol=code, start_date=START_DATE, end_date=END_DATE)
        )
        data = raw.rename(
            columns={
                "日期": "日期",
                "开盘价": "开盘",
                "最高价": "最高",
                "最低价": "最低",
                "收盘价": "收盘",
                "涨跌幅": "涨跌幅",
                "成交量": "成交量",
                "成交额": "成交额",
            }
        )
        # 国证接口：成交量单位为万手，成交额单位为亿元。
        data["成交量"] = pd.to_numeric(data["成交量"], errors="coerce") * 1_000_000
        data["成交额"] = pd.to_numeric(data["成交额"], errors="coerce") * 100_000_000
    elif code == "930986":
        raw = call_with_retry(
            lambda: ak.stock_zh_index_hist_csindex(
                symbol=code, start_date=START_DATE, end_date=END_DATE
            )
        )
        data = raw.rename(columns={"成交金额": "成交额"})
        # 中证接口：成交金额单位为亿元，成交量单位为股。
        data["成交额"] = pd.to_numeric(data["成交额"], errors="coerce") * 100_000_000
    else:
        raise ValueError(f"不支持的指数代码：{code}")

    keep = ["日期", "开盘", "最高", "最低", "收盘", "涨跌幅", "成交量", "成交额"]
    data = clean_date_index(data[keep])
    for column in keep[1:]:
        data[column] = pd.to_numeric(data[column], errors="coerce")
    data.to_csv(cache_path, index=False, encoding="utf-8-sig")
    return data


def fetch_benchmark_history() -> pd.DataFrame:
    cache_path = DATA_DIR / "index_000300.csv"
    raw = call_with_retry(
        lambda: ak.stock_zh_index_hist_csindex(
            symbol="000300", start_date=START_DATE, end_date=END_DATE
        )
    )
    data = clean_date_index(raw[["日期", "收盘"]])
    data["收盘"] = pd.to_numeric(data["收盘"], errors="coerce")
    data.to_csv(cache_path, index=False, encoding="utf-8-sig")
    return data


def infer_exchange(code: str) -> str:
    if code.startswith(("4", "8", "9")):
        return "北京证券交易所"
    if code.startswith(("5", "6", "68")):
        return "上海证券交易所"
    return "深圳证券交易所"


def fetch_constituents(code: str) -> pd.DataFrame:
    cache_path = DATA_DIR / f"constituents_{code}.csv"
    if code == "399006":
        raw = call_with_retry(lambda: ak.index_detail_cni(symbol=code))
        data = raw.rename(
            columns={"日期": "数据日期", "样本代码": "股票代码", "样本简称": "股票名称", "权重": "当前权重"}
        )
    elif code == "930986":
        raw = call_with_retry(lambda: ak.index_stock_cons_weight_csindex(symbol=code))
        data = raw.rename(
            columns={"日期": "数据日期", "成分券代码": "股票代码", "成分券名称": "股票名称", "权重": "当前权重"}
        )
    else:
        raise ValueError(f"不支持的指数代码：{code}")

    data["股票代码"] = data["股票代码"].astype(str).str.extract(r"(\d+)", expand=False).str.zfill(6)
    data["所属交易所"] = data.get("交易所", data["股票代码"].map(infer_exchange))
    data["数据日期"] = pd.to_datetime(data["数据日期"], errors="coerce").dt.strftime("%Y-%m-%d")
    data["当前权重"] = pd.to_numeric(data["当前权重"], errors="coerce")
    result = data[["股票代码", "股票名称", "当前权重", "所属交易所", "数据日期"]].drop_duplicates("股票代码")
    result = result.sort_values("股票代码").reset_index(drop=True)
    result.to_csv(cache_path, index=False, encoding="utf-8-sig")
    return result


def market_prefix(code: str) -> str:
    if code.startswith(("4", "8", "9")):
        return "bj"
    if code.startswith(("5", "6", "68")):
        return "sh"
    return "sz"


def fetch_stock_history(code: str) -> pd.DataFrame:
    cache_path = STOCK_CACHE_DIR / f"{code}.csv"
    if cache_path.exists():
        cached = pd.read_csv(cache_path, dtype={"股票代码": str}, parse_dates=["日期"])
        if not cached.empty and cached["日期"].max() >= pd.Timestamp(END_DATE) - pd.Timedelta(days=7):
            return cached

    symbol = f"{market_prefix(code)}{code}"
    raw = call_with_retry(
        lambda: ak.stock_zh_a_daily(
            symbol=symbol,
            start_date=START_DATE,
            end_date=END_DATE,
            adjust="",
        )
    )
    if raw.empty:
        raise ValueError(f"{code} 未返回历史行情")
    data = raw.rename(
        columns={
            "date": "日期",
            "close": "收盘",
            "volume": "成交量",
            "amount": "成交额",
        }
    )
    data = clean_date_index(data, "日期")
    for column in ("收盘", "成交量", "成交额"):
        data[column] = pd.to_numeric(data[column], errors="coerce")
    data["涨跌幅"] = data["收盘"].pct_change(fill_method=None)
    data.insert(0, "股票代码", code)
    data = data[["股票代码", "日期", "收盘", "涨跌幅", "成交量", "成交额"]]
    data.to_csv(cache_path, index=False, encoding="utf-8-sig")
    return data


def fetch_all_stock_histories(constituents: dict[str, pd.DataFrame]) -> tuple[dict[str, pd.DataFrame], dict[str, str]]:
    codes = sorted(set().union(*(set(frame["股票代码"]) for frame in constituents.values())))
    histories: dict[str, pd.DataFrame] = {}
    failures: dict[str, str] = {}
    for position, code in enumerate(codes, start=1):
        try:
            histories[code] = fetch_stock_history(code)
        except Exception as exc:  # noqa: BLE001
            failures[code] = f"{type(exc).__name__}: {exc}"
        if position % 20 == 0 or position == len(codes):
            print(f"成分股行情进度：{position}/{len(codes)}，失败 {len(failures)}")
        time.sleep(0.08)
    (DATA_DIR / "stock_history_failures.json").write_text(
        json.dumps(failures, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return histories, failures


def fetch_confirmed_etfs() -> pd.DataFrame:
    """保存已由交易所列表或基金公告确认跟踪标的的ETF当前份额。"""
    raw = call_with_retry(ak.fund_etf_spot_em, attempts=2)
    raw["代码"] = raw["代码"].astype(str).str.extract(r"(\d+)", expand=False).str.zfill(6)
    rows = []
    for index_code, fund_codes in CONFIRMED_ETFS.items():
        for fund_code in sorted(fund_codes):
            matched = raw[raw["代码"].eq(fund_code)]
            if matched.empty:
                rows.append(
                    {
                        "指数代码": index_code,
                        "ETF代码": fund_code,
                        "ETF名称": np.nan,
                        "跟踪指数": index_code,
                        "所属交易所": infer_exchange(fund_code),
                        "当前份额": np.nan,
                        "份额日期": np.nan,
                        "跟踪关系确认来源": "交易所ETF列表或基金公告",
                        "历史份额状态": "无法稳定取得连续日历史",
                    }
                )
                continue
            row = matched.iloc[0]
            rows.append(
                {
                    "指数代码": index_code,
                    "ETF代码": fund_code,
                    "ETF名称": row.get("名称"),
                    "跟踪指数": index_code,
                    "所属交易所": infer_exchange(fund_code),
                    "当前份额": pd.to_numeric(row.get("最新份额"), errors="coerce"),
                    "份额日期": str(row.get("数据日期")),
                    "跟踪关系确认来源": "交易所ETF列表或基金公告",
                    "历史份额状态": "无法稳定取得连续日历史",
                }
            )
    result = pd.DataFrame(rows).sort_values(["指数代码", "ETF代码"]).reset_index(drop=True)
    result.to_csv(DATA_DIR / "etf_tracking.csv", index=False, encoding="utf-8-sig")
    return result


def margin_table_for_date(date: pd.Timestamp, exchange: str) -> pd.DataFrame:
    day = date.strftime("%Y%m%d")
    cache_path = MARGIN_CACHE_DIR / f"{day}_{exchange}.csv"
    if cache_path.exists():
        return pd.read_csv(cache_path, dtype={"股票代码": str})

    if exchange == "SZ":
        raw = call_with_retry(lambda: ak.stock_margin_detail_szse(date=day), attempts=2)
        data = raw.rename(columns={"证券代码": "股票代码"})
    elif exchange == "SH":
        raw = call_with_retry(lambda: ak.stock_margin_detail_sse(date=day), attempts=2)
        data = raw.rename(columns={"标的证券代码": "股票代码", "标的证券简称": "证券简称"})
        data["融券余额"] = np.nan
    elif exchange == "BJ":
        raw = call_with_retry(lambda: ak.stock_margin_detail_bse(date=day), attempts=2)
        data = raw.rename(columns={"证券代码": "股票代码"})
    else:
        raise ValueError(f"不支持的交易所：{exchange}")

    data["股票代码"] = data["股票代码"].astype(str).str.extract(r"(\d+)", expand=False).str.zfill(6)
    for column in ("融资余额", "融资买入额", "融券余额"):
        if column not in data:
            data[column] = np.nan
        data[column] = pd.to_numeric(data[column], errors="coerce")
    result = data[["股票代码", "融资余额", "融资买入额", "融券余额"]]
    result.to_csv(cache_path, index=False, encoding="utf-8-sig")
    return result


def fetch_margin_aggregates(
    constituents: dict[str, pd.DataFrame], calendars: dict[str, pd.DatetimeIndex]
) -> tuple[dict[str, pd.DataFrame], list[str]]:
    target_dates = sorted(set().union(*(set(index[-25:]) for index in calendars.values())))
    daily_tables: dict[pd.Timestamp, dict[str, pd.DataFrame]] = {}
    failures: list[str] = []
    for position, date in enumerate(target_dates, start=1):
        exchange_frames: dict[str, pd.DataFrame] = {}
        for exchange in ("SZ", "SH", "BJ"):
            try:
                frame = margin_table_for_date(date, exchange)
                frame = frame.copy()
                frame["交易所"] = exchange
                exchange_frames[exchange] = frame
            except Exception as exc:  # noqa: BLE001
                failures.append(f"{date.date()} {exchange}: {type(exc).__name__}: {exc}")
        daily_tables[date] = exchange_frames
        print(f"融资明细进度：{position}/{len(target_dates)}")

    aggregates: dict[str, pd.DataFrame] = {}
    for code, members in constituents.items():
        member_codes = set(members["股票代码"])
        required_exchanges = {
            {"sz": "SZ", "sh": "SH", "bj": "BJ"}[market_prefix(member_code)]
            for member_code in member_codes
        }
        rows = []
        for date in calendars[code][-25:]:
            exchange_frames = daily_tables.get(date, {})
            if not required_exchanges.issubset(exchange_frames):
                rows.append({"日期": date, "融资余额合计": np.nan, "匹配证券数": 0})
                continue
            table = pd.concat(
                [exchange_frames[exchange] for exchange in sorted(required_exchanges)],
                ignore_index=True,
            )
            matched = table[table["股票代码"].isin(member_codes)]
            rows.append(
                {
                    "日期": date,
                    "融资余额合计": matched["融资余额"].sum(min_count=1),
                    "匹配证券数": int(matched["融资余额"].notna().sum()),
                }
            )
        frame = pd.DataFrame(rows).sort_values("日期").reset_index(drop=True)
        frame.to_csv(DATA_DIR / f"margin_aggregate_{code}.csv", index=False, encoding="utf-8-sig")
        aggregates[code] = frame
    (DATA_DIR / "margin_failures.json").write_text(
        json.dumps(failures, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return aggregates, failures


def build_close_matrix(
    member_codes: list[str], histories: dict[str, pd.DataFrame], calendar: pd.DatetimeIndex
) -> pd.DataFrame:
    series = {}
    for code in member_codes:
        history = histories.get(code)
        if history is None:
            continue
        series[code] = history.set_index("日期")["收盘"]
    if not series:
        return pd.DataFrame(index=calendar)
    return pd.DataFrame(series).reindex(calendar)


def average_pairwise_correlation(window: pd.DataFrame) -> float:
    valid = window.dropna(axis=1, how="any")
    if len(valid) < 20 or valid.shape[1] < 2:
        return np.nan
    corr = valid.corr(min_periods=20).to_numpy(dtype=float)
    mask = ~np.eye(corr.shape[0], dtype=bool)
    values = corr[mask]
    values = values[np.isfinite(values)]
    return float(values.mean()) if len(values) else np.nan


def rolling_percentile(series: pd.Series) -> pd.Series:
    values = series.to_numpy(dtype=float)
    result = np.full(len(values), np.nan)
    for index, current in enumerate(values):
        if not np.isfinite(current):
            continue
        start = max(0, index - PERCENTILE_WINDOW + 1)
        history = values[start : index + 1]
        history = history[np.isfinite(history)]
        if len(history) < MIN_PERCENTILE_OBS:
            continue
        less = np.sum(history < current)
        equal = np.sum(history == current)
        result[index] = (less + 0.5 * equal) / len(history) * 100
    return pd.Series(result, index=series.index)


def compute_metrics(
    code: str,
    index_history: pd.DataFrame,
    benchmark: pd.DataFrame,
    members: pd.DataFrame,
    histories: dict[str, pd.DataFrame],
    margin: pd.DataFrame,
    write_legacy_outputs: bool = True,
    latest_only: bool = True,
) -> tuple[pd.DataFrame, dict[str, int]]:
    data = index_history.set_index("日期").copy()
    calendar = data.index
    benchmark_close = benchmark.set_index("日期")["收盘"].reindex(calendar)
    closes = build_close_matrix(members["股票代码"].tolist(), histories, calendar)

    data["RET20_raw"] = data["收盘"] / data["收盘"].shift(20) - 1
    data["BIAS20_raw"] = data["收盘"] / data["收盘"].rolling(20, min_periods=20).mean() - 1
    benchmark_ret20 = benchmark_close / benchmark_close.shift(20) - 1
    data["RS20_raw"] = data["RET20_raw"] - benchmark_ret20

    direction = np.sign(data["收盘"].pct_change(fill_method=None))
    signed_amount = data["成交额"].where(direction > 0, -data["成交额"].where(direction < 0, 0))
    data["VolumeStrength_raw"] = (
        signed_amount.rolling(20, min_periods=20).sum()
        / data["成交额"].rolling(20, min_periods=20).sum()
    )

    stock_ma20 = closes.rolling(20, min_periods=20).mean()
    breadth_valid = closes.notna() & stock_ma20.notna()
    data["BreadthMA20_raw"] = (closes > stock_ma20).where(breadth_valid).sum(axis=1, min_count=1) / breadth_valid.sum(axis=1).replace(0, np.nan)

    high20 = closes.rolling(20, min_periods=20).max()
    low20 = closes.rolling(20, min_periods=20).min()
    hl_valid = closes.notna() & high20.notna() & low20.notna()
    new_highs = closes.eq(high20).where(hl_valid).sum(axis=1, min_count=1)
    new_lows = closes.eq(low20).where(hl_valid).sum(axis=1, min_count=1)
    data["HLBreadth_raw"] = (new_highs - new_lows) / hl_valid.sum(axis=1).replace(0, np.nan)

    margin_series = margin.set_index("日期")["融资余额合计"].reindex(calendar)
    data["Margin20_raw"] = margin_series / margin_series.shift(20) - 1
    data["ETFShare20_raw"] = np.nan

    returns = closes.pct_change(fill_method=None)
    avg_corr = pd.Series(index=calendar, dtype=float)
    for position in range(19, len(calendar)):
        avg_corr.iloc[position] = average_pairwise_correlation(returns.iloc[position - 19 : position + 1])
    data["Sync_raw"] = np.sign(data["RET20_raw"]) * avg_corr

    raw_names = [column for column in data.columns if column.endswith("_raw")]
    for raw_name in raw_names:
        score_name = raw_name.replace("_raw", "_score")
        if raw_name in ("Margin20_raw", "ETFShare20_raw"):
            data[score_name] = np.nan
        else:
            data[score_name] = rolling_percentile(data[raw_name])

    result = data.reset_index().rename(columns={"index": "日期"})
    result = result[RESULT_COLUMNS]
    latest = result.tail(5).copy()
    latest["日期"] = latest["日期"].dt.strftime("%Y-%m-%d")
    if write_legacy_outputs:
        latest.to_csv(
            OUTPUT_DIR / f"index_crowding_{code}_latest5.csv",
            index=False,
            na_rep="NA",
            encoding="utf-8-sig",
        )
        result.to_csv(
            DATA_DIR / f"metrics_history_{code}.csv",
            index=False,
            na_rep="NA",
            encoding="utf-8-sig",
        )

    counts = {
        "成分股数量": len(members),
        "成功行情数量": sum(code_ in histories for code_ in members["股票代码"]),
        "最新广度有效数": int(closes.iloc[-1].notna().sum()),
        "最新MA20有效数": int((closes.notna() & stock_ma20.notna()).iloc[-1].sum()),
    }
    return (latest if latest_only else result), counts


def validate_outputs(results: dict[str, pd.DataFrame], index_histories: dict[str, pd.DataFrame]) -> dict[str, list[str]]:
    findings: dict[str, list[str]] = {}
    for code, result in results.items():
        issues: list[str] = []
        history = index_histories[code]
        if not history["日期"].is_monotonic_increasing:
            issues.append("指数日期不是升序")
        if history["日期"].duplicated().any():
            issues.append("指数日期存在重复")
        if history["收盘"].isna().mean() > 0.01:
            issues.append("指数收盘价空值超过1%")
        score_columns = [column for column in result if column.endswith("_score")]
        if any(((result[column] < 0) | (result[column] > 100)).any() for column in score_columns):
            issues.append("百分位超出0到100")
        if ((result["BreadthMA20_raw"] < 0) | (result["BreadthMA20_raw"] > 1)).any():
            issues.append("MA20市场广度超出0到1")
        if ((result["HLBreadth_raw"] < -1) | (result["HLBreadth_raw"] > 1)).any():
            issues.append("新高新低扩散度超出-1到1")
        if ((result["Sync_raw"] < -1.000001) | (result["Sync_raw"] > 1.000001)).any():
            issues.append("同步度超出-1到1")
        expected_dates = history["日期"].tail(5).dt.strftime("%Y-%m-%d").tolist()
        if result["日期"].tolist() != expected_dates:
            issues.append("结果日期不是该指数数据源最新5个有效交易日")
        findings[code] = issues
    return findings


def report_table_row(
    item: str,
    status_399006: str,
    status_930986: str,
    source: str,
    interface: str,
    range_399006: str,
    range_930986: str,
    count_399006: str,
    count_930986: str,
    missing: str,
    stable: str,
    note: str,
) -> str:
    cells = [
        item,
        status_399006,
        status_930986,
        source,
        interface,
        range_399006,
        range_930986,
        count_399006,
        count_930986,
        missing,
        stable,
        note,
    ]
    return "| " + " | ".join(str(cell).replace("|", "／") for cell in cells) + " |"


def create_report(
    index_histories: dict[str, pd.DataFrame],
    constituents: dict[str, pd.DataFrame],
    histories: dict[str, pd.DataFrame],
    stock_failures: dict[str, str],
    margin: dict[str, pd.DataFrame],
    margin_failures: list[str],
    etf_tracking: pd.DataFrame,
    metric_counts: dict[str, dict[str, int]],
    validations: dict[str, list[str]],
) -> None:
    ranges = {
        code: f"{frame['日期'].min().date()} 至 {frame['日期'].max().date()}"
        for code, frame in index_histories.items()
    }
    margin_ranges = {
        code: f"{frame['日期'].min().date()} 至 {frame['日期'].max().date()}"
        for code, frame in margin.items()
    }
    stock_date_min = min(frame["日期"].min() for frame in histories.values()).date() if histories else "NA"
    stock_date_max = max(frame["日期"].max() for frame in histories.values()).date() if histories else "NA"
    history_success = {
        code: sum(member in histories for member in frame["股票代码"])
        for code, frame in constituents.items()
    }
    index_status_399006 = STATUS_LIMITED if index_histories["399006"]["日期"].max().date() < datetime.now().date() else STATUS_OK
    rows = [
        report_table_row(
            "指数日行情",
            index_status_399006,
            STATUS_OK,
            "国证指数官网；中证指数官网",
            "index_hist_cni；stock_zh_index_hist_csindex",
            ranges["399006"],
            ranges["930986"],
            str(len(index_histories["399006"])),
            str(len(index_histories["930986"])),
            "核心字段无大量缺失",
            STATUS_LIMITED,
            "已统一成交量为股、成交额为元；删除官方接口返回的周末记录。",
        ),
        report_table_row(
            "当前成分股及权重",
            STATUS_OK,
            STATUS_OK,
            "国证指数官网；中证指数官网",
            "index_detail_cni；index_stock_cons_weight_csindex",
            constituents["399006"]["数据日期"].min(),
            constituents["930986"]["数据日期"].min(),
            str(len(constituents["399006"])),
            str(len(constituents["930986"])),
            "代码、名称、权重、交易所均可得",
            STATUS_OK,
            "权重日期均为官方最新下载文件日期。",
        ),
        report_table_row(
            "成分股历史行情",
            STATUS_OK if history_success["399006"] == len(constituents["399006"]) else STATUS_LIMITED,
            STATUS_OK if history_success["930986"] == len(constituents["930986"]) else STATUS_LIMITED,
            "新浪财经（AKShare封装）",
            "stock_zh_a_daily",
            f"{stock_date_min} 至 {stock_date_max}",
            f"{stock_date_min} 至 {stock_date_max}",
            f"{history_success['399006']}/{len(constituents['399006'])}",
            f"{history_success['930986']}/{len(constituents['930986'])}",
            f"失败代码共{len(stock_failures)}只",
            STATUS_LIMITED,
            "采用逐只请求和本地缓存；涨跌幅由未复权收盘价计算。",
        ),
        report_table_row(
            "融资融券明细与聚合",
            STATUS_LIMITED,
            STATUS_LIMITED,
            "深交所、上交所、北交所",
            "stock_margin_detail_szse／sse／bse",
            margin_ranges["399006"],
            margin_ranges["930986"],
            str(len(margin["399006"])),
            str(len(margin["930986"])),
            f"接口失败记录{len(margin_failures)}条；非融资标的不在明细中",
            STATUS_LIMITED,
            "仅抓取计算最近5日Margin20原值所需的25个交易日；历史百分位为NA。",
        ),
        report_table_row(
            "ETF跟踪关系",
            STATUS_OK,
            STATUS_OK,
            "深交所月报、基金公告、上交所基金公告",
            "交易所公开文件与基金公告",
            "当前公开文件",
            "当前公开文件",
            str(len(etf_tracking[etf_tracking["指数代码"].eq("399006")])),
            str(len(etf_tracking[etf_tracking["指数代码"].eq("930986")])),
            "未将名称相似但跟踪其他指数的ETF纳入",
            STATUS_OK,
            "跟踪关系可确认，但本阶段不以产品名称单独作为确认依据。",
        ),
        report_table_row(
            "ETF历史份额",
            STATUS_FAILED,
            STATUS_FAILED,
            "深交所、上交所、东方财富公开接口",
            "fund_etf_scale_szse；fund_etf_scale_sse；fund_etf_spot_em",
            "仅当前份额",
            "仅部分上交所产品可按日取得",
            "不足21个完整聚合交易日",
            "不足21个完整聚合交易日",
            "深交所历史份额接口解析失败；跨市场总份额不完整",
            STATUS_FAILED,
            "不使用成交量、基金规模或部分ETF份额替代，ETFShare20及分位均为NA。",
        ),
    ]

    checks = []
    for code in INDEX_SPECS:
        if validations[code]:
            checks.append(f"- {code}：" + "；".join(validations[code]))
        else:
            checks.append(f"- {code}：必要检查通过。")

    direct_metrics = "RET20、BIAS20、RS20、VolumeStrength、BreadthMA20、HLBreadth、Sync"
    report = f"""# 指数拥挤度公开数据可行性报告

生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 结论

- 两个指数的行情、当前成分股、权重和成分股历史行情均可取得，能够直接计算 {direct_metrics}。
- 融资余额能够按当前成分股聚合，并计算最近5个交易日的20日变化率；由于仅抓取25个交易日，Margin20历史百分位为 `NA`。
- ETF跟踪关系可以确认，但无法从公开接口稳定取得覆盖全部已确认ETF的连续历史份额，因此 ETFShare20 原值和历史百分位均为 `NA`。
- 399006国证官方历史行情当前更新到 {index_histories['399006']['日期'].max().date()}，比系统日期存在发布时滞；结果严格使用该官方数据源最新5个有效交易日。

## 数据获取情况

| 数据项 | 399006 | 930986 | 数据来源 | 具体接口/函数 | 399006历史起止 | 930986历史起止 | 399006数据量 | 930986数据量 | 缺失情况 | 是否稳定 | 备注 |
|---|---|---|---|---|---|---|---:|---:|---|---|---|
{chr(10).join(rows)}

## 指标可计算性

| 指标 | 399006 | 930986 | 说明 |
|---|---|---|---|
| RET20 | {STATUS_OK} | {STATUS_OK} | 官方指数收盘价可得。 |
| BIAS20 | {STATUS_OK} | {STATUS_OK} | 官方指数收盘价可得。 |
| RS20 | {STATUS_OK} | {STATUS_OK} | 沪深300中证官方行情可得，并按共同交易日对齐。 |
| VolumeStrength | {STATUS_OK} | {STATUS_OK} | 指数成交额可得，已统一为元。 |
| BreadthMA20 | {STATUS_OK} | {STATUS_OK} | 使用当前成分股和未复权收盘价。 |
| HLBreadth | {STATUS_OK} | {STATUS_OK} | 使用当前成分股和未复权收盘价。 |
| Margin20原值 | {STATUS_LIMITED} | {STATUS_LIMITED} | 可聚合最近25个交易日；非融资标的自然不在交易所明细中。 |
| Margin20历史百分位 | {STATUS_FAILED} | {STATUS_FAILED} | 未抓取756日逐日融资明细。 |
| ETFShare20 | {STATUS_FAILED} | {STATUS_FAILED} | 缺少覆盖全部确认ETF的连续日份额。 |
| Sync | {STATUS_OK} | {STATUS_OK} | 每个窗口仅纳入20日收益率完整的成分股。 |

## 当前成分股回算限制

使用当前成分股回算历史指标可能产生一定幸存者偏差，本阶段仅用于数据获取可行性测试。

## 历史百分位口径

- 每日仅使用该日及以前、最长756个交易日的指标历史值。
- 有效历史值少于252个时记为 `NA`。
- 并列值采用中位秩百分位，不使用未来数据。
- Margin20和ETFShare20由于可用历史不足，分位数保持 `NA`。

## 必要数据检查

{chr(10).join(checks)}

- 成分股数量：399006为{len(constituents['399006'])}只，930986为{len(constituents['930986'])}只，和官方最新文件一致。
- 成分股行情成功数：399006为{history_success['399006']}只，930986为{history_success['930986']}只。
- 最新MA20有效样本：399006为{metric_counts['399006']['最新MA20有效数']}只，930986为{metric_counts['930986']['最新MA20有效数']}只。
- 输出CSV中的缺失结果使用 `NA`，没有用0或其他字段替代。

## 接口稳定性风险

- 东方财富历史K线接口在本次验证中出现代理断开，未作为最终成分股历史行情来源。
- 新浪历史行情大量逐只请求可能触发限流，已使用串行请求与本地缓存。
- 国证399006历史行情存在约一个交易日的发布时滞。
- 深交所ETF份额接口当前返回内容与AKShare解析格式不一致，无法可靠形成日历史。
- 交易所融资明细按日请求适合短窗口验证，不适合在当前简单脚本中无节制回溯756日。

## V1建议

第一版适合纳入：RET20、BIAS20、RS20、VolumeStrength、BreadthMA20、HLBreadth、Sync。这些指标数据覆盖较完整，来源可追溯，且已能形成三年滚动分位。

暂不建议纳入V1：Margin20历史分位和ETFShare20。融资原值短窗口可算，但三年逐日抓取成本和稳定性较差；ETF历史份额无法形成完整、连续、跨市场的指数级汇总。
"""
    (ROOT / "data_feasibility_report.md").write_text(report, encoding="utf-8")


def initialize_database(connection: duckdb.DuckDBPyConnection) -> None:
    """创建盘后版所需表。所有业务表都使用稳定业务键防止重复。"""
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS index_daily (
            index_code VARCHAR NOT NULL,
            date DATE NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            pct_change DOUBLE,
            volume DOUBLE,
            amount DOUBLE,
            source VARCHAR,
            fetched_at TIMESTAMP,
            PRIMARY KEY (index_code, date)
        );
        CREATE TABLE IF NOT EXISTS stock_daily (
            stock_code VARCHAR NOT NULL,
            date DATE NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            pct_change DOUBLE,
            volume DOUBLE,
            amount DOUBLE,
            source VARCHAR,
            fetched_at TIMESTAMP,
            PRIMARY KEY (stock_code, date)
        );
        CREATE TABLE IF NOT EXISTS index_constituents (
            index_code VARCHAR NOT NULL,
            stock_code VARCHAR NOT NULL,
            stock_name VARCHAR,
            weight DOUBLE,
            exchange VARCHAR,
            effective_date DATE NOT NULL,
            source VARCHAR,
            fetched_at TIMESTAMP,
            PRIMARY KEY (index_code, stock_code, effective_date)
        );
        CREATE TABLE IF NOT EXISTS margin_daily (
            index_code VARCHAR NOT NULL,
            date DATE NOT NULL,
            margin_balance DOUBLE,
            matched_count INTEGER,
            source VARCHAR,
            fetched_at TIMESTAMP,
            PRIMARY KEY (index_code, date)
        );
        CREATE TABLE IF NOT EXISTS indicator_daily (
            index_code VARCHAR NOT NULL,
            date DATE NOT NULL,
            formula_version VARCHAR NOT NULL DEFAULT 'V1.0',
            constituent_mode VARCHAR NOT NULL DEFAULT 'current_constituents_proxy',
            RET20_raw DOUBLE,
            RET20_score DOUBLE,
            BIAS20_raw DOUBLE,
            BIAS20_score DOUBLE,
            RS20_raw DOUBLE,
            RS20_score DOUBLE,
            VolumeStrength_raw DOUBLE,
            VolumeStrength_score DOUBLE,
            BreadthMA20_raw DOUBLE,
            BreadthMA20_score DOUBLE,
            HLBreadth_raw DOUBLE,
            HLBreadth_score DOUBLE,
            Sync_raw DOUBLE,
            Sync_score DOUBLE,
            temperature DOUBLE,
            temperature_change_1d DOUBLE,
            temperature_level VARCHAR,
            calculated_at TIMESTAMP,
            PRIMARY KEY (index_code, date, formula_version)
        );
        CREATE TABLE IF NOT EXISTS update_log (
            run_id VARCHAR,
            logged_at TIMESTAMP,
            action VARCHAR,
            entity_type VARCHAR,
            entity_code VARCHAR,
            start_date DATE,
            end_date DATE,
            row_count INTEGER,
            status VARCHAR,
            message VARCHAR
        );
        """
    )


def insert_frame(
    connection: duckdb.DuckDBPyConnection,
    table: str,
    frame: pd.DataFrame,
    columns: list[str],
) -> int:
    """只插入不存在的业务键，默认不覆盖历史数据。"""
    if frame.empty:
        return 0
    before = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    incoming = frame[columns].copy()
    connection.register("incoming_frame", incoming)
    quoted = ", ".join(f'"{column}"' for column in columns)
    try:
        connection.execute(
            f"INSERT OR IGNORE INTO {table} ({quoted}) SELECT {quoted} FROM incoming_frame"
        )
    finally:
        connection.unregister("incoming_frame")
    after = int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
    return after - before


def log_update(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    action: str,
    entity_type: str,
    entity_code: str,
    row_count: int,
    status: str,
    message: str = "",
    start_date: object | None = None,
    end_date: object | None = None,
) -> None:
    connection.execute(
        "INSERT INTO update_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            run_id,
            datetime.now(),
            action,
            entity_type,
            entity_code,
            start_date,
            end_date,
            row_count,
            status,
            message,
        ],
    )


def temperature_level(value: float) -> str | None:
    if pd.isna(value):
        return None
    if value < 10:
        return "冰点"
    if value < 20:
        return "恐惧"
    if value < 40:
        return "偏冷"
    if value < 60:
        return "中性"
    if value < 80:
        return "偏热"
    if value < 90:
        return "贪婪"
    return "狂热"


def add_temperature_columns(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    score_columns = list(TEMPERATURE_WEIGHTS)
    valid = result[score_columns].notna().all(axis=1)
    weighted = sum(result[column] * weight for column, weight in TEMPERATURE_WEIGHTS.items())
    result["temperature"] = weighted.where(valid).round(1)
    result["temperature_change_1d"] = result["temperature"].diff().round(1)
    result["temperature_level"] = result["temperature"].map(temperature_level)
    return result


def migrate_existing_files(
    connection: duckdb.DuckDBPyConnection, run_id: str
) -> dict[str, int]:
    """首次运行仅导入现有缓存，不重新下载已有历史。"""
    now = datetime.now()
    counts = {
        "index_daily": 0,
        "stock_daily": 0,
        "index_constituents": 0,
        "margin_daily": 0,
        "indicator_daily": 0,
    }
    index_columns = [
        "index_code", "date", "open", "high", "low", "close", "pct_change",
        "volume", "amount", "source", "fetched_at",
    ]
    for code in [*INDEX_SPECS, "000300"]:
        path = DATA_DIR / f"index_{code}.csv"
        if not path.exists():
            continue
        cached = pd.read_csv(path, encoding="utf-8-sig")
        migrated = pd.DataFrame(
            {
                "index_code": code,
                "date": pd.to_datetime(cached["日期"], errors="coerce"),
                "open": pd.to_numeric(cached.get("开盘"), errors="coerce"),
                "high": pd.to_numeric(cached.get("最高"), errors="coerce"),
                "low": pd.to_numeric(cached.get("最低"), errors="coerce"),
                "close": pd.to_numeric(cached.get("收盘"), errors="coerce"),
                "pct_change": pd.to_numeric(cached.get("涨跌幅"), errors="coerce"),
                "volume": pd.to_numeric(cached.get("成交量"), errors="coerce"),
                "amount": pd.to_numeric(cached.get("成交额"), errors="coerce"),
                "source": "现有CSV缓存",
                "fetched_at": now,
            }
        ).dropna(subset=["date"])
        counts["index_daily"] += insert_frame(
            connection, "index_daily", migrated, index_columns
        )

    stock_columns = [
        "stock_code", "date", "open", "high", "low", "close", "pct_change",
        "volume", "amount", "source", "fetched_at",
    ]
    for path in sorted(STOCK_CACHE_DIR.glob("*.csv")):
        cached = pd.read_csv(path, dtype={"股票代码": str}, encoding="utf-8-sig")
        migrated = pd.DataFrame(
            {
                "stock_code": cached["股票代码"].astype(str).str.zfill(6),
                "date": pd.to_datetime(cached["日期"], errors="coerce"),
                "open": pd.to_numeric(cached.get("开盘"), errors="coerce"),
                "high": pd.to_numeric(cached.get("最高"), errors="coerce"),
                "low": pd.to_numeric(cached.get("最低"), errors="coerce"),
                "close": pd.to_numeric(cached.get("收盘"), errors="coerce"),
                "pct_change": pd.to_numeric(cached.get("涨跌幅"), errors="coerce"),
                "volume": pd.to_numeric(cached.get("成交量"), errors="coerce"),
                "amount": pd.to_numeric(cached.get("成交额"), errors="coerce"),
                "source": "现有CSV缓存",
                "fetched_at": now,
            }
        ).dropna(subset=["date"])
        counts["stock_daily"] += insert_frame(
            connection, "stock_daily", migrated, stock_columns
        )

    constituent_columns = [
        "index_code", "stock_code", "stock_name", "weight", "exchange",
        "effective_date", "source", "fetched_at",
    ]
    for code in INDEX_SPECS:
        path = DATA_DIR / f"constituents_{code}.csv"
        if not path.exists():
            continue
        cached = pd.read_csv(path, dtype={"股票代码": str}, encoding="utf-8-sig")
        migrated = pd.DataFrame(
            {
                "index_code": code,
                "stock_code": cached["股票代码"].astype(str).str.zfill(6),
                "stock_name": cached["股票名称"],
                "weight": pd.to_numeric(cached["当前权重"], errors="coerce"),
                "exchange": cached["所属交易所"],
                "effective_date": pd.to_datetime(cached["数据日期"], errors="coerce"),
                "source": INDEX_SPECS[code].history_source,
                "fetched_at": now,
            }
        ).dropna(subset=["effective_date"])
        counts["index_constituents"] += insert_frame(
            connection, "index_constituents", migrated, constituent_columns
        )

    margin_columns = [
        "index_code", "date", "margin_balance", "matched_count", "source", "fetched_at"
    ]
    for code in INDEX_SPECS:
        path = DATA_DIR / f"margin_aggregate_{code}.csv"
        if not path.exists():
            continue
        cached = pd.read_csv(path, encoding="utf-8-sig")
        migrated = pd.DataFrame(
            {
                "index_code": code,
                "date": pd.to_datetime(cached["日期"], errors="coerce"),
                "margin_balance": pd.to_numeric(cached["融资余额合计"], errors="coerce"),
                "matched_count": pd.to_numeric(cached["匹配证券数"], errors="coerce"),
                "source": "现有融资聚合CSV",
                "fetched_at": now,
            }
        ).dropna(subset=["date"])
        counts["margin_daily"] += insert_frame(
            connection, "margin_daily", migrated, margin_columns
        )

    indicator_columns = [
        "index_code", "date", "formula_version", "constituent_mode",
        "RET20_raw", "RET20_score", "BIAS20_raw",
        "BIAS20_score", "RS20_raw", "RS20_score", "VolumeStrength_raw",
        "VolumeStrength_score", "BreadthMA20_raw", "BreadthMA20_score",
        "HLBreadth_raw", "HLBreadth_score", "Sync_raw", "Sync_score",
        "temperature", "temperature_change_1d", "temperature_level", "calculated_at",
    ]
    for code in INDEX_SPECS:
        path = DATA_DIR / f"metrics_history_{code}.csv"
        if not path.exists():
            continue
        cached = pd.read_csv(path, na_values=["NA"], encoding="utf-8-sig")
        migrated = pd.DataFrame({"index_code": code, "date": pd.to_datetime(cached["日期"])})
        for column in [
            "RET20_raw", "RET20_score", "BIAS20_raw", "BIAS20_score",
            "RS20_raw", "RS20_score", "VolumeStrength_raw", "VolumeStrength_score",
            "BreadthMA20_raw", "BreadthMA20_score", "HLBreadth_raw",
            "HLBreadth_score", "Sync_raw", "Sync_score",
        ]:
            migrated[column] = pd.to_numeric(cached[column], errors="coerce")
        migrated = add_temperature_columns(migrated)
        migrated["formula_version"] = "V1.0"
        migrated["constituent_mode"] = "current_constituents_proxy"
        migrated["calculated_at"] = now
        counts["indicator_daily"] += insert_frame(
            connection, "indicator_daily", migrated, indicator_columns
        )

    for table, count in counts.items():
        if count:
            log_update(
                connection, run_id, "migrate", table, table, count, "success",
                "由现有CSV缓存导入，未发起历史全量下载",
            )
    return counts


def latest_date(
    connection: duckdb.DuckDBPyConnection, table: str, code_column: str, code: str
) -> pd.Timestamp | None:
    value = connection.execute(
        f"SELECT MAX(date) FROM {table} WHERE {code_column} = ?", [code]
    ).fetchone()[0]
    return pd.Timestamp(value) if value is not None else None


def fetch_index_range(code: str, start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
    start = start_date.strftime("%Y%m%d")
    end = end_date.strftime("%Y%m%d")
    if code == "399006":
        raw = call_with_retry(lambda: ak.index_hist_cni(symbol=code, start_date=start, end_date=end))
        if raw.empty:
            return pd.DataFrame()
        data = raw.rename(
            columns={"开盘价": "开盘", "最高价": "最高", "最低价": "最低", "收盘价": "收盘"}
        )
        data["成交量"] = pd.to_numeric(data["成交量"], errors="coerce") * 1_000_000
        data["成交额"] = pd.to_numeric(data["成交额"], errors="coerce") * 100_000_000
    elif code in ("930986", "000300"):
        raw = call_with_retry(
            lambda: ak.stock_zh_index_hist_csindex(symbol=code, start_date=start, end_date=end)
        )
        if raw.empty:
            return pd.DataFrame()
        data = raw.rename(columns={"成交金额": "成交额"})
        if "成交额" in data:
            data["成交额"] = pd.to_numeric(data["成交额"], errors="coerce") * 100_000_000
    else:
        raise ValueError(f"不支持的指数代码：{code}")
    keep = ["日期", "开盘", "最高", "最低", "收盘", "涨跌幅", "成交量", "成交额"]
    for column in keep:
        if column not in data:
            data[column] = np.nan
    data = clean_date_index(data[keep])
    for column in keep[1:]:
        data[column] = pd.to_numeric(data[column], errors="coerce")
    return data


def locked_index_daily_source(
    connection: duckdb.DuckDBPyConnection, code: str
) -> tuple[str | None, str | None]:
    registered = connection.execute(
        "SELECT primary_source, source_symbol FROM index_registry WHERE index_code = ?", [code]
    ).fetchone()
    if registered is not None:
        return registered
    benchmark = connection.execute(
        "SELECT primary_source, source_symbol FROM data_source_lock "
        "WHERE entity_type = 'index_benchmark' AND canonical_code = ?", [code]
    ).fetchone()
    return benchmark if benchmark is not None else (None, None)


def fetch_hithink_index_range(
    symbol: str, start_date: pd.Timestamp, end_date: pd.Timestamp, previous_close: float
) -> pd.DataFrame:
    # 仅在正式库已锁定同花顺代码后取缺口，不推测供应商代码。
    key = os.getenv("HITHINK_FINANCE_API_KEY")
    if not key:
        raise RuntimeError("缺少 HITHINK_FINANCE_API_KEY，无法从已锁定的同花顺主源增量取数")
    if not np.isfinite(previous_close) or previous_close <= 0:
        raise ValueError("缺少有效前收盘价，不能计算新增指数日涨跌幅")
    begin = pd.Timestamp(start_date).tz_localize("Asia/Shanghai")
    finish = (pd.Timestamp(end_date) + pd.Timedelta(days=1)).tz_localize("Asia/Shanghai")
    query = urlencode({
        "thscode": symbol, "interval": "1d",
        "start": int(begin.timestamp() * 1000),
        "end": int(finish.timestamp() * 1000),
    })
    request = Request(
        "https://fuyao.aicubes.cn/api/a-share-index/prices/historical?" + query,
        headers={"X-api-key": key},
    )
    with urlopen(request, timeout=30) as response:
        payload = json.load(response)
    if payload.get("code") != 0:
        raise RuntimeError(f"同花顺指数日线获取失败：code={payload.get('code')}")
    rows = []
    for bar in payload["data"]["item"]:
        date = pd.to_datetime(bar["date_ms"], unit="ms", utc=True).tz_convert("Asia/Shanghai").date()
        if pd.Timestamp(start_date).date() <= date <= pd.Timestamp(end_date).date():
            rows.append({
                "日期": pd.Timestamp(date), "开盘": bar["open_price"],
                "最高": bar["high_price"], "最低": bar["low_price"],
                "收盘": bar["close_price"], "成交量": bar.get("volume"),
                "成交额": bar.get("turnover"),
            })
    data = pd.DataFrame(rows)
    if data.empty:
        return data
    data = data.sort_values("日期").drop_duplicates("日期")
    data["涨跌幅"] = (data["收盘"] / data["收盘"].shift(1) - 1) * 100
    data.loc[data.index[0], "涨跌幅"] = (data.iloc[0]["收盘"] / previous_close - 1) * 100
    return data[["日期", "开盘", "最高", "最低", "收盘", "涨跌幅", "成交量", "成交额"]]


def append_index_cache(code: str, new_data: pd.DataFrame) -> None:
    if new_data.empty:
        return
    path = DATA_DIR / f"index_{code}.csv"
    cached = pd.read_csv(path, encoding="utf-8-sig") if path.exists() else pd.DataFrame()
    combined = pd.concat([cached, new_data], ignore_index=True)
    combined = clean_date_index(combined)
    combined.to_csv(path, index=False, encoding="utf-8-sig")


def update_index_daily(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    code: str,
    target_date: pd.Timestamp,
    failures: list[str],
) -> int:
    local_max = latest_date(connection, "index_daily", "index_code", code)
    start_date = pd.Timestamp(START_DATE) if local_max is None else local_max + pd.Timedelta(days=1)
    if start_date > target_date:
        return 0
    try:
        locked_source, locked_symbol = locked_index_daily_source(connection, code)
        if locked_source == "同花顺金融数据服务":
            if not locked_symbol:
                raise RuntimeError(f"{code} 已锁定同花顺主源但缺少source_symbol")
            previous_close = connection.execute(
                "SELECT close FROM index_daily WHERE index_code = ? AND date = ?",
                [code, local_max.date()],
            ).fetchone()[0]
            data = fetch_hithink_index_range(locked_symbol, start_date, target_date, previous_close)
            source = locked_source
        else:
            data = fetch_index_range(code, start_date, target_date)
            source = INDEX_SPECS[code].history_source if code in INDEX_SPECS else "中证指数官网（AKShare stock_zh_index_hist_csindex）"
        if data.empty:
            log_update(
                connection, run_id, "incremental_fetch", "index", code, 0, "no_data",
                "接口未返回新交易日",
                start_date.date(), target_date.date(),
            )
            return 0
        incoming = pd.DataFrame(
            {
                "index_code": code,
                "date": data["日期"],
                "open": data["开盘"],
                "high": data["最高"],
                "low": data["最低"],
                "close": data["收盘"],
                "pct_change": data["涨跌幅"],
                "volume": data["成交量"],
                "amount": data["成交额"],
                "source": source,
                "source_symbol": locked_symbol if locked_source == "同花顺金融数据服务" else None,
                "fetched_at": datetime.now(),
            }
        )
        columns = list(incoming.columns)
        added = insert_frame(connection, "index_daily", incoming, columns)
        append_index_cache(code, data)
        log_update(
            connection, run_id, "incremental_fetch", "index", code, added, "success",
            "", start_date.date(), target_date.date(),
        )
        return added
    except Exception as exc:  # noqa: BLE001
        if code == "399006" and isinstance(exc, ValueError) and "Length mismatch" in str(exc):
            log_update(
                connection,
                run_id,
                "incremental_fetch",
                "index",
                code,
                0,
                "no_data",
                "国证官方源尚未返回新交易日，按数据滞后记录",
                start_date.date(),
                target_date.date(),
            )
            return 0
        message = f"{code}: {type(exc).__name__}: {exc}"
        failures.append(message)
        log_update(
            connection, run_id, "incremental_fetch", "index", code, 0, "failed",
            message, start_date.date(), target_date.date(),
        )
        return 0


def latest_constituents_from_database(
    connection: duckdb.DuckDBPyConnection, code: str
) -> pd.DataFrame:
    frame = connection.execute(
        """
        SELECT stock_code AS 股票代码, stock_name AS 股票名称,
               weight AS 当前权重, exchange AS 所属交易所,
               CAST(effective_date AS VARCHAR) AS 数据日期
        FROM index_constituents
        WHERE index_code = ?
          AND effective_date = (
              SELECT MAX(effective_date) FROM index_constituents WHERE index_code = ?
          )
        ORDER BY stock_code
        """,
        [code, code],
    ).df()
    if not frame.empty:
        frame["股票代码"] = frame["股票代码"].astype(str).str.zfill(6)
    return frame


def refresh_constituents(
    connection: duckdb.DuckDBPyConnection, run_id: str, failures: list[str]
) -> dict[str, pd.DataFrame]:
    results: dict[str, pd.DataFrame] = {}
    columns = [
        "index_code", "stock_code", "stock_name", "weight", "exchange",
        "effective_date", "source", "fetched_at",
    ]
    for code in INDEX_SPECS:
        try:
            frame = fetch_constituents(code)
            incoming = pd.DataFrame(
                {
                    "index_code": code,
                    "stock_code": frame["股票代码"],
                    "stock_name": frame["股票名称"],
                    "weight": frame["当前权重"],
                    "exchange": frame["所属交易所"],
                    "effective_date": pd.to_datetime(frame["数据日期"]),
                    "source": INDEX_SPECS[code].history_source,
                    "fetched_at": datetime.now(),
                }
            )
            added = insert_frame(connection, "index_constituents", incoming, columns)
            log_update(
                connection, run_id, "refresh", "constituents", code, added, "success",
                "官方当前成分股检查完成",
            )
            results[code] = frame
        except Exception as exc:  # noqa: BLE001
            message = f"{code}成分股: {type(exc).__name__}: {exc}"
            failures.append(message)
            log_update(
                connection, run_id, "refresh", "constituents", code, 0, "failed", message
            )
            results[code] = latest_constituents_from_database(connection, code)
    return results


def fetch_stock_range(
    code: str, start_date: pd.Timestamp, end_date: pd.Timestamp, previous_close: float | None
) -> pd.DataFrame:
    raw = call_with_retry(
        lambda: ak.stock_zh_a_daily(
            symbol=f"{market_prefix(code)}{code}",
            start_date=start_date.strftime("%Y%m%d"),
            end_date=end_date.strftime("%Y%m%d"),
            adjust="",
        )
    )
    if raw.empty:
        return pd.DataFrame()
    data = raw.rename(
        columns={
            "date": "日期", "open": "开盘", "high": "最高", "low": "最低",
            "close": "收盘", "volume": "成交量", "amount": "成交额",
        }
    )
    data = clean_date_index(data)
    for column in ("开盘", "最高", "最低", "收盘", "成交量", "成交额"):
        data[column] = pd.to_numeric(data[column], errors="coerce")
    prior = pd.Series([previous_close, *data["收盘"].iloc[:-1].tolist()], index=data.index)
    data["涨跌幅"] = data["收盘"] / prior - 1
    data.insert(0, "股票代码", code)
    return data[["股票代码", "日期", "开盘", "最高", "最低", "收盘", "涨跌幅", "成交量", "成交额"]]


def append_stock_cache(code: str, new_data: pd.DataFrame) -> None:
    if new_data.empty:
        return
    path = STOCK_CACHE_DIR / f"{code}.csv"
    cached = pd.read_csv(path, dtype={"股票代码": str}, encoding="utf-8-sig") if path.exists() else pd.DataFrame()
    combined = pd.concat([cached, new_data], ignore_index=True)
    combined["股票代码"] = combined["股票代码"].astype(str).str.zfill(6)
    combined["日期"] = pd.to_datetime(combined["日期"], errors="coerce")
    combined = combined.dropna(subset=["日期"]).sort_values("日期").drop_duplicates("日期", keep="last")
    combined.to_csv(path, index=False, encoding="utf-8-sig")


def update_stock_daily(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    constituents: dict[str, pd.DataFrame],
    target_date: pd.Timestamp,
) -> tuple[int, dict[str, str]]:
    codes = sorted(set().union(*(set(frame["股票代码"]) for frame in constituents.values())))
    local_dates = {
        str(code): pd.Timestamp(date) if date is not None else None
        for code, date in connection.execute(
            "SELECT stock_code, MAX(date) FROM stock_daily GROUP BY stock_code"
        ).fetchall()
    }
    failures: dict[str, str] = {}
    total_added = 0
    columns = [
        "stock_code", "date", "open", "high", "low", "close", "pct_change",
        "volume", "amount", "source", "fetched_at",
    ]
    request_count = 0
    for code in codes:
        local_max = local_dates.get(code)
        start_date = pd.Timestamp(START_DATE) if local_max is None else local_max + pd.Timedelta(days=1)
        if start_date > target_date:
            continue
        request_count += 1
        previous_close_row = connection.execute(
            "SELECT close FROM stock_daily WHERE stock_code = ? ORDER BY date DESC LIMIT 1", [code]
        ).fetchone()
        previous_close = previous_close_row[0] if previous_close_row else None
        try:
            data = fetch_stock_range(code, start_date, target_date, previous_close)
            if data.empty:
                continue
            incoming = pd.DataFrame(
                {
                    "stock_code": code,
                    "date": data["日期"],
                    "open": data["开盘"],
                    "high": data["最高"],
                    "low": data["最低"],
                    "close": data["收盘"],
                    "pct_change": data["涨跌幅"],
                    "volume": data["成交量"],
                    "amount": data["成交额"],
                    "source": "新浪财经（AKShare stock_zh_a_daily）",
                    "fetched_at": datetime.now(),
                }
            )
            added = insert_frame(connection, "stock_daily", incoming, columns)
            total_added += added
            append_stock_cache(code, data)
        except Exception as exc:  # noqa: BLE001
            failures[code] = f"{type(exc).__name__}: {exc}"
        time.sleep(0.08)
    log_update(
        connection,
        run_id,
        "incremental_fetch",
        "stock",
        "current_constituents",
        total_added,
        "success" if not failures else "partial",
        f"实际请求{request_count}只，失败{len(failures)}只",
    )
    (DATA_DIR / "stock_history_failures.json").write_text(
        json.dumps(failures, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return total_added, failures


def load_index_history_from_database(
    connection: duckdb.DuckDBPyConnection, code: str
) -> pd.DataFrame:
    frame = connection.execute(
        """
        SELECT date AS 日期, open AS 开盘, high AS 最高, low AS 最低,
               close AS 收盘, pct_change AS 涨跌幅, volume AS 成交量, amount AS 成交额
        FROM index_daily WHERE index_code = ? ORDER BY date
        """,
        [code],
    ).df()
    frame["日期"] = pd.to_datetime(frame["日期"])
    return frame


def load_stock_histories_from_database(
    connection: duckdb.DuckDBPyConnection, codes: set[str]
) -> dict[str, pd.DataFrame]:
    histories: dict[str, pd.DataFrame] = {}
    for code in sorted(codes):
        frame = connection.execute(
            """
            SELECT stock_code AS 股票代码, date AS 日期, close AS 收盘,
                   pct_change AS 涨跌幅, volume AS 成交量, amount AS 成交额
            FROM stock_daily WHERE stock_code = ? ORDER BY date
            """,
            [code],
        ).df()
        if not frame.empty:
            frame["日期"] = pd.to_datetime(frame["日期"])
            histories[code] = frame
    return histories


def load_margin_from_database(
    connection: duckdb.DuckDBPyConnection, code: str
) -> pd.DataFrame:
    frame = connection.execute(
        "SELECT date AS 日期, margin_balance AS 融资余额合计 FROM margin_daily WHERE index_code = ? ORDER BY date",
        [code],
    ).df()
    if not frame.empty:
        frame["日期"] = pd.to_datetime(frame["日期"])
    return frame


def update_indicators(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    constituents: dict[str, pd.DataFrame],
) -> tuple[dict[str, pd.DataFrame], dict[str, dict[str, int]], dict[str, int]]:
    index_histories = {
        code: load_index_history_from_database(connection, code) for code in INDEX_SPECS
    }
    benchmark = load_index_history_from_database(connection, "000300")[["日期", "收盘"]]
    member_codes = set().union(*(set(frame["股票代码"]) for frame in constituents.values()))
    histories = load_stock_histories_from_database(connection, member_codes)
    metric_counts: dict[str, dict[str, int]] = {}
    added_counts: dict[str, int] = {}
    insert_columns = [
        "index_code", "date", "formula_version", "constituent_mode",
        "RET20_raw", "RET20_score", "BIAS20_raw",
        "BIAS20_score", "RS20_raw", "RS20_score", "VolumeStrength_raw",
        "VolumeStrength_score", "BreadthMA20_raw", "BreadthMA20_score",
        "HLBreadth_raw", "HLBreadth_score", "Sync_raw", "Sync_score",
        "temperature", "temperature_change_1d", "temperature_level", "calculated_at",
    ]
    for code in INDEX_SPECS:
        full, metric_counts[code] = compute_metrics(
            code,
            index_histories[code],
            benchmark,
            constituents[code],
            histories,
            load_margin_from_database(connection, code),
            write_legacy_outputs=False,
            latest_only=False,
        )
        full = add_temperature_columns(full)
        incoming = full.rename(columns={"日期": "date"}).copy()
        incoming.insert(0, "index_code", code)
        incoming["formula_version"] = "V1.0"
        incoming["constituent_mode"] = "current_constituents_proxy"
        incoming["calculated_at"] = datetime.now()
        added_counts[code] = insert_frame(
            connection, "indicator_daily", incoming, insert_columns
        )
        log_update(
            connection,
            run_id,
            "calculate",
            "indicator",
            code,
            added_counts[code],
            "success",
            "只写入数据库中缺少的指标日期",
        )
    return index_histories, metric_counts, added_counts


def load_latest_results(
    connection: duckdb.DuckDBPyConnection, code: str
) -> pd.DataFrame:
    frame = connection.execute(
        """
        SELECT date AS 日期,
               RET20_raw, RET20_score, BIAS20_raw, BIAS20_score,
               RS20_raw, RS20_score, VolumeStrength_raw, VolumeStrength_score,
               BreadthMA20_raw, BreadthMA20_score, HLBreadth_raw, HLBreadth_score,
               Sync_raw, Sync_score, temperature, temperature_change_1d, temperature_level
        FROM indicator_daily WHERE index_code = ? AND formula_version = 'V1.0'
        ORDER BY date DESC LIMIT 5
        """,
        [code],
    ).df().sort_values("日期").reset_index(drop=True)
    frame["日期"] = pd.to_datetime(frame["日期"])
    return frame[TEMPERATURE_RESULT_COLUMNS]


def validate_temperature_outputs(
    connection: duckdb.DuckDBPyConnection,
    results: dict[str, pd.DataFrame],
    index_histories: dict[str, pd.DataFrame],
) -> tuple[dict[str, list[str]], list[str]]:
    findings: dict[str, list[str]] = {}
    anomalies: list[str] = []
    stock_ohl_null = connection.execute(
        """
        SELECT AVG(CASE WHEN open IS NULL OR high IS NULL OR low IS NULL THEN 1.0 ELSE 0.0 END)
        FROM stock_daily
        """
    ).fetchone()[0]
    if stock_ohl_null is not None and stock_ohl_null > 0.01:
        anomalies.append(
            f"现有成分股CSV未保存历史开高低，迁移后相关空值比例为{stock_ohl_null:.1%}；7项指标仅使用收盘等已有字段，不受影响"
        )
    for code, result in results.items():
        issues: list[str] = []
        history = index_histories[code]
        if not history["日期"].is_monotonic_increasing:
            issues.append("指数日期不是升序")
        if history["日期"].duplicated().any():
            issues.append("指数日期存在重复")
        if history[["开盘", "最高", "最低", "收盘"]].isna().mean().max() > 0.01:
            issues.append("指数OHLC空值超过1%")
        score_columns = [column for column in result if column.endswith("_score")]
        if any(((result[column] < 0) | (result[column] > 100)).any() for column in score_columns):
            issues.append("百分位超出0到100")
        if ((result["BreadthMA20_raw"] < 0) | (result["BreadthMA20_raw"] > 1)).any():
            issues.append("BreadthMA20超出0到1")
        if ((result["HLBreadth_raw"] < -1) | (result["HLBreadth_raw"] > 1)).any():
            issues.append("HLBreadth超出-1到1")
        if ((result["Sync_raw"] < -1.000001) | (result["Sync_raw"] > 1.000001)).any():
            issues.append("Sync超出-1到1")
        if ((result["temperature"] < 0) | (result["temperature"] > 100)).any():
            issues.append("temperature超出0到100")
        expected = history["日期"].tail(5).dt.normalize().tolist()
        actual = result["日期"].dt.normalize().tolist()
        if actual != expected:
            issues.append("结果日期不是该指数最新5个有效交易日")
        findings[code] = issues
    latest_dates = {code: frame["日期"].max() for code, frame in index_histories.items()}
    if len(set(latest_dates.values())) > 1:
        anomalies.append(
            "两个指数最新日期不一致，横向比较应使用共同最新交易日"
        )
    return findings, anomalies


def build_summary_rows(
    results: dict[str, pd.DataFrame], index_histories: dict[str, pd.DataFrame]
) -> list[dict[str, object]]:
    latest_dates = {code: frame["日期"].max() for code, frame in index_histories.items()}
    newest = max(latest_dates.values())
    rows = []
    for code, spec in INDEX_SPECS.items():
        latest = results[code].iloc[-1]
        lag_days = int((newest - latest_dates[code]).days)
        rows.append(
            {
                "指数代码": code,
                "指数名称": spec.name,
                "数据日期": latest_dates[code].strftime("%Y-%m-%d"),
                "当前温度": None if pd.isna(latest["temperature"]) else float(latest["temperature"]),
                "较上一日变化": None if pd.isna(latest["temperature_change_1d"]) else float(latest["temperature_change_1d"]),
                "温度区间": latest["temperature_level"],
                "数据状态": "最新" if lag_days == 0 else f"滞后{lag_days}日",
            }
        )
    return rows


def write_outputs(
    results: dict[str, pd.DataFrame], summary_rows: list[dict[str, object]]
) -> None:
    for code, frame in results.items():
        output = frame.copy()
        output["日期"] = output["日期"].dt.strftime("%Y-%m-%d")
        output.to_csv(
            OUTPUT_DIR / f"index_temperature_{code}_latest5.csv",
            index=False,
            na_rep="NA",
            encoding="utf-8-sig",
        )
    (OUTPUT_DIR / "temperature_summary.json").write_text(
        json.dumps(summary_rows, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def create_temperature_workbook() -> None:
    node = shutil.which("node")
    if node is None:
        raise RuntimeError("未找到Node.js，无法生成Excel工作簿")
    completed = subprocess.run(
        [node, str(ROOT / "build_workbook.mjs")],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    (DATA_DIR / "workbook_build_log.txt").write_text(
        completed.stdout + completed.stderr, encoding="utf-8"
    )
    if completed.returncode != 0:
        raise RuntimeError(f"Excel生成失败，详见{DATA_DIR / 'workbook_build_log.txt'}")


def main() -> None:
    ensure_directories()
    run_id = str(uuid.uuid4())
    failures: list[str] = []
    with duckdb.connect(str(DATABASE_PATH)) as connection:
        initialize_database(connection)
        migration_counts = migrate_existing_files(connection, run_id)
        original_latest = {
            code: latest_date(connection, "index_daily", "index_code", code)
            for code in INDEX_SPECS
        }

        today = pd.Timestamp(datetime.now().date())
        index_added = {"000300": update_index_daily(connection, run_id, "000300", today, failures)}
        benchmark_latest = latest_date(connection, "index_daily", "index_code", "000300")
        if benchmark_latest is None:
            raise RuntimeError("沪深300基准行情不存在，无法计算RS20")
        for code in INDEX_SPECS:
            index_added[code] = update_index_daily(
                connection, run_id, code, benchmark_latest, failures
            )

        constituents = refresh_constituents(connection, run_id, failures)
        if any(frame.empty for frame in constituents.values()):
            raise RuntimeError("成分股数据不完整，无法计算市场广度")
        stock_added, stock_failures = update_stock_daily(
            connection, run_id, constituents, benchmark_latest
        )
        failures.extend(f"{code}: {message}" for code, message in stock_failures.items())

        index_histories, metric_counts, indicator_added = update_indicators(
            connection, run_id, constituents
        )
        results = {code: load_latest_results(connection, code) for code in INDEX_SPECS}
        validations, anomalies = validate_temperature_outputs(
            connection, results, index_histories
        )
        summary_rows = build_summary_rows(results, index_histories)
        write_outputs(results, summary_rows)

        updated_latest = {
            code: latest_date(connection, "index_daily", "index_code", code)
            for code in INDEX_SPECS
        }
        run_summary = {
            "运行时间": datetime.now().isoformat(timespec="seconds"),
            "运行ID": run_id,
            "数据库路径": str(DATABASE_PATH),
            "历史迁移新增行数": migration_counts,
            "各指数原最新日期": {
                code: value.strftime("%Y-%m-%d") if value is not None else None
                for code, value in original_latest.items()
            },
            "各指数更新后最新日期": {
                code: value.strftime("%Y-%m-%d") if value is not None else None
                for code, value in updated_latest.items()
            },
            "新增指数行情行数": {code: index_added.get(code, 0) for code in INDEX_SPECS},
            "新增基准行情行数": index_added["000300"],
            "新增股票行情行数": stock_added,
            "新增指标行数": indicator_added,
            "指标最新日期": {
                code: results[code]["日期"].max().strftime("%Y-%m-%d") for code in INDEX_SPECS
            },
            "当前温度": {row["指数代码"]: row["当前温度"] for row in summary_rows},
            "temperature_change_1d": {
                row["指数代码"]: row["较上一日变化"] for row in summary_rows
            },
            "数据状态": {row["指数代码"]: row["数据状态"] for row in summary_rows},
            "成分股数量": {code: len(frame) for code, frame in constituents.items()},
            "指标有效样本": metric_counts,
            "数据检查": validations,
            "数据异常": anomalies,
            "接口失败数量": len(failures),
            "接口失败明细": failures,
        }
        (OUTPUT_DIR / "run_summary.json").write_text(
            json.dumps(run_summary, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    create_temperature_workbook()
    print(json.dumps(run_summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    # 仅清理当前进程的代理变量，避免本地失效代理影响公开接口；不修改系统配置。
    for proxy_name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
        os.environ.pop(proxy_name, None)
    os.environ["NO_PROXY"] = "*"
    main()
