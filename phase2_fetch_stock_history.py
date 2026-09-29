from __future__ import annotations

import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import akshare as ak
import duckdb
import pandas as pd
import requests


ROOT = Path(__file__).resolve().parent
DATABASE = ROOT / "data" / "market.duckdb"
CONSTITUENTS = ROOT / "data" / "phase2_constituents"
PROGRESS = ROOT / "data" / "phase2_stock_fetch_progress.json"
START_DATE = pd.Timestamp("2022-01-01")
STOCK_COLUMNS = [
    "market", "stock_code", "date", "open", "high", "low", "close",
    "pct_change", "volume", "amount", "source", "fetched_at",
]


def source_symbol(market: str, code: str) -> str:
    return market.lower() + code


def fetch_tencent_stock_range(market: str, code: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    # 腾讯区间接口只返回指定日期，成交额源字段缺失，保留空值。
    records = []
    cursor = start
    while cursor <= end:
        segment_end = min(cursor + pd.Timedelta(days=365), end)
        response = requests.get(
            "http://web.ifzq.gtimg.cn/appstock/app/kline/kline",
            params={"_var": "kline_day", "param": f"{market.lower()}{code},day,{cursor.date()},{segment_end.date()},640,"},
            timeout=20,
        )
        response.raise_for_status()
        payload = json.loads(response.text.split("=", 1)[1])
        if payload.get("code") != 0:
            raise RuntimeError(f"腾讯股票行情返回异常：{payload.get('msg')}")
        records.extend(payload.get("data", {}).get(f"{market.lower()}{code}", {}).get("day", []))
        cursor = segment_end + pd.Timedelta(days=1)
    if not records:
        return pd.DataFrame()
    raw = pd.DataFrame([row[:6] for row in records], columns=["date", "open", "close", "high", "low", "volume"])
    raw["amount"] = float("nan")
    return raw


def fetch_one(market: str, code: str, start: pd.Timestamp, end: pd.Timestamp,
              previous_close: float | None = None) -> pd.DataFrame:
    if market == "HK":
        raw = ak.stock_hk_daily(symbol=code, adjust="") if start == START_DATE else fetch_tencent_stock_range(market, code, start, end)
    elif market == "SH" and code == "689009":
        # 九号公司存托凭证的新浪份额接口返回非 JSON；该股票固定使用腾讯区间日K。
        raw = fetch_tencent_stock_range(market, code, start, end)
    else:
        raw = ak.stock_zh_a_daily(
            symbol=source_symbol(market, code), start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"), adjust="",
        )
        raw = raw.rename(columns={"date": "date"})
    if raw.empty:
        return pd.DataFrame()
    required = {"date", "open", "high", "low", "close", "volume", "amount"}
    missing = required - set(raw.columns)
    if missing:
        raise RuntimeError(f"行情缺列：{sorted(missing)}")
    raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
    for field in ("open", "high", "low", "close", "volume", "amount"):
        raw[field] = pd.to_numeric(raw[field], errors="coerce")
    raw = raw[(raw["date"] >= start) & (raw["date"] <= end)]
    raw = raw.dropna(subset=["date", "close"])
    raw = raw.sort_values("date").drop_duplicates("date", keep="last")
    if raw.empty:
        return raw
    prior_close = raw["close"].shift(1)
    if previous_close is not None:
        prior_close.iloc[0] = previous_close
    raw["pct_change"] = raw["close"] / prior_close - 1
    raw["market"] = market
    raw["stock_code"] = code
    if market == "SH" and code == "689009":
        raw["source"] = "腾讯财经区间日K"
    elif market == "HK":
        raw["source"] = "新浪财经（AKShare stock_hk_daily）" if start == START_DATE else "腾讯财经区间日K"
    else:
        raw["source"] = "新浪财经（AKShare stock_zh_a_daily）"
    raw["fetched_at"] = datetime.now()
    return raw[STOCK_COLUMNS]


def load_union() -> pd.DataFrame:
    files = [CONSTITUENTS / f"{code}.csv" for code in
             ("399006", "930986", "000001", "000688", "899050", "932000", "HSTECH", "931787")]
    if any(not path.exists() for path in files):
        raise RuntimeError("8个指数的当前成分股清单不完整")
    frames = [pd.read_csv(path, dtype={"index_code": str, "stock_code": str}, encoding="utf-8-sig") for path in files]
    result = pd.concat(frames, ignore_index=True)[["market", "stock_code"]].drop_duplicates()
    if not result["market"].isin(["SH", "SZ", "BJ", "HK"]).all():
        raise RuntimeError("成分股市场代码异常")
    return result.sort_values(["market", "stock_code"]).reset_index(drop=True)


def write_progress(state: dict) -> None:
    PROGRESS.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()
    union = load_union()
    if union.empty:
        raise RuntimeError("股票并集为空")
    with duckdb.connect(str(DATABASE), read_only=True) as connection:
        benchmark_date = connection.execute("SELECT MAX(date) FROM index_daily WHERE index_code = '000300'").fetchone()[0]
    if benchmark_date is None:
        raise RuntimeError("沪深300基准日期不存在")
    target_date = pd.Timestamp(benchmark_date)
    if PROGRESS.exists():
        state = json.loads(PROGRESS.read_text(encoding="utf-8"))
    else:
        state = {"started_at": datetime.now().isoformat(), "target_date": str(target_date.date()),
                 "union_size": len(union), "requests": 0, "inserted_rows": 0, "failures": {}}
    with duckdb.connect(str(DATABASE)) as connection:
        local_max = {
            (market, code): (pd.Timestamp(day), close)
            for market, code, day, close in connection.execute(
                "SELECT market, stock_code, MAX(date), ARG_MAX(close, date) FROM stock_daily GROUP BY 1, 2"
            ).fetchall()
        }
        pending = []
        for position, member in enumerate(union.itertuples(index=False), start=1):
            market, code = member.market, member.stock_code
            latest = local_max.get((market, code))
            start = START_DATE if latest is None else latest[0] + pd.Timedelta(days=1)
            if start <= target_date:
                pending.append((position, market, code, start, None if latest is None else latest[1]))
        if args.limit:
            pending = pending[:args.limit]
        with ThreadPoolExecutor(max_workers=4) as executor:
            jobs = {
                executor.submit(fetch_one, market, code, start, target_date, previous_close): (position, market, code)
                for position, market, code, start, previous_close in pending
            }
            for handled, future in enumerate(as_completed(jobs), start=1):
                position, market, code = jobs[future]
                key = f"{market}:{code}"
                try:
                    data = future.result()
                    state["requests"] += 1
                    if not data.empty:
                        connection.register("incoming_stock", data)
                        try:
                            before = connection.execute("SELECT COUNT(*) FROM stock_daily").fetchone()[0]
                            connection.execute(
                                "INSERT OR IGNORE INTO stock_daily SELECT market, stock_code, date, open, high, low, close, pct_change, volume, amount, source, fetched_at FROM incoming_stock"
                            )
                            after = connection.execute("SELECT COUNT(*) FROM stock_daily").fetchone()[0]
                            state["inserted_rows"] += after - before
                        finally:
                            connection.unregister("incoming_stock")
                    state["failures"].pop(key, None)
                except Exception as exc:
                    state["failures"][key] = f"{type(exc).__name__}: {exc}"[:250]
                if handled % 25 == 0 or handled == len(pending):
                    state["updated_at"] = datetime.now().isoformat()
                    state["completed_this_run"] = handled
                    state["pending_this_run"] = len(pending)
                    write_progress(state)
                    print(f"股票进度 {handled}/{len(pending)}；总并集 {len(union)}；已请求 {state['requests']}；新增 {state['inserted_rows']} 行；失败 {len(state['failures'])}", flush=True)
    state["updated_at"] = datetime.now().isoformat()
    write_progress(state)
    print(json.dumps({"handled_this_run": len(pending), "requests_total": state["requests"],
                      "inserted_rows_total": state["inserted_rows"], "failure_count": len(state["failures"])}, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
