from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import akshare as ak
import pandas as pd
import requests


ROOT = Path(__file__).resolve().parent
CACHE = ROOT / "data" / "phase2_index_history"
FIRST_INDEX_CODES = ("000001", "000688", "899050", "932000", "HSTECH", "931787")
START_DATE = "20220101"
_today = pd.Timestamp(datetime.now().date())
END_DATE = (_today if _today.weekday() < 5 else _today - pd.offsets.BDay(1)).strftime("%Y%m%d")


def fetch_hstech_range(start_date: str, end_date: str) -> pd.DataFrame:
    # 腾讯指数日K按日期分段请求；第六列为成交额，成交量不提供。
    records = []
    cursor = pd.Timestamp(start_date)
    end = pd.Timestamp(end_date)
    while cursor <= end:
        segment_end = min(cursor + pd.Timedelta(days=365), end)
        response = requests.get(
            "http://web.ifzq.gtimg.cn/appstock/app/kline/kline",
            params={"_var": "kline_day", "param": f"hkHSTECH,day,{cursor.date()},{segment_end.date()},640,"},
            timeout=20,
        )
        response.raise_for_status()
        payload = json.loads(response.text.split("=", 1)[1])
        if payload.get("code") != 0:
            raise RuntimeError(f"恒生科技指数区间行情异常：{payload.get('msg')}")
        records.extend(payload.get("data", {}).get("hkHSTECH", {}).get("day", []))
        cursor = segment_end + pd.Timedelta(days=1)
    if not records:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume", "amount"])
    data = pd.DataFrame([row[:6] for row in records], columns=["date", "open", "close", "high", "low", "amount"])
    data["volume"] = float("nan")
    return data


def fetch(code: str, start_date: str = START_DATE) -> pd.DataFrame:
    if code == "HSTECH":
        raw = fetch_hstech_range(start_date, END_DATE)
        source = "腾讯财经区间指数日K"
    else:
        try:
            raw = ak.stock_zh_index_hist_csindex(symbol=code, start_date=start_date, end_date=END_DATE)
        except ValueError as exc:
            if "Expected axis has 0 elements" not in str(exc):
                raise
            raw = pd.DataFrame()
        if raw.empty:
            return pd.DataFrame(columns=["index_code", "date", "open", "high", "low", "close", "volume", "amount", "pct_change", "source", "fetched_at"])
        raw = raw.rename(columns={"日期": "date", "开盘": "open", "最高": "high", "最低": "low",
                                  "收盘": "close", "成交量": "volume", "成交金额": "amount"})
        raw["amount"] = pd.to_numeric(raw["amount"], errors="coerce") * 100_000_000
        source = "中证指数官网（AKShare stock_zh_index_hist_csindex）"
    if raw.empty:
        return pd.DataFrame(columns=["index_code", "date", "open", "high", "low", "close", "volume", "amount", "pct_change", "source", "fetched_at"])
    required = ["date", "open", "high", "low", "close", "volume", "amount"]
    if any(column not in raw for column in required):
        raise RuntimeError(f"指数历史缺列：{[column for column in required if column not in raw]}")
    data = raw[required].copy()
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    for field in required[1:]:
        data[field] = pd.to_numeric(data[field], errors="coerce")
    data = data.dropna(subset=["date", "close"]).sort_values("date").drop_duplicates("date")
    data = data[data["date"] >= pd.Timestamp(start_date)]
    data = data[data["date"].dt.weekday < 5]
    data["pct_change"] = data["close"].pct_change(fill_method=None)
    data["index_code"] = code
    data["source"] = source
    data["fetched_at"] = datetime.now()
    return data[["index_code", *required, "pct_change", "source", "fetched_at"]]


def main() -> None:
    CACHE.mkdir(parents=True, exist_ok=True)
    summary = []
    for code in FIRST_INDEX_CODES:
        path = CACHE / f"{code}.csv"
        if path.exists():
            data = pd.read_csv(path, dtype={"index_code": str})
            data["date"] = pd.to_datetime(data["date"], errors="coerce")
            start = data["date"].max() + pd.Timedelta(days=1)
            if start <= pd.Timestamp(END_DATE):
                incoming = fetch(code, start.strftime("%Y%m%d"))
                data = pd.concat([data, incoming], ignore_index=True).drop_duplicates("date", keep="first")
                status = "incremental" if len(incoming) else "cached"
            else:
                status = "cached"
        else:
            try:
                data = fetch(code)
                if data.empty:
                    raise RuntimeError("首次初始化指数历史为空")
                status = "success"
            except Exception as exc:
                summary.append({"index_code": code, "status": "failed", "reason": f"{type(exc).__name__}: {exc}"})
                print(f"{code}: 失败 {type(exc).__name__}: {exc}", flush=True)
                continue
        data = data[data["date"].dt.weekday < 5].sort_values("date")
        if code in ("HSTECH", "931787"):
            # 台风停市等零成交占位行不属于真实交易日。
            data = data[pd.to_numeric(data["amount"], errors="coerce") > 0]
        data["pct_change"] = pd.to_numeric(data["close"], errors="coerce").pct_change(fill_method=None)
        data.to_csv(path, index=False, encoding="utf-8-sig")
        record = {"index_code": code, "status": status, "rows": len(data),
                  "start": str(pd.to_datetime(data["date"]).min().date()),
                  "end": str(pd.to_datetime(data["date"]).max().date()), "path": str(path)}
        summary.append(record)
        print(f"{code}: {record['rows']} 行，{record['start']} 至 {record['end']}", flush=True)
    (CACHE / "fetch_summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
