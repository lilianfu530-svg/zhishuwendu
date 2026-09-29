from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path

import akshare as ak
import pandas as pd


ROOT = Path(__file__).resolve().parent
CACHE = ROOT / "data" / "phase2_constituents"
CODES = ("399006", "930986", "000001", "000688", "899050", "932000", "931787")
EXCHANGE_MARKET = {
    "上海证券交易所": "SH", "深圳证券交易所": "SZ", "北京证券交易所": "BJ",
    "香港交易所": "HK", "香港联合交易所": "HK", "香港证券交易所": "HK",
    "Shanghai Stock Exchange": "SH", "Shenzhen Stock Exchange": "SZ",
    "Beijing Stock Exchange": "BJ", "Hong Kong Stock Exchange": "HK",
}


def main() -> None:
    CACHE.mkdir(parents=True, exist_ok=True)
    results = []
    for code in CODES:
        path = CACHE / f"{code}.csv"
        if path.exists():
            frame = pd.read_csv(path, dtype={"stock_code": str}, encoding="utf-8-sig")
            results.append({"index_code": code, "count": len(frame), "status": "cached", "path": str(path)})
            print(f"{code}: 缓存 {len(frame)}", flush=True)
            continue
        try:
            raw = ak.index_detail_cni(symbol=code) if code == "399006" else ak.index_stock_cons_weight_csindex(symbol=code)
            if raw.empty:
                raise RuntimeError("成分股接口返回空表")
            raw.to_csv(CACHE / f"{code}_raw.csv", index=False, encoding="utf-8-sig")
            if code == "399006":
                name_field, code_field, date_field = "样本简称", "样本代码", "日期"
            else:
                name_field, code_field, date_field = "成分券名称", "成分券代码", "日期"
            code_len = 5 if code == "931787" else 6
            stock_code = raw[code_field].astype(str).str.extract(r"(\d+)", expand=False).str.zfill(code_len)
            if code == "399006":
                # 创业板成分券的六位 3 开头代码对应深交所。
                market = stock_code.map(lambda value: "SZ" if value.startswith("3") and len(value) == 6 else None)
                exchange = pd.Series(["深圳证券交易所"] * len(raw))
            elif code == "931787":
                # 港股创新药指数成分券使用五位香港股票代码。
                market = stock_code.map(lambda value: "HK" if len(value) == 5 else None)
                exchange = pd.Series(["香港交易所"] * len(raw))
            else:
                exchange = raw["交易所"].astype(str)
                market = exchange.map(EXCHANGE_MARKET)
            if market.isna().any():
                unknown = exchange[market.isna()].value_counts().to_dict()
                raise RuntimeError(f"交易所无法识别：{unknown}")
            frame = pd.DataFrame({
                "index_code": code, "market": market,
                "stock_code": stock_code, "stock_name": raw[name_field].astype(str),
                "effective_date": pd.to_datetime(raw[date_field], errors="coerce").dt.strftime("%Y-%m-%d"),
                "source": "国证指数官网" if code == "399006" else "中证指数官网",
            })
            if frame["stock_code"].isna().any() or frame["effective_date"].isna().any():
                raise RuntimeError("代码或成分日期缺失")
            if frame.duplicated(["market", "stock_code"]).any():
                raise RuntimeError("当前成分股存在重复市场代码")
            frame.to_csv(path, index=False, encoding="utf-8-sig")
            results.append({"index_code": code, "count": len(frame), "status": "success", "path": str(path),
                            "date": frame["effective_date"].max()})
            print(f"{code}: 获取 {len(frame)}", flush=True)
        except Exception as exc:
            results.append({"index_code": code, "count": None, "status": "failed", "reason": f"{type(exc).__name__}: {exc}"})
            print(f"{code}: 失败 {type(exc).__name__}: {exc}", flush=True)
        time.sleep(.15)
    (CACHE / "fetch_summary.json").write_text(
        json.dumps({"run_at": datetime.now().isoformat(), "indices": results}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
