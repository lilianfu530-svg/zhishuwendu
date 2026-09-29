from __future__ import annotations

import getpass
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests


ROOT = Path(__file__).resolve().parent
CACHE = ROOT / "data" / "hithink_phase15_cache"
OUTPUT = ROOT / "output"
BASE = "https://fuyao.aicubes.cn"
TARGETS = [
    ("证券", "HY0490100", "industry"),
    ("银行", "HY0480000", "industry"),
    ("有色金属", "HY0240000", "industry"),
    ("基础化工", "HY0220000", "industry"),
    ("国防军工", "HY0650000", "industry"),
    ("电网设备", "HY0630800", "industry"),
    ("半导体设备", "HY0270108", "industry"),
    ("黄金", "HY0240401", "industry"),
    ("创新药", "GN0701247", "cn_concept"),
    ("AI应用", "GN0980346", "cn_concept"),
    ("PCB概念", "GN0700740", "cn_concept"),
    ("CPO概念", "GN0701159", "cn_concept"),
    ("超微盘股", "GN0990001", "cn_concept"),
]
ALIAS_CANDIDATES = {
    "国防军工": ("军工", False),
    "黄金": ("黄金概念", False),
    "CPO概念": ("共封装光学(CPO)", True),
}


def write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def api_get(session: requests.Session, path: str, params: dict) -> dict:
    response = session.get(BASE + path, params=params, timeout=45)
    response.raise_for_status()
    payload = response.json()
    if payload.get("code") != 0:
        raise RuntimeError(f"API code={payload.get('code')} message={payload.get('message')}")
    return payload.get("data") or {}


def catalog(session: requests.Session, tag: str) -> list[dict]:
    path = CACHE / f"catalog_{tag}.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))["item"]
    data = api_get(session, "/api/a-share-index/catalog/ths-index-list", {"tag": tag})
    item = [{"name": row.get("name"), "thscode": row.get("thscode")} for row in data.get("item", [])]
    write_json(path, {"tag": tag, "timestamp": data.get("timestamp"), "item": item})
    print(f"目录 {tag}: {len(item)} 条，已缓存", flush=True)
    return item


def normalize_name(value: str) -> str:
    return re.sub(r"[\s（）()·・]", "", value or "").casefold()


def name_candidates(name: str, rows: list[dict]) -> list[dict]:
    norm = normalize_name(name)
    exact = [r for r in rows if normalize_name(r.get("name", "")) == norm]
    close = [r for r in rows if norm in normalize_name(r.get("name", "")) or normalize_name(r.get("name", "")) in norm]
    alias = ALIAS_CANDIDATES.get(name, ("", False))[0]
    aliases = [r for r in rows if alias and r.get("name") == alias]
    found = {r["thscode"]: r for r in exact + close + aliases if r.get("thscode", "").endswith(".TI")}
    return list(found.values())


def unix_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def ths_history(session: requests.Session, thscode: str, start: int, end: int, stock: bool = False) -> pd.DataFrame:
    path = "/api/a-share/prices/historical" if stock else "/api/a-share-index/prices/historical"
    params = {"thscode": thscode, "interval": "1d", "start": start, "end": end}
    if stock:
        params["adjust"] = "forward"
    item = api_get(session, path, params).get("item", [])
    frame = pd.DataFrame(item)
    if frame.empty:
        return pd.DataFrame(columns=["date", "close"])
    frame["date"] = pd.to_datetime(frame["date_ms"], unit="ms", utc=True).dt.tz_convert("Asia/Shanghai").dt.date
    frame["close"] = pd.to_numeric(frame["close_price"], errors="coerce")
    return frame[["date", "close"]].dropna().drop_duplicates("date").sort_values("date")


def sina_history(session: requests.Session, code: str) -> pd.DataFrame:
    response = session.get(
        "https://quotes.sina.cn/cn/api/json_v2.php/CN_MarketDataService.getKLineData",
        params={"symbol": code.lower(), "scale": 240, "ma": "no", "datalen": 90},
        timeout=30,
    )
    response.raise_for_status()
    response.encoding = "gbk"
    item = response.json()
    if isinstance(item, dict):
        item = item.get("result", {}).get("data", [])
    frame = pd.DataFrame(item)
    if frame.empty:
        return pd.DataFrame(columns=["date", "close"])
    frame["date"] = pd.to_datetime(frame["day"], errors="coerce").dt.date
    frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
    return frame[["date", "close"]].dropna().drop_duplicates("date").sort_values("date")


def compare(official: pd.DataFrame, sina: pd.DataFrame) -> tuple[dict, list[dict]]:
    official = official.copy()
    sina = sina.copy()
    official["ths_return"] = official["close"].pct_change()
    sina["sina_return"] = sina["close"].pct_change()
    joined = official.rename(columns={"close": "ths_close"}).merge(
        sina.rename(columns={"close": "sina_close"}), on="date", how="inner"
    ).tail(60)
    returns = joined.dropna(subset=["ths_return", "sina_return"]).copy()
    if returns.empty:
        return {"common_days": len(joined)}, []
    diff = returns["ths_return"] - returns["sina_return"]
    absolute = diff.abs()
    first = joined.iloc[0]
    normalized_ths = joined["ths_close"] / first["ths_close"]
    normalized_sina = joined["sina_close"] / first["sina_close"]
    metrics = {
        "common_days": len(returns),
        "close_correlation": float(normalized_ths.corr(normalized_sina)),
        "return_pearson": float(returns["ths_return"].corr(returns["sina_return"])),
        "return_spearman": float(returns["ths_return"].rank().corr(returns["sina_return"].rank())),
        "return_median_abs_diff": float(absolute.median()),
        "return_p95_abs_diff": float(absolute.quantile(.95)),
        "return_max_abs_diff": float(absolute.max()),
        "return_mean_signed_diff": float(diff.mean()),
        "start_date": str(returns["date"].min()),
        "end_date": str(returns["date"].max()),
    }
    detail = [
        {"date": str(r.date), "ths_close": float(r.ths_close), "sina_close": float(r.sina_close),
         "ths_return": float(r.ths_return), "sina_return": float(r.sina_return),
         "return_abs_diff": abs(float(r.ths_return - r.sina_return))}
        for r in returns.itertuples()
    ]
    return metrics, detail


def identity_pass(metrics: dict) -> bool:
    return all([
        metrics.get("common_days", 0) >= 50,
        metrics.get("return_pearson", -1) >= .995,
        metrics.get("return_spearman", -1) >= .995,
        metrics.get("return_median_abs_diff", 1) <= .0005,
        metrics.get("return_p95_abs_diff", 1) <= .0015,
        metrics.get("return_max_abs_diff", 1) <= .003,
        abs(metrics.get("return_mean_signed_diff", 1)) <= .0003,
    ])


def constituent_probe(session: requests.Session, thscode: str, start: int, end: int) -> dict:
    item = api_get(session, "/api/a-share-index/constituents/ths-stock-list", {"thscode": thscode}).get("item", [])
    codes = [str(r.get("thscode", "")) for r in item]
    bad_codes = [code for code in codes if not re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", code)]
    duplicates = len(codes) - len(set(codes))
    available = bool(item) and not bad_codes and duplicates == 0 and 3 <= len(item) <= 3000
    result = {
        "constituent_count": len(item),
        "constituent_available": available,
        "constituent_bad_codes": bad_codes[:10],
        "constituent_duplicates": duplicates,
        "stock_history_probe": "未执行",
        "stock_history_details": [],
    }
    if not available:
        return result
    rng = np.random.default_rng(20260919)
    sample = rng.choice(len(item), size=min(3, len(item)), replace=False)
    for index in sample:
        row = item[int(index)]
        code = row["thscode"]
        try:
            history = ths_history(session, code, start, end, stock=True)
            span = (history["date"].max() - history["date"].min()).days if len(history) else 0
            ok = len(history) >= 600 and span >= 900
            result["stock_history_details"].append({
                "thscode": code, "ticker": row.get("ticker"), "name": row.get("name"),
                "days": len(history), "span_days": span, "success": ok,
            })
        except Exception as exc:
            result["stock_history_details"].append({"thscode": code, "success": False, "reason": str(exc)[:160]})
    result["stock_history_probe"] = "成功" if all(r["success"] for r in result["stock_history_details"]) else "部分失败"
    return result


def finalize_payload(payload: dict) -> None:
    for row in payload["boards"]:
        if not row["candidate_count"]:
            row["note"] = "industry 与 cn_concept 两份官方目录均无对应名称或可信别名"
        elif not row["name_match"]:
            tag = row.get("ths_catalog_tag", "")
            correlation = row.get("return_correlation")
            corr_text = f"；Pearson={correlation:.6f}" if correlation is not None else ""
            row["note"] = f"候选来自{tag}目录，官方名称不等同于原板块{corr_text}；不查询成分股"
        elif row["final_status"] != "EXACT" and row.get("return_correlation") is not None:
            row["note"] = (
                f"日收益身份不符：Pearson={row['return_correlation']:.6f}、"
                f"Spearman={row['return_spearman']:.6f}、"
                f"P95差={row['return_p95_abs_diff']:.6f}；未查成分股"
            )
    write_json(CACHE / "feasibility_payload.json", payload)
    results = payload["boards"]
    summary = {
        "as_of": payload["as_of"], "total": len(results),
        "with_catalog_candidate": sum(r["candidate_count"] > 0 for r in results),
        "identity_passed": sum(r["final_status"] == "EXACT" or r["note"].startswith("K线身份通过") for r in results),
        "constituent_available": sum(bool(r["constituent_available"]) for r in results),
        "exact": sum(r["final_status"] == "EXACT" for r in results),
        "boards": [{k: r.get(k) for k in ("display_name", "original_code", "ths_name", "ths_code", "candidate_count", "kline_common_days", "return_correlation", "return_spearman", "return_median_abs_diff", "return_p95_abs_diff", "return_max_abs_diff", "constituent_count", "constituent_available", "stock_history_probe", "final_status", "note")} for r in results],
    }
    write_json(OUTPUT / "hithink_board_feasibility_summary.json", summary)


def main() -> None:
    if "--finalize-cache" in sys.argv:
        payload = json.loads((CACHE / "feasibility_payload.json").read_text(encoding="utf-8"))
        finalize_payload(payload)
        return
    key = os.environ.pop("HITHINK_FINANCE_API_KEY", "")
    if not key:
        key = getpass.getpass("API Key: ") if sys.stdin.isatty() else sys.stdin.readline().strip()
    if not key:
        raise RuntimeError("未提供本次进程的 API Key")
    official_session = requests.Session()
    official_session.headers["X-api-key"] = key
    sina_session = requests.Session()
    sina_session.trust_env = False
    sina_session.headers.update({"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn/"})
    now = datetime.now(timezone.utc)
    end = unix_ms(now + timedelta(days=1))
    board_start = unix_ms(now - timedelta(days=150))
    stock_start = unix_ms(now - timedelta(days=3 * 365 + 40))
    rows_by_tag = {tag: [{**r, "catalog_tag": tag} for r in catalog(official_session, tag)] for tag in ("industry", "cn_concept")}
    all_catalog_rows = rows_by_tag["industry"] + rows_by_tag["cn_concept"]
    results = []
    comparisons = []
    for display_name, original_code, category in TARGETS:
        candidates = name_candidates(display_name, all_catalog_rows)
        row = {
            "display_name": display_name, "original_code": original_code, "category": category,
            "ths_name": None, "ths_code": None, "name_match": False,
            "candidate_count": len(candidates), "candidates": candidates,
            "kline_common_days": None, "close_correlation": None,
            "return_correlation": None, "return_spearman": None,
            "return_median_abs_diff": None, "return_p95_abs_diff": None,
            "return_max_abs_diff": None, "return_mean_signed_diff": None,
            "constituent_count": None, "constituent_available": False,
            "stock_history_probe": "未执行", "final_status": "UNSUPPORTED", "note": "",
        }
        if not candidates:
            row["note"] = "官方对应目录没有名称候选"
            results.append(row)
            print(f"{display_name}: 无目录候选", flush=True)
            continue
        exact = [r for r in candidates if normalize_name(r["name"]) == normalize_name(display_name)]
        if len(exact) == 1:
            candidate = exact[0]
            name_match = True
        else:
            alias, confirmed = ALIAS_CANDIDATES.get(display_name, ("", False))
            alias_rows = [r for r in candidates if r["name"] == alias]
            if len(alias_rows) != 1:
                row["note"] = "目录名称未唯一明确匹配；候选仅供人工复核"
                results.append(row)
                print(f"{display_name}: 名称未唯一匹配", flush=True)
                continue
            candidate = alias_rows[0]
            name_match = confirmed
        row.update({"ths_name": candidate["name"], "ths_code": candidate["thscode"], "name_match": name_match,
                    "ths_catalog_tag": candidate["catalog_tag"]})
        try:
            ths = ths_history(official_session, candidate["thscode"], board_start, end)
            sina = sina_history(sina_session, original_code)
            metrics, detail = compare(ths, sina)
            row.update({
                "kline_common_days": metrics.get("common_days"),
                "close_correlation": metrics.get("close_correlation"),
                "return_correlation": metrics.get("return_pearson"),
                "return_spearman": metrics.get("return_spearman"),
                "return_median_abs_diff": metrics.get("return_median_abs_diff"),
                "return_p95_abs_diff": metrics.get("return_p95_abs_diff"),
                "return_max_abs_diff": metrics.get("return_max_abs_diff"),
                "return_mean_signed_diff": metrics.get("return_mean_signed_diff"),
                "kline_start_date": metrics.get("start_date"), "kline_end_date": metrics.get("end_date"),
                "ths_kline_days": len(ths), "sina_kline_days": len(sina),
            })
            comparisons.extend([{"original_code": original_code, "ths_code": candidate["thscode"], **r} for r in detail])
            if not name_match:
                row["note"] = "仅相近名称，官方目录未明确确认同一板块；不认定EXACT，也不查询成分股"
            elif not identity_pass(metrics):
                row["note"] = "名称匹配，但K线指标未全部达到严格身份判定标准；需人工复核，不认定EXACT"
            else:
                probe = constituent_probe(official_session, candidate["thscode"], stock_start, end)
                row.update(probe)
                if probe["constituent_available"]:
                    row["final_status"] = "EXACT"
                    row["note"] = "名称及K线身份通过；成分股为当前清单，仅作current_constituents_proxy"
                else:
                    row["note"] = "K线身份通过，但当前成分股为空或存在代码、重复、数量异常"
        except Exception as exc:
            row["note"] = f"身份验证接口失败：{str(exc)[:180]}"
        results.append(row)
        print(f"{display_name}: {row['final_status']}，共同交易日={row['kline_common_days']}，成分股={row['constituent_count']}", flush=True)
    payload = {"as_of": now.isoformat(), "thresholds": {
        "common_days_min": 50, "return_pearson_min": .995, "return_spearman_min": .995,
        "return_median_abs_diff_max": .0005, "return_p95_abs_diff_max": .0015,
        "return_max_abs_diff_max": .003, "return_mean_signed_diff_abs_max": .0003,
    }, "boards": results, "kline_comparisons": comparisons}
    finalize_payload(payload)
    print("阶段1.5数据验证完成", flush=True)


if __name__ == "__main__":
    main()
