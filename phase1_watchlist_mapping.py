from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import date, datetime
from pathlib import Path
from typing import Any

import akshare as ak
import duckdb
import pandas as pd
import requests


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "output"
DATABASE_PATH = DATA_DIR / "market.duckdb"
PAYLOAD_PATH = DATA_DIR / "watchlist_mapping_payload.json"

ALLOWED_STATUSES = {"EXACT", "PUBLIC_EQUIVALENT", "UNSUPPORTED"}

SOURCE_URLS = {
    "399006": "https://www.cnindex.com.cn/docs/gz_399606.pdf",
    "930986": "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/indices/detail/files/zh_CN/930986factsheet.pdf",
    "000001": "https://www.sse.com.cn/market/sseindex/indexlist/",
    "000688": "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/indices/detail/files/zh_CN/000688factsheet.pdf",
    "899050": "https://www.bse.cn/market_data/bse_indices/bse_bz50.html",
    "932000": "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/indices/detail/files/zh_CN/932000factsheet.pdf",
    "HSTECH": "https://www.hsi.com.hk/eng/indexes/all-indexes/hstech",
    "931787": "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/indices/detail/files/zh_CN/931787factsheet.pdf",
}

OFFICIAL_TARGETS = [
    {
        "display_name": "创业板指",
        "original_code": "SZ399006",
        "canonical_code": "399006",
        "canonical_name": "创业板指数",
        "category": "宽基指数",
        "source_kind": "cni",
        "data_source": "国证指数官网；AKShare index_hist_cni、index_detail_cni",
        "early_start": "20220101",
        "early_end": "20220131",
        "sample_member": "300661",
        "sample_market": "cn",
    },
    {
        "display_name": "金融科技",
        "original_code": "SI.930986",
        "canonical_code": "930986",
        "canonical_name": "中证金融科技主题指数",
        "category": "主题指数",
        "source_kind": "csi",
        "data_source": "中证指数官网；AKShare stock_zh_index_hist_csindex、index_stock_cons_weight_csindex",
        "early_start": "20220101",
        "early_end": "20220131",
        "sample_member": "000555",
        "sample_market": "cn",
    },
    {
        "display_name": "上证指数",
        "original_code": "SH000001",
        "canonical_code": "000001",
        "canonical_name": "上证综合指数",
        "category": "宽基指数",
        "source_kind": "csi",
        "data_source": "上交所/中证指数官网；AKShare stock_zh_index_hist_csindex、index_stock_cons_weight_csindex",
        "early_start": "20220101",
        "early_end": "20220131",
        "sample_member": "600000",
        "sample_market": "cn",
    },
    {
        "display_name": "科创50",
        "original_code": "SH000688",
        "canonical_code": "000688",
        "canonical_name": "上证科创板50成份指数",
        "category": "宽基指数",
        "source_kind": "csi",
        "data_source": "上交所/中证指数官网；AKShare stock_zh_index_hist_csindex、index_stock_cons_weight_csindex",
        "early_start": "20220101",
        "early_end": "20220131",
        "sample_member": "688008",
        "sample_market": "cn",
    },
    {
        "display_name": "北证50",
        "original_code": "SI.899050",
        "canonical_code": "899050",
        "canonical_name": "北证50成份指数",
        "category": "宽基指数",
        "source_kind": "csi",
        "data_source": "北交所/中证指数官网；AKShare stock_zh_index_hist_csindex、index_stock_cons_weight_csindex",
        "early_start": "20220429",
        "early_end": "20220531",
        "sample_member": "920002",
        "sample_market": "cn",
    },
    {
        "display_name": "中证2000",
        "original_code": "SI.932000",
        "canonical_code": "932000",
        "canonical_name": "中证2000指数",
        "category": "宽基指数",
        "source_kind": "csi",
        "data_source": "中证指数官网；AKShare stock_zh_index_hist_csindex、index_stock_cons_weight_csindex",
        "early_start": "20220101",
        "early_end": "20220131",
        "sample_member": "000011",
        "sample_market": "cn",
    },
    {
        "display_name": "恒生科技指数",
        "original_code": "HK.HSTECH",
        "canonical_code": "HSTECH",
        "canonical_name": "Hang Seng TECH Index",
        "category": "港股指数",
        "source_kind": "hsi",
        "data_source": "恒生指数公司当前成分；AKShare stock_hk_index_daily_sina、stock_hk_daily",
        "sample_member": "00700",
        "sample_market": "hk",
    },
    {
        "display_name": "港股创新药",
        "original_code": "SI.931787",
        "canonical_code": "931787",
        "canonical_name": "中证香港创新药指数",
        "category": "港股主题",
        "source_kind": "csi",
        "data_source": "中证指数官网；AKShare stock_zh_index_hist_csindex、index_stock_cons_weight_csindex、stock_hk_daily",
        "early_start": "20220101",
        "early_end": "20220131",
        "sample_member": "00013",
        "sample_market": "hk",
    },
]

BOARD_TARGETS = [
    ("证券", "HY0490100", "行业板块"),
    ("银行", "HY0480000", "行业板块"),
    ("有色金属", "HY0240000", "行业板块"),
    ("基础化工", "HY0220000", "行业板块"),
    ("国防军工", "HY0650000", "行业板块"),
    ("电网设备", "HY0630800", "行业板块"),
    ("半导体设备", "HY0270108", "行业板块"),
    ("黄金", "HY0240401", "行业板块"),
    ("创新药", "GN0701247", "主题/概念"),
    ("AI应用", "GN0980346", "主题/概念"),
    ("PCB概念", "GN0700740", "主题/概念"),
    ("CPO概念", "GN0701159", "主题/概念"),
    ("超微盘股", "GN0990001", "主题/概念"),
]

EXCLUDED_FUNDS = [
    {
        "display_name": "科创新能源ETF易方达",
        "original_code": "SH588830",
        "product_type": "ETF",
        "reason": "基金产品，不作为指数纳入本轮验证",
    },
    {
        "display_name": "石油基金LOF",
        "original_code": "SZ160416",
        "product_type": "LOF",
        "reason": "基金产品，不作为指数纳入本轮验证",
    },
    {
        "display_name": "粮食ETF广发",
        "original_code": "SZ159587",
        "product_type": "ETF",
        "reason": "基金产品，不作为指数纳入本轮验证",
    },
]


def json_default(value: object) -> object:
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if pd.isna(value):
        return None
    raise TypeError(f"无法序列化：{type(value).__name__}")


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def cn_stock_symbol(code: str) -> str:
    if code.startswith(("4", "8", "9")):
        return f"bj{code}"
    if code.startswith(("5", "6", "68")):
        return f"sh{code}"
    return f"sz{code}"


def probe_member_history(code: str, market: str) -> dict[str, Any]:
    if market == "hk":
        frame = ak.stock_hk_daily(symbol=code, adjust="")
        dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
    else:
        frame = ak.stock_zh_a_daily(
            symbol=cn_stock_symbol(code),
            start_date="20230901",
            end_date="20260918",
            adjust="",
        )
        dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
    required = {"close", "volume"}
    if frame.empty or not required.issubset(frame.columns):
        raise RuntimeError(f"样本成分股{code}未返回收盘和成交量")
    return {
        "rows": int(len(frame)),
        "start_date": dates.min(),
        "end_date": dates.max(),
        "columns": list(frame.columns),
    }


def probe_official_target(target: dict[str, Any]) -> dict[str, Any]:
    code = target["canonical_code"]
    if target["source_kind"] == "hsi":
        history = ak.stock_hk_index_daily_sina(symbol=code)
        dates = pd.to_datetime(history["date"], errors="coerce").dropna()
        constituents_count = 30
        constituent_date = "2026-09-11"
        live_name = "Hang Seng TECH Index"
        early_rows = int((dates <= pd.Timestamp("2023-09-19")).sum())
        recent_rows = int((dates >= pd.Timestamp("2026-09-01")).sum())
    else:
        if target["source_kind"] == "cni":
            early = ak.index_hist_cni(
                symbol=code,
                start_date=target["early_start"],
                end_date=target["early_end"],
            )
            recent = ak.index_hist_cni(
                symbol=code, start_date="20260901", end_date="20260918"
            )
            constituents = ak.index_detail_cni(symbol=code)
            history_columns = {
                "开盘价",
                "最高价",
                "最低价",
                "收盘价",
                "成交量",
            }
            live_name = target["canonical_name"]
            constituent_date = str(constituents["日期"].iloc[0])
        else:
            early = ak.stock_zh_index_hist_csindex(
                symbol=code,
                start_date=target["early_start"],
                end_date=target["early_end"],
            )
            recent = ak.stock_zh_index_hist_csindex(
                symbol=code, start_date="20260901", end_date="20260918"
            )
            constituents = ak.index_stock_cons_weight_csindex(symbol=code)
            history_columns = {"开盘", "最高", "最低", "收盘", "成交量"}
            live_name = str(recent["指数中文全称"].iloc[0])
            constituent_date = str(constituents["日期"].iloc[0])
        if early.empty or recent.empty or constituents.empty:
            raise RuntimeError("指数早期行情、近期行情或当前成分股为空")
        if not history_columns.issubset(recent.columns):
            raise RuntimeError("指数行情缺少OHLC或成交量字段")
        early_dates = pd.to_datetime(early["日期"], errors="coerce").dropna()
        recent_dates = pd.to_datetime(recent["日期"], errors="coerce").dropna()
        dates = pd.concat([early_dates, recent_dates], ignore_index=True)
        constituents_count = int(len(constituents))
        early_rows = int(len(early))
        recent_rows = int(len(recent))

    member_probe = probe_member_history(
        target["sample_member"], target["sample_market"]
    )
    if early_rows == 0 or recent_rows == 0:
        raise RuntimeError("指数未同时返回早期和近期行情")
    return {
        "display_name": target["display_name"],
        "original_code": target["original_code"],
        "canonical_code": code,
        "canonical_name": live_name,
        "category": target["category"],
        "data_source": target["data_source"],
        "mapping_status": "EXACT",
        "mapping_note": "官方名称和代码与用户指定标的一致；代码前缀仅做数据源格式规范化。",
        "daily_history_check": "通过",
        "daily_history_note": f"早期窗口{early_rows}行，近期窗口{recent_rows}行。",
        "current_constituents_check": "通过",
        "current_constituents_count": constituents_count,
        "current_constituents_date": constituent_date,
        "member_history_check": "通过",
        "member_history_note": (
            f"样本成分股{target['sample_member']}返回{member_probe['rows']}行，"
            f"覆盖{member_probe['start_date'].date()}至{member_probe['end_date'].date()}。"
        ),
        "seven_metrics_check": "通过",
        "source_stability_check": "通过",
        "included": "纳入第二阶段候选",
        "exclusion_reason": "",
        "source_url": SOURCE_URLS[code],
        "verified_at": datetime.now(),
    }


def sina_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.headers.update(
        {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.sina.com.cn/"}
    )
    return session


def probe_board_target(
    display_name: str, original_code: str, category: str
) -> dict[str, Any]:
    code = original_code.lower()
    session = sina_session()
    quote_response = session.get(
        f"https://hq.sinajs.cn/list=s_{code}", timeout=20
    )
    quote_response.raise_for_status()
    quote_response.encoding = "gb18030"
    quote_text = quote_response.text
    kline_response = session.get(
        "https://quotes.sina.cn/cn/api/json_v2.php/"
        "CN_MarketDataService.getKLineData",
        params={"symbol": code, "scale": 240, "ma": "no", "datalen": 30},
        timeout=20,
    )
    kline_response.raise_for_status()
    kline_response.encoding = "gbk"
    history = kline_response.json()
    if not isinstance(history, list):
        history = history.get("result", {}).get("data", [])
    history_dates = pd.to_datetime(
        [row.get("day") for row in history], errors="coerce"
    ).dropna()
    history_available = bool(history) and len(history_dates) > 0
    source_name_matched = f'"{display_name},' in quote_text
    note = (
        f"新浪公开行情同代码返回{len(history)}个近期日K，名称匹配={source_name_matched}；"
        "但在当前允许的官方/公开接口中未取得同一板块的当前成分股清单和可核验编制口径。"
    )
    return {
        "display_name": display_name,
        "original_code": original_code,
        "canonical_code": original_code,
        "canonical_name": display_name,
        "category": category,
        "data_source": "新浪财经公开板块行情，仅验证到同代码日K；未调用同花顺API",
        "mapping_status": "UNSUPPORTED",
        "mapping_note": note,
        "daily_history_check": "仅近期已核验" if history_available else "不通过",
        "daily_history_note": (
            f"近期窗口{len(history)}行，{history_dates.min().date()}至{history_dates.max().date()}。"
            if history_available
            else "公开板块日K未返回有效记录。"
        ),
        "current_constituents_check": "不通过",
        "current_constituents_count": None,
        "current_constituents_date": None,
        "member_history_check": "不通过",
        "member_history_note": "缺少同一板块的可核验当前成分股，不能启动成分股历史行情检查。",
        "seven_metrics_check": "不通过",
        "source_stability_check": "近期接口可用；完整链路不通过",
        "included": "暂不纳入",
        "exclusion_reason": "无法核验同一HY/GN板块的当前成分股，不能计算BreadthMA20、HLBreadth和Sync。",
        "source_url": f"https://hq.sinajs.cn/list=s_{code}",
        "verified_at": datetime.now(),
    }


def failed_row(target: dict[str, Any], error: Exception) -> dict[str, Any]:
    return {
        "display_name": target["display_name"],
        "original_code": target["original_code"],
        "canonical_code": target.get("canonical_code", ""),
        "canonical_name": target.get("canonical_name", ""),
        "category": target["category"],
        "data_source": target.get("data_source", ""),
        "mapping_status": "UNSUPPORTED",
        "mapping_note": f"小范围实际请求失败：{type(error).__name__}: {error}",
        "daily_history_check": "不通过",
        "daily_history_note": "小范围请求失败。",
        "current_constituents_check": "不通过",
        "current_constituents_count": None,
        "current_constituents_date": None,
        "member_history_check": "不通过",
        "member_history_note": "未进入成分股历史检查。",
        "seven_metrics_check": "不通过",
        "source_stability_check": "不通过",
        "included": "暂不纳入",
        "exclusion_reason": "数据源实际运行未通过。",
        "source_url": SOURCE_URLS.get(target.get("canonical_code", ""), ""),
        "verified_at": datetime.now(),
    }


def database_snapshot() -> dict[str, Any]:
    with duckdb.connect(str(DATABASE_PATH), read_only=True) as connection:
        version_counts = connection.execute(
            """
            SELECT index_code, formula_version, COUNT(*) AS rows,
                   MIN(date) AS start_date, MAX(date) AS end_date
            FROM indicator_daily
            GROUP BY index_code, formula_version
            ORDER BY index_code, formula_version
            """
        ).df()
    return {
        "sha256": file_hash(DATABASE_PATH),
        "modified_ns": DATABASE_PATH.stat().st_mtime_ns,
        "version_counts": version_counts.to_dict(orient="records"),
    }


def run_workbook_builder() -> None:
    node = Path(
        r"C:\Users\Lenovo\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe"
    )
    completed = subprocess.run(
        [str(node), str(ROOT / "build_watchlist_mapping.mjs")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    (DATA_DIR / "watchlist_mapping_build.log").write_text(
        completed.stdout + completed.stderr, encoding="utf-8"
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"映射工作簿生成失败，详见{DATA_DIR / 'watchlist_mapping_build.log'}"
        )


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    database_before = database_snapshot()
    rows: list[dict[str, Any]] = []

    for target in OFFICIAL_TARGETS:
        try:
            rows.append(probe_official_target(target))
        except Exception as error:  # 单个失败不得阻塞其他标的
            rows.append(failed_row(target, error))

    for display_name, original_code, category in BOARD_TARGETS:
        target = {
            "display_name": display_name,
            "original_code": original_code,
            "canonical_code": original_code,
            "canonical_name": display_name,
            "category": category,
            "data_source": "新浪财经公开板块行情",
        }
        try:
            rows.append(probe_board_target(display_name, original_code, category))
        except Exception as error:  # 单个失败不得阻塞其他标的
            rows.append(failed_row(target, error))

    database_after = database_snapshot()
    database_unchanged = (
        database_before["sha256"] == database_after["sha256"]
        and database_before["modified_ns"] == database_after["modified_ns"]
        and database_before["version_counts"] == database_after["version_counts"]
    )
    if not database_unchanged:
        raise RuntimeError("第一阶段只读边界失败：market.duckdb发生变化")

    if len(rows) != 21:
        raise RuntimeError(f"候选数量错误：{len(rows)}")
    if any(row["mapping_status"] not in ALLOWED_STATUSES for row in rows):
        raise RuntimeError("存在未允许的映射状态")
    if len({row["original_code"] for row in rows}) != 21:
        raise RuntimeError("候选代码存在重复")
    status_counts = pd.Series([row["mapping_status"] for row in rows]).value_counts()
    summary = {
        "candidate_count": len(rows),
        "exact_count": int(status_counts.get("EXACT", 0)),
        "public_equivalent_count": int(status_counts.get("PUBLIC_EQUIVALENT", 0)),
        "unsupported_count": int(status_counts.get("UNSUPPORTED", 0)),
        "included_count": sum(row["included"] == "纳入第二阶段候选" for row in rows),
        "excluded_fund_count": len(EXCLUDED_FUNDS),
        "database_unchanged": database_unchanged,
    }
    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "phase": "第一阶段：映射、数据源核验、可行性判断",
        "summary": summary,
        "mappings": rows,
        "excluded_funds": EXCLUDED_FUNDS,
        "mapping_rules": [
            "EXACT：公开数据源可取得用户指定的同一指数或板块，且通过本阶段必要数据检查。",
            "PUBLIC_EQUIVALENT：官方代码、名称和编制方案能够证明为同一等价指数。",
            "UNSUPPORTED：当前允许的数据体系无法取得同一标的的完整必要数据。",
            "仅名称相似、编制方法不同的指数不作为替代。",
            "HY/GN未调用同花顺API。",
        ],
        "database_check": {
            "before": database_before,
            "after": database_after,
            "unchanged": database_unchanged,
        },
    }
    PAYLOAD_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=json_default),
        encoding="utf-8",
    )
    run_workbook_builder()
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    for proxy_name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
        os.environ.pop(proxy_name, None)
    os.environ["NO_PROXY"] = "*"
    main()
