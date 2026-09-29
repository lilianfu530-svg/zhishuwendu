from __future__ import annotations

import json
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path

import akshare as ak
import exchange_calendars as xcals
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
LOG = ROOT / "logs" / "scheduled_task.log"
BAT = ROOT / "更新并发布.bat"


def record(message: str) -> None:
    LOG.parent.mkdir(exist_ok=True)
    with LOG.open("a", encoding="utf-8") as stream:
        stream.write(f"{datetime.now():%Y-%m-%d %H:%M:%S} {message}\n")


def trading_dates(year: int, market: str) -> set[date]:
    if market not in {"Ashare", "HK"}:
        raise ValueError(f"不支持的交易日历：{market}")
    prefix = "a_share" if market == "Ashare" else "hk"
    path = DATA / f"{prefix}_trading_dates_{year}.json"
    if path.exists():
        saved = json.loads(path.read_text(encoding="utf-8"))
        if saved.get("year") != year or not saved.get("dates"):
            raise RuntimeError(f"交易日历缓存无效：{path}")
        return {date.fromisoformat(value) for value in saved["dates"]}

    # 仅在本年缓存不存在时获取交易日历，不重复请求历史日期。
    if market == "Ashare":
        frame = ak.tool_trade_date_hist_sina()
        dates = sorted({pd.Timestamp(value).date() for value in frame["trade_date"]
                        if pd.Timestamp(value).year == year})
        source = "新浪财经交易日历（AKShare）"
    else:
        calendar = xcals.get_calendar("XHKG", start=f"{year}-01-01", end=f"{year}-12-31")
        dates = sorted(value.date() for value in calendar.sessions)
        source = "exchange_calendars XHKG"
    if not dates or dates[-1] < date(year, 12, 1):
        raise RuntimeError(f"数据源尚未提供完整的 {year} 年 {market} 交易日历")
    DATA.mkdir(exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps({"year": year, "market": market, "source": source,
                    "fetched_at": datetime.now().isoformat(timespec="seconds"),
                    "dates": [day.isoformat() for day in dates]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(path)
    record(f"{market} 交易日历已缓存：{year} 年，{len(dates)} 个交易日，{path}")
    return set(dates)


def main() -> int:
    today = datetime.now().date()
    if today.weekday() >= 5:
        record(f"{today} 为周末，跳过更新")
        return 0
    markets = [market for market in ("Ashare", "HK") if today in trading_dates(today.year, market)]
    if not markets:
        record(f"{today} 非 A 股及港股交易日，跳过更新")
        return 0
    if not BAT.is_file():
        raise FileNotFoundError(BAT)
    record(f"{today} 为 {','.join(markets)} 交易日，开始执行更新并发布.bat")
    completed = subprocess.run(
        ["cmd.exe", "/d", "/c", str(BAT), "--scheduled"],
        cwd=ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace",
        check=False,
    )
    with LOG.open("a", encoding="utf-8") as stream:
        stream.write(completed.stdout)
        stream.write(completed.stderr)
    record(f"更新并发布.bat 结束，退出码 {completed.returncode}")
    return completed.returncode


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        record(f"计划任务失败：{type(exc).__name__}: {exc}")
        print(f"计划任务失败：{type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
