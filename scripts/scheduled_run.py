from __future__ import annotations

import json
import msvcrt
import os
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import akshare as ak
import duckdb
import exchange_calendars as xcals
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
LOG = ROOT / "logs" / "scheduled_task.log"
BAT = ROOT / "更新并发布.bat"
PUBLISHED = ROOT / "logs" / "publish_status.json"


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
    now = datetime.now()
    # 补跑只处理已经收盘的交易日；休市或次日登录仍可补发上一交易日。
    cutoff = now.date() if now.hour >= 19 else now.date() - timedelta(days=1)
    with duckdb.connect(str(DATA / "market.duckdb"), read_only=True) as connection:
        markets = [row[0] for row in connection.execute(
            "SELECT DISTINCT market FROM index_registry WHERE status IN ('active','temperature_pending')"
        ).fetchall()]
    if not markets:
        raise RuntimeError("注册表没有需要更新的市场")
    targets = {}
    for market in markets:
        days = {day for day in trading_dates(cutoff.year, market) if day <= cutoff}
        if not days:
            days = trading_dates(cutoff.year - 1, market)
        targets[market] = max(days)
    target = max(targets.values())
    if not BAT.is_file():
        raise FileNotFoundError(BAT)
    environment = os.environ.copy()
    environment.update(INDEX_UPDATE_DATE=str(target), PYTHONIOENCODING="utf-8", PYTHONUTF8="1")
    record(f"各市场最近收盘交易日 {targets}，开始执行更新并发布.bat")
    # 直接写日志，进程中断时仍能保留已经执行的阶段和异常现场。
    with LOG.open("a", encoding="utf-8") as stream:
        completed = subprocess.run(
            ["cmd.exe", "/d", "/c", str(BAT), "--scheduled"], cwd=ROOT,
            stdout=stream, stderr=subprocess.STDOUT, env=environment, check=False,
        )
    record(f"更新并发布.bat 结束，退出码 {completed.returncode}")
    if completed.returncode == 0:
        published = json.loads(PUBLISHED.read_text(encoding="utf-8"))
        if published["target_date"] != str(target) or published["data_state"] != "complete":
            raise RuntimeError("发布记录未覆盖本次应更新的交易日")
    else:
        record("更新尚未完整完成，向 Windows 计划任务返回非零状态，等待自动重试")
    return completed.returncode


if __name__ == "__main__":
    try:
        LOG.parent.mkdir(exist_ok=True)
        with (LOG.parent / "daily_update.lock").open("a+b") as lock:
            lock.seek(0, 2)
            if lock.tell() == 0:
                lock.write(b"0")
                lock.flush()
            lock.seek(0)
            try:
                msvcrt.locking(lock.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                record("已有更新进程持有运行锁，本次不重复执行")
                raise SystemExit(1)
            try:
                raise SystemExit(main())
            finally:
                lock.seek(0)
                msvcrt.locking(lock.fileno(), msvcrt.LK_UNLCK, 1)
    except Exception as exc:
        record(f"计划任务失败：{type(exc).__name__}: {exc}")
        print(f"计划任务失败：{type(exc).__name__}: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
