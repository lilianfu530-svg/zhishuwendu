from __future__ import annotations

import hashlib
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import duckdb
import pandas as pd

import build_dashboard
import phase2_compute_models as models
import update_20260928 as sources
from phase2_fetch_stock_history import fetch_one
from update_missing_20260924 import INDEX_FIELDS, STOCK_FIELDS

DB = ROOT / "data" / "market.duckdb"
LOG = ROOT / "logs" / "update.log"
PAGE = ROOT / "index.html"
MEMBERS = ROOT / "data" / "index_update_20260922" / "model_constituents"


def log(message: str) -> None:
    LOG.parent.mkdir(exist_ok=True)
    line = f"{datetime.now():%Y-%m-%d %H:%M:%S} {message}"
    print(line, flush=True)
    with LOG.open("a", encoding="utf-8") as stream:
        stream.write(line + "\n")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def registered(connection) -> list[tuple]:
    return sources.source_rows(connection)


def fetch_indices(connection, registry: list[tuple], target: pd.Timestamp) -> pd.DataFrame:
    frames = []
    for code, market, source, symbol in registry:
        last, previous_close = connection.execute(
            "SELECT date,close FROM index_daily WHERE index_code=? ORDER BY date DESC LIMIT 1", [code]
        ).fetchone()
        start = pd.Timestamp(last) + pd.Timedelta(days=1)
        if start > target:
            continue
        if not source or not symbol or previous_close is None:
            raise RuntimeError(f"{code} 缺少锁定来源或前收盘")
        # 只请求正式库最大日期之后的小窗口，不重新下载历史。
        expected = pd.date_range(start, target, freq="B").date.tolist()
        scale = sources.daily_scale(connection, "index_daily", "index_code=?", [code])
        try:
            frame = sources.fetch_index(code, source, symbol, expected, previous_close, scale)
        except ValueError as exc:
            if "Length mismatch" not in str(exc) or not source.startswith(("中证指数官网", "国证指数官网")):
                raise
            # 官方接口尚未发布新日线时保持本地最后有效日期。
            log(f"{code}：官方主源尚未发布新日线，source_lag，最后交易日 {last}")
            continue
        if not frame.empty:
            if frame[["open", "high", "low", "close"]].isna().any().any():
                raise RuntimeError(f"{code} 新行情价格为空")
            frames.append(frame)
        log(f"{code}：原最新 {last}，新增 {len(frame)}，来源 {source}，最新 {frame['date'].max() if len(frame) else last}")
    if not frames:
        return pd.DataFrame(columns=INDEX_FIELDS)
    result = pd.concat(frames, ignore_index=True)[INDEX_FIELDS]
    if result.duplicated(["index_code", "date"]).any():
        raise RuntimeError("指数暂存存在重复业务键")
    return result


def current_stocks(connection) -> pd.DataFrame:
    return connection.execute(
        "WITH latest AS (SELECT index_code,MAX(effective_date) effective_date FROM index_constituents GROUP BY 1) "
        "SELECT DISTINCT c.market,c.stock_code FROM index_constituents c "
        "JOIN latest l USING(index_code,effective_date) JOIN index_registry r USING(index_code) "
        "WHERE r.status IN ('active','temperature_pending')"
    ).df()


def fetch_stocks(connection, staged_indices: pd.DataFrame, target: pd.Timestamp) -> pd.DataFrame:
    members = current_stocks(connection)
    if members.empty:
        raise RuntimeError("注册表内指数没有当前成分股")
    market_days = {}
    for market in ("Ashare", "HK"):
        benchmark = "000300" if market == "Ashare" else "HSTECH"
        market_days[market] = sorted(staged_indices.loc[
            staged_indices.index_code == benchmark, "date"
        ].tolist())
    if not any(market_days.values()):
        log("成分股行情：基准无新增交易日，复用正式库")
        return pd.DataFrame(columns=STOCK_FIELDS)
    latest = connection.execute(
        "SELECT market,stock_code,MAX(date) last_date,MAX_BY(close,date) prior_close,MAX_BY(source,date) source "
        "FROM stock_daily GROUP BY 1,2"
    ).df()
    wanted = members.merge(latest, on=["market", "stock_code"], how="left", validate="one_to_one")
    if wanted["last_date"].isna().any():
        raise RuntimeError("当前成分股缺少已初始化的股票历史")
    wanted["missing"] = wanted.apply(
        lambda row: [day for day in market_days["HK" if row.market == "HK" else "Ashare"] if day > row.last_date],
        axis=1,
    )
    wanted = wanted[wanted.missing.map(bool)]
    if wanted.empty:
        log("成分股行情：本地已覆盖所需日期，无外部请求")
        return pd.DataFrame(columns=STOCK_FIELDS)
    frames = []
    a_stocks = wanted[(wanted.market != "HK") & wanted.source.str.startswith("同花顺金融数据服务")]
    if not a_stocks.empty:
        # 现有已核验的批量日 K 路径只下载一次，按已锁定股票及实际缺口过滤。
        sources.OUT = ROOT / "data" / "daily_update_cache"
        sources.TARGET = target
        sources.DUMP = sources.OUT / "hithink_daily_k_10d.parquet"
        sources.get_dump()
        newest_dump_date = connection.execute(
            "SELECT MAX(CAST((to_timestamp(date_ms/1000) AT TIME ZONE 'Asia/Shanghai') AS DATE)) "
            "FROM read_parquet(?)", [str(sources.DUMP)]
        ).fetchone()[0]
        newest_needed_date = max(day for days in a_stocks.missing for day in days)
        if newest_dump_date is None or newest_dump_date < newest_needed_date:
            raise RuntimeError(f"同花顺日 K 包尚未发布 {newest_needed_date}，最新仅 {newest_dump_date}")
        dump = connection.execute(
            "SELECT h.thscode,CAST((to_timestamp(h.date_ms/1000) AT TIME ZONE 'Asia/Shanghai') AS DATE) date, "
            "h.open_price AS open,h.high_price AS high,h.low_price AS low,h.close_price AS close,"
            "h.volume,h.turnover AS amount FROM read_parquet(?) h",
            [str(sources.DUMP)],
        ).df()
        dump["date"] = pd.to_datetime(dump["date"]).dt.date
        needed = a_stocks[["market", "stock_code", "prior_close", "missing"]].explode("missing")
        needed["thscode"] = needed.stock_code + "." + needed.market
        joined = needed.merge(dump, left_on=["thscode", "missing"], right_on=["thscode", "date"], how="left")
        missing_count = int(joined["close"].isna().sum())
        if missing_count:
            log(f"同花顺日 K 包无有效成交：{missing_count} 条，按 NA 保留")
        joined = joined[joined["close"].notna()].copy()
        joined = joined.sort_values(["market", "stock_code", "date"])
        prior = joined.groupby(["market", "stock_code"])["close"].shift().fillna(joined.prior_close)
        joined["pct_change"] = joined.close / prior - 1
        joined["source"] = "同花顺金融数据服务 REST A股日K（未复权）"
        joined["fetched_at"] = datetime.now()
        frames.append(joined[STOCK_FIELDS])
        log(f"同花顺 A 股日 K：新增 {len(joined)} 条")
    others = wanted.drop(a_stocks.index)
    for row in others.itertuples():
        if row.market == "HK" or row.source.startswith("腾讯财经") or row.source.startswith("新浪财经"):
            frame = fetch_one(row.market, row.stock_code, pd.Timestamp(min(row.missing)),
                              pd.Timestamp(max(row.missing)), row.prior_close)
            frame = frame[frame.date.dt.date.isin(row.missing)]
            if frame.empty:
                log(f"{row.market}.{row.stock_code} 主源未返回新交易，保持 NA")
                continue
            if row.market != "HK" and row.source.startswith("新浪财经") and not frame.source.str.startswith("新浪财经").all():
                raise RuntimeError(f"{row.market}.{row.stock_code} 股票来源发生变化")
            frames.append(frame)
        else:
            raise RuntimeError(f"{row.market}.{row.stock_code} 未知股票来源 {row.source}")
    if not frames:
        return pd.DataFrame(columns=STOCK_FIELDS)
    result = pd.concat(frames, ignore_index=True)[STOCK_FIELDS]
    result["date"] = pd.to_datetime(result["date"]).dt.date
    if result.duplicated(["market", "stock_code", "date"]).any():
        raise RuntimeError("股票暂存存在重复业务键")
    log(f"成分股行情：新增 {len(result)} 条")
    return result


def insert_frame(connection, table: str, frame: pd.DataFrame, fields: list[str]) -> None:
    if frame.empty:
        return
    connection.register("incoming_rows", frame[fields])
    try:
        connection.execute(
            f"INSERT INTO {table} ({','.join(fields)}) SELECT {','.join(fields)} FROM incoming_rows"
        )
    finally:
        connection.unregister("incoming_rows")


def publish_page() -> bool:
    payload = json.dumps(build_dashboard.read_dashboard_data(), ensure_ascii=False,
                         separators=(",", ":"), allow_nan=False)
    payload = payload.replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
    content = build_dashboard.HTML.replace("__DATA__", payload)
    if PAGE.exists() and PAGE.read_text(encoding="utf-8") == content:
        return False
    temporary = PAGE.with_suffix(".html.tmp")
    temporary.write_text(content, encoding="utf-8")
    temporary.replace(PAGE)
    return True


def main() -> None:
    if not DB.is_file():
        raise FileNotFoundError(DB)
    target = pd.Timestamp(datetime.now().date())
    log("开始更新；正式库优先，按注册表和各业务键核查新增日期")
    with duckdb.connect(str(DB), read_only=True) as connection:
        registry = registered(connection)
        staged_indices = fetch_indices(connection, registry, target)
        staged_stocks = fetch_stocks(connection, staged_indices, target)
    if not staged_indices.empty or not staged_stocks.empty:
        backup_dir = ROOT / "data" / "backups"
        backup_dir.mkdir(exist_ok=True)
        backup = backup_dir / f"market_before_auto_update_{datetime.now():%Y%m%d_%H%M%S}.duckdb"
        original_hash = sha256(DB)
        shutil.copy2(DB, backup)
        if sha256(backup) != original_hash:
            raise RuntimeError("正式库备份 SHA-256 校验失败")
        log(f"正式库已备份：{backup} SHA-256 {original_hash}")
        with duckdb.connect(str(DB)) as connection:
            connection.execute("BEGIN TRANSACTION")
            try:
                insert_frame(connection, "index_daily", staged_indices, INDEX_FIELDS)
                insert_frame(connection, "stock_daily", staged_stocks, STOCK_FIELDS)
                for code, group in staged_indices.groupby("index_code"):
                    if code != "000300":
                        connection.execute("UPDATE index_registry SET last_data_date=? WHERE index_code=?",
                                           [max(group.date), code])
                models.CONSTITUENTS = MEMBERS
                models.validate_fast_correlation()
                models.frozen_v10.average_pairwise_correlation = models.fast_average_pairwise_correlation
                benchmark = models.load_index(connection, "000300")
                codes = [row[0] for row in registry if row[0] != "000300"]
                for code in codes:
                    latest_index, latest_v10, latest_v11 = connection.execute(
                        "SELECT (SELECT MAX(date) FROM index_daily WHERE index_code=?),"
                        "(SELECT MAX(date) FROM indicator_daily WHERE index_code=? AND formula_version='V1.0'),"
                        "(SELECT MAX(date) FROM indicator_daily WHERE index_code=? AND formula_version='V1.1')",
                        [code, code, code],
                    ).fetchone()
                    if latest_index > latest_v10 or latest_index > latest_v11:
                        result = models.write_new_index(connection, code, benchmark)
                        log(f"{code} 指标计算：{result['status']}，新增 {result.get('inserted_indicator_rows', 0)}")
                for table, keys in (("index_daily", "index_code,date"),
                                    ("stock_daily", "market,stock_code,date"),
                                    ("indicator_daily", "index_code,date,formula_version")):
                    duplicate = connection.execute(
                        f"SELECT COUNT(*) FROM (SELECT {keys} FROM {table} GROUP BY {keys} HAVING COUNT(*)>1)"
                    ).fetchone()[0]
                    if duplicate:
                        raise RuntimeError(f"{table} 存在重复业务键")
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
        log(f"写入完成：指数 {len(staged_indices)} 条，股票 {len(staged_stocks)} 条；旧数据保留")
    changed = publish_page()
    log(f"HTML 生成成功：{PAGE}；内容{'已更新' if changed else '无变化'}")
    with duckdb.connect(str(DB), read_only=True) as connection:
        last = connection.execute("SELECT MAX(date) FROM index_daily WHERE index_code IN "
                                  "(SELECT index_code FROM index_registry WHERE status IN ('active','temperature_pending'))").fetchone()[0]
    log(f"更新完成；最新数据日期：{last}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"本次更新失败：{type(exc).__name__}: {exc}；本地历史数据未删除")
        raise SystemExit(1) from exc
