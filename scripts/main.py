from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import time
import traceback
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
from update_missing_20260924 import INDEX_FIELDS, STOCK_FIELDS, assert_no_old_changes, write_parquet
from scheduled_run import trading_dates

DB = ROOT / "data" / "market.duckdb"
LOG = ROOT / "logs" / "update.log"
PAGE = ROOT / "index.html"
MEMBERS = ROOT / "data" / "index_update_20260922" / "model_constituents"
STATUS = ROOT / "logs" / "daily_update_status.json"
ISSUES: list[dict] = []


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


def issue(identity: str, reason: str, *, retryable: bool) -> None:
    ISSUES.append({"identity": identity, "reason": reason, "retryable": retryable})
    log(f"{identity}：{reason}；保持真实缺失状态，{'等待补跑' if retryable else '需要核实'}")


def request_primary(identity: str, action):
    # 仅重试原主源；单个标的失败不丢弃其他已经取得的行情。
    for attempt in range(1, 4):
        try:
            return action()
        except Exception as exc:
            if attempt == 3:
                raise
            log(f"{identity} 主源请求失败 {type(exc).__name__}，第 {attempt}/3 次，稍后重试")
            time.sleep(3 * attempt)


def save_status(status: dict) -> None:
    STATUS.parent.mkdir(exist_ok=True)
    temporary = STATUS.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(status, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(STATUS)


def registered(connection) -> list[tuple]:
    return sources.source_rows(connection)


def index_from_primary(code: str, source: str, symbol: str, expected: list, previous_close, fetch) -> pd.DataFrame:
    # 已成功取得的暂存行情先落盘，后续股票或写库失败时可直接断点续跑。
    directory = ROOT / "data" / "daily_update_cache" / "indices"
    directory.mkdir(parents=True, exist_ok=True)
    identity = hashlib.sha256(f"{code}|{source}|{symbol}".encode("utf-8")).hexdigest()[:20]
    path = directory / f"{identity}.parquet"
    with duckdb.connect() as local:
        cached = local.execute("SELECT * FROM read_parquet(?)", [str(path)]).df() if path.exists() else pd.DataFrame(columns=INDEX_FIELDS)
    cached["date"] = pd.to_datetime(cached["date"]).dt.date
    missing = sorted(set(expected) - set(cached["date"]))
    if missing:
        prior = cached[(cached.date < min(missing)) & (cached.date >= min(expected))].sort_values("date")
        if not prior.empty:
            previous_close = prior.iloc[-1]["close"]
        incoming = request_primary(code, lambda: fetch(missing, previous_close))
        if not incoming.empty:
            if incoming[["open", "high", "low", "close"]].isna().any().any():
                raise RuntimeError(f"{code} 新行情价格为空")
            cached = incoming.copy() if cached.empty else pd.concat([cached, incoming], ignore_index=True)
            cached = cached.drop_duplicates(["index_code", "date"], keep="first")
            temporary = path.with_suffix(".parquet.tmp")
            write_parquet(cached, temporary)
            temporary.replace(path)
    else:
        log(f"{code}：复用已核对主源身份的本地暂存行情 {len(expected)} 条")
    return cached[cached.date.isin(expected)].copy()


def fetch_indices(connection, registry: list[tuple], target: pd.Timestamp) -> pd.DataFrame:
    frames = []
    for code, market, source, symbol in registry:
        last, previous_close, first_date = connection.execute(
            "SELECT MAX(date),MAX_BY(close,date),MIN(date) FROM index_daily WHERE index_code=? AND close IS NOT NULL", [code]
        ).fetchone()
        start = max(pd.Timestamp(first_date), min(pd.Timestamp(last) + pd.Timedelta(days=1), target - pd.Timedelta(days=14)))
        if not source or not symbol or previous_close is None:
            raise RuntimeError(f"{code} 缺少锁定来源或前收盘")
        # 同时核查近期实际交易日，防止部分成功后最大日期掩盖中间缺口。
        required = sorted(
            day for year in range(start.year, target.year + 1)
            for day in trading_dates(year, market)
            if start.date() <= day <= target.date()
        )
        local_dates = {row[0] for row in connection.execute(
            "SELECT date FROM index_daily WHERE index_code=? AND date BETWEEN ? AND ? AND close IS NOT NULL",
            [code, start.date(), target.date()]).fetchall()}
        expected = [day for day in required if day not in local_dates]
        if not expected:
            continue
        groups = []
        for day in required:
            if day in local_dates:
                continue
            if not groups or required.index(day) != required.index(groups[-1][-1]) + 1:
                groups.append([])
            groups[-1].append(day)
        scale = sources.daily_scale(connection, "index_daily", "index_code=?", [code])
        try:
            pieces = []
            for days in groups:
                previous_close = connection.execute(
                    "SELECT close FROM index_daily WHERE index_code=? AND date<? AND close IS NOT NULL ORDER BY date DESC LIMIT 1",
                    [code, min(days)]).fetchone()[0]
                pieces.append(index_from_primary(code, source, symbol, days, previous_close,
                                                 lambda days, close: sources.fetch_index(code, source, symbol, days, close, scale)))
            frame = pd.concat(pieces, ignore_index=True)
        except ValueError as exc:
            if "Length mismatch" not in str(exc) or not source.startswith(("中证指数官网", "国证指数官网")):
                issue(code, f"主源请求失败 {type(exc).__name__}", retryable=True)
                continue
            # 官方接口尚未发布新日线时保持本地最后有效日期。
            log(f"{code}：官方主源尚未发布新日线，source_lag，最后交易日 {last}")
            issue(code, f"source_lag，缺少 {','.join(map(str, expected))}", retryable=True)
            continue
        except Exception as exc:
            issue(code, f"主源请求失败 {type(exc).__name__}", retryable=True)
            continue
        if not frame.empty:
            if frame[["open", "high", "low", "close"]].isna().any().any():
                raise RuntimeError(f"{code} 新行情价格为空")
            frames.append(frame)
        absent = sorted(set(expected) - set(frame["date"]))
        if absent:
            issue(code, f"source_lag，缺少 {','.join(map(str, absent))}", retryable=True)
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
    latest = connection.execute(
        'SELECT market,stock_code,MAX(date) last_date,MIN(date) first_date,MAX_BY(close,date) prior_close,MAX_BY(source,date) AS "source" '
        "FROM stock_daily GROUP BY 1,2"
    ).df()
    wanted = members.merge(latest, on=["market", "stock_code"], how="left", validate="one_to_one")
    if wanted["last_date"].isna().any():
        uninitialized = wanted.loc[wanted["last_date"].isna(), ["market", "stock_code"]]
        for row in uninitialized.itertuples(index=False):
            issue(f"{row.market}:{row.stock_code}", "缺少已初始化的股票历史，来源待核实，保持 NA", retryable=False)
        wanted = wanted[wanted["last_date"].notna()].copy()
    if wanted.empty:
        return pd.DataFrame(columns=STOCK_FIELDS)
    # 股票独立按交易日历补缺，不能以基准本次是否新增决定是否补跑。
    market_days = {}
    first = min(pd.Timestamp(wanted.last_date.min()) + pd.Timedelta(days=1), target - pd.Timedelta(days=14))
    for market in ("Ashare", "HK"):
        market_days[market] = sorted(
            day for year in range(first.year, target.year + 1)
            for day in trading_dates(year, market)
            if first.date() <= day <= target.date()
        )
    local = connection.execute(
        "SELECT market,stock_code,LIST(date) local_dates FROM stock_daily "
        "WHERE date BETWEEN ? AND ? AND close IS NOT NULL GROUP BY 1,2", [first.date(), target.date()]
    ).df()
    local_dates = {(row.market, row.stock_code): {pd.Timestamp(day).date() for day in row.local_dates}
                   for row in local.itertuples(index=False)}
    wanted["missing"] = wanted.apply(
        lambda row: [day for day in market_days["HK" if row.market == "HK" else "Ashare"]
                     if pd.Timestamp(day) >= max(pd.Timestamp(row.first_date), min(
                         pd.Timestamp(row.last_date) + pd.Timedelta(days=1), target - pd.Timedelta(days=14)))
                     and day not in local_dates.get((row.market, row.stock_code), set())],
        axis=1,
    )
    wanted = wanted[wanted.missing.map(bool)]
    if wanted.empty:
        log("成分股行情：本地已覆盖所需日期，无外部请求")
        return pd.DataFrame(columns=STOCK_FIELDS)
    # 主源以来源锁及已记录的换源证据为准，不从最后一行行情推断。
    evidence = connection.execute(
        "WITH transitions AS (SELECT canonical_code,new_source,source_transition_date,"
        "ROW_NUMBER() OVER (PARTITION BY canonical_code ORDER BY source_transition_date DESC,recorded_at DESC) n "
        "FROM data_source_transition WHERE entity_type='stock' AND source_transition_date<=?) "
        'SELECT COALESCE(k.canonical_code,t.canonical_code) AS "identity",CASE '
        "WHEN t.source_transition_date>=CAST(k.locked_at AS DATE) OR k.canonical_code IS NULL "
        "THEN t.new_source ELSE k.primary_source END locked_source "
        "FROM (SELECT * FROM data_source_lock WHERE entity_type='stock') k "
        "FULL JOIN (SELECT * FROM transitions WHERE n=1) t USING(canonical_code)", [target.date()]
    ).df()
    wanted["identity"] = wanted.market + ":" + wanted.stock_code
    wanted = wanted.merge(evidence, on="identity", how="left", validate="one_to_one")
    unverified = wanted.locked_source.isna()
    for row in wanted[unverified].itertuples():
        issue(row.identity, "source_unverified，缺少来源锁及换源证据，行情保留 NA", retryable=False)
    wanted = wanted[~unverified].copy()
    frames = []
    a_stocks = wanted[(wanted.market != "HK") & wanted.locked_source.str.startswith("同花顺金融数据服务")]
    if not a_stocks.empty:
        # 现有已核验的批量日 K 路径只下载一次，按已锁定股票及实际缺口过滤。
        sources.OUT = ROOT / "data" / "daily_update_cache"
        sources.TARGET = target
        sources.DUMP = sources.OUT / "hithink_daily_k_10d.parquet"
        request_primary("同花顺日 K 包", sources.get_dump)
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
            issue("同花顺日 K 包", f"主源未提供 {missing_count} 条有效日线，保留 NA，不推测停牌原因", retryable=False)
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
        if row.locked_source.startswith(("腾讯财经", "新浪财经")):
            try:
                frame = request_primary(row.identity, lambda: fetch_one(
                    row.market, row.stock_code, pd.Timestamp(min(row.missing)),
                    pd.Timestamp(max(row.missing)), row.prior_close))
            except Exception as exc:
                issue(row.identity, f"主源 {row.locked_source} 请求失败 {type(exc).__name__}", retryable=True)
                continue
            if frame.empty:
                log(f"{row.market}.{row.stock_code} 主源未返回新交易，保持 NA")
                continue
            frame = frame[frame.date.dt.date.isin(row.missing)]
            if frame.empty:
                log(f"{row.market}.{row.stock_code} 主源未返回新交易，保持 NA")
                continue
            if not frame.source.eq(row.locked_source).all():
                issue(row.identity, "返回来源与锁定证据不一致，拒绝写入", retryable=False)
                continue
            frames.append(frame)
        else:
            issue(row.identity, f"未知的已锁定股票来源 {row.locked_source}", retryable=False)
    if not frames:
        return pd.DataFrame(columns=STOCK_FIELDS)
    result = pd.concat(frames, ignore_index=True)[STOCK_FIELDS]
    result["date"] = pd.to_datetime(result["date"]).dt.date
    if result.duplicated(["market", "stock_code", "date"]).any():
        raise RuntimeError("股票暂存存在重复业务键")
    # 补中间缺口时按实际前一有效收盘计算，避免误用该股票最新一天的收盘。
    connection.register("staged_stock_closes", result[["market", "stock_code", "date", "close"]])
    try:
        closes = connection.execute(
            "WITH bounds AS (SELECT market,stock_code,MIN(date) first_date,MAX(date) last_date "
            "FROM staged_stock_closes GROUP BY 1,2) "
            "SELECT s.market,s.stock_code,s.date,s.close FROM stock_daily s JOIN bounds b USING(market,stock_code) "
            "WHERE s.date BETWEEN b.first_date AND b.last_date AND s.close IS NOT NULL UNION ALL "
            "SELECT s.market,s.stock_code,MAX(s.date),MAX_BY(s.close,s.date) FROM stock_daily s "
            "JOIN bounds b USING(market,stock_code) WHERE s.date<b.first_date AND s.close IS NOT NULL GROUP BY 1,2"
        ).df()
    finally:
        connection.unregister("staged_stock_closes")
    closes["date"] = pd.to_datetime(closes["date"]).dt.date
    closes = pd.concat([closes, result[["market", "stock_code", "date", "close"]]], ignore_index=True)
    closes = closes.drop_duplicates(["market", "stock_code", "date"], keep="first").sort_values(["market", "stock_code", "date"])
    closes["normalized_change"] = closes.close / closes.groupby(["market", "stock_code"]).close.shift() - 1
    result = result.merge(closes[["market", "stock_code", "date", "normalized_change"]],
                          on=["market", "stock_code", "date"], how="left", validate="one_to_one")
    result["pct_change"] = result.pop("normalized_change")
    log(f"成分股行情：新增 {len(result)} 条")
    return result


def insert_frame(connection, table: str, frame: pd.DataFrame, fields: list[str]) -> None:
    if frame.empty:
        return
    connection.register("incoming_rows", frame[fields])
    try:
        connection.execute(
            f"INSERT INTO {table} ({','.join(fields)}) SELECT {','.join(fields)} FROM incoming_rows ON CONFLICT DO NOTHING"
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
    ISSUES.clear()
    target = pd.Timestamp(os.environ.get("INDEX_UPDATE_DATE", str(datetime.now().date())))
    status = {"target_date": str(target.date()), "started_at": datetime.now().isoformat(timespec="seconds"),
              "state": "running"}
    save_status(status)
    log("开始更新；正式库优先，按注册表和各业务键核查新增日期")
    with duckdb.connect(str(DB), read_only=True) as connection:
        registry = registered(connection)
        staged_indices = fetch_indices(connection, registry, target)
        try:
            staged_stocks = fetch_stocks(connection, staged_indices, target)
        except Exception as exc:
            issue("成分股行情", f"主源请求或标准化失败 {type(exc).__name__}", retryable=True)
            staged_stocks = pd.DataFrame(columns=STOCK_FIELDS)
    if not staged_indices.empty or not staged_stocks.empty:
        backup_dir = ROOT / "data" / "backups"
        backup_dir.mkdir(exist_ok=True)
        backup = backup_dir / f"market_before_auto_update_{datetime.now():%Y%m%d_%H%M%S}.duckdb"
        original_hash = sha256(DB)
        shutil.copy2(DB, backup)
        if sha256(backup) != original_hash:
            raise RuntimeError("正式库备份 SHA-256 校验失败")
        log(f"正式库已备份：{backup} SHA-256 {original_hash}")
        status["backup"] = {"path": str(backup), "sha256": original_hash,
                            "created_at": datetime.now().isoformat(timespec="seconds")}
        with duckdb.connect(str(DB)) as connection:
            escaped_backup = str(backup).replace("'", "''")
            connection.execute(f"ATTACH '{escaped_backup}' AS before_update (READ_ONLY)")
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
                    assert_no_old_changes(connection, "before_update", table, keys.split(","))
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
        status["indices"] = []
        for code, market, source, symbol in registry:
            expected = max(day for year in (target.year - 1, target.year)
                           for day in trading_dates(year, market) if day <= target.date()) if target.month == 1 else max(
                               day for day in trading_dates(target.year, market) if day <= target.date())
            latest = connection.execute("SELECT MAX(date) FROM index_daily WHERE index_code=?", [code]).fetchone()[0]
            versions = {} if code == "000300" else dict(connection.execute(
                "SELECT formula_version,MAX(date) FROM indicator_daily WHERE index_code=? GROUP BY 1", [code]).fetchall())
            if latest is None or latest < expected or (code != "000300" and any(
                    versions.get(version) is None or versions[version] < expected for version in ("V1.0", "V1.1"))):
                issue(code, f"日期未达标，预期 {expected}，行情 {latest}，指标 {versions}", retryable=True)
            status["indices"].append({"index_code": code, "market": market, "primary_source": source,
                                      "source_symbol": symbol, "expected_date": str(expected),
                                      "last_data_date": str(latest),
                                      "versions": {key: str(value) for key, value in versions.items()}})
    status.update(state="complete" if not any(item["retryable"] for item in ISSUES) else "partial",
                  issues=ISSUES, inserted_index_rows=len(staged_indices), inserted_stock_rows=len(staged_stocks),
                  page_sha256=sha256(PAGE), finished_at=datetime.now().isoformat(timespec="seconds"))
    save_status(status)
    log(f"更新完成；最新数据日期：{last}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log(f"本次更新失败：{type(exc).__name__}: {exc}；本地历史数据未删除")
        save_status({"target_date": os.environ.get("INDEX_UPDATE_DATE", str(datetime.now().date())),
                     "state": "failed", "error": type(exc).__name__,
                     "finished_at": datetime.now().isoformat(timespec="seconds")})
        traceback.print_exc()
        raise SystemExit(1) from exc
