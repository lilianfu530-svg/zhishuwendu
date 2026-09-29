from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import akshare as ak
import duckdb
import pandas as pd

import main
import phase2_compute_models as models
from phase2_fetch_index_history import fetch_hstech_range
from phase2_fetch_stock_history import fetch_one


ROOT = Path(__file__).resolve().parent
DB = ROOT / "data" / "market.duckdb"
OUT = ROOT / "data" / "update_20260924"
START = pd.Timestamp("2026-09-23")
END = pd.Timestamp("2026-09-24")
DATES = [START.date(), END.date()]
INDEX_FIELDS = ["index_code", "date", "open", "high", "low", "close", "pct_change", "volume", "amount", "source", "fetched_at", "source_symbol"]
STOCK_FIELDS = ["market", "stock_code", "date", "open", "high", "low", "close", "pct_change", "volume", "amount", "source", "fetched_at"]


def registered(connection):
    return connection.execute(
        "SELECT index_code, primary_source, source_symbol FROM index_registry "
        "WHERE status IN ('active','temperature_pending') ORDER BY index_code"
    ).fetchall()


def write_parquet(frame: pd.DataFrame, path: Path) -> None:
    local = duckdb.connect()
    try:
        local.register("stage_frame", frame)
        local.execute("COPY stage_frame TO ? (FORMAT PARQUET)", [str(path)])
    finally:
        local.close()


def fetch_index(code, source, symbol, previous_close):
    if source == "同花顺金融数据服务":
        raw = main.fetch_hithink_index_range(symbol, START, END, previous_close)
        if raw.empty:
            return pd.DataFrame(columns=INDEX_FIELDS)
        raw = raw.rename(columns={"日期": "date", "开盘": "open", "最高": "high", "最低": "low", "收盘": "close", "成交量": "volume", "成交额": "amount"})
    elif code == "399006" and source.startswith("国证指数官网"):
        raw = ak.index_hist_cni(symbol=code, start_date="20260923", end_date="20260924")
        raw = raw.rename(columns={"日期": "date", "开盘价": "open", "最高价": "high", "最低价": "low", "收盘价": "close", "成交量": "volume", "成交额": "amount"})
        raw["volume"] = pd.to_numeric(raw["volume"], errors="coerce") * 1_000_000
        raw["amount"] = pd.to_numeric(raw["amount"], errors="coerce") * 100_000_000
    elif source.startswith("中证指数官网"):
        raw = ak.stock_zh_index_hist_csindex(symbol=code, start_date="20260923", end_date="20260924")
        raw = raw.rename(columns={"日期": "date", "开盘": "open", "最高": "high", "最低": "low", "收盘": "close", "成交量": "volume", "成交金额": "amount"})
        raw["amount"] = pd.to_numeric(raw["amount"], errors="coerce") * 100_000_000
    elif code == "HSTECH" and source == "腾讯财经区间指数日K":
        raw = fetch_hstech_range("20260923", "20260924")
    else:
        raise RuntimeError(f"{code}来源锁与取数路由不一致")
    if raw.empty:
        return pd.DataFrame(columns=INDEX_FIELDS)
    raw = raw.copy()
    raw["date"] = pd.to_datetime(raw["date"]).dt.date
    raw = raw[raw["date"].isin(DATES)].sort_values("date").drop_duplicates("date")
    for field in ("open", "high", "low", "close", "volume", "amount"):
        raw[field] = pd.to_numeric(raw[field], errors="coerce")
    if code in ("HSTECH", "931787"):
        raw = raw[raw["amount"] > 0]
    raw["pct_change"] = raw["close"] / raw["close"].shift(1) - 1
    if not raw.empty:
        raw.loc[raw.index[0], "pct_change"] = raw.iloc[0]["close"] / previous_close - 1
    raw["index_code"] = code
    raw["source"] = source
    raw["source_symbol"] = symbol
    raw["fetched_at"] = datetime.now()
    return raw[INDEX_FIELDS]


def stage():
    OUT.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(DB), read_only=True)
    rows = registered(connection)
    missing = []
    for code, source, symbol in rows:
        dates = {r[0] for r in connection.execute("SELECT date FROM index_daily WHERE index_code=? AND date BETWEEN ? AND ?", [code, START.date(), END.date()]).fetchall()}
        absent = set(DATES) - dates
        if absent:
            if not symbol:
                raise RuntimeError(f"{code}缺少锁定source_symbol")
            prior = connection.execute("SELECT close FROM index_daily WHERE index_code=? AND date<? ORDER BY date DESC LIMIT 1", [code, min(absent)]).fetchone()
            if prior is None or prior[0] is None:
                raise RuntimeError(f"{code}缺少前收盘")
            missing.append((code, source, symbol, prior[0], absent))
    print(f"缺口指数 {len(missing)}；只取 {START.date()} 至 {END.date()}", flush=True)
    frames, failures = [], {}
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {pool.submit(fetch_index, code, source, symbol, prior): (code, absent) for code, source, symbol, prior, absent in missing}
        for future in as_completed(futures):
            code, absent = futures[future]
            try:
                frame = future.result()
                frame = frame[frame["date"].isin(absent)]
                if set(frame["date"]) != absent:
                    raise RuntimeError(f"缺少日期 {sorted(absent-set(frame['date']))}")
                frames.append(frame)
                print(f"{code}: {len(frame)} 日", flush=True)
            except Exception as exc:
                failures[code] = f"{type(exc).__name__}: {exc}"
                print(f"{code}: 取数失败 {failures[code]}", flush=True)
    if frames:
        write_parquet(pd.concat(frames, ignore_index=True), OUT / "index_stage.parquet")
    hk = connection.execute(
        "WITH members AS (SELECT DISTINCT market,stock_code FROM index_constituents WHERE index_code IN "
        "(SELECT index_code FROM index_registry WHERE status IN ('active','temperature_pending')) "
        "AND effective_date=(SELECT MAX(effective_date) FROM index_constituents b WHERE b.index_code=index_constituents.index_code)) "
        "SELECT m.market,m.stock_code,MAX_BY(s.close,s.date) FROM members m JOIN stock_daily s USING(market,stock_code) "
        "WHERE m.market='HK' AND s.date<? AND EXISTS (SELECT 1 FROM stock_daily p WHERE p.market=m.market AND p.stock_code=m.stock_code AND p.source='腾讯财经区间日K') "
        "GROUP BY 1,2 HAVING COUNT(*) FILTER (WHERE s.date>=DATE '2026-09-22')>0",
        [START.date()],
    ).fetchall()
    needed = []
    for market, code, prior in hk:
        dates = {r[0] for r in connection.execute("SELECT date FROM stock_daily WHERE market=? AND stock_code=? AND date BETWEEN ? AND ?", [market, code, START.date(), END.date()]).fetchall()}
        if set(DATES)-dates:
            needed.append((market, code, prior, set(DATES)-dates))
    connection.close()
    print(f"港股股票缺口 {len(needed)} 只；本地A股缓存复用", flush=True)
    hk_frames = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {pool.submit(fetch_one, market, code, START, END, prior): (market, code, absent) for market, code, prior, absent in needed}
        for future in as_completed(futures):
            market, code, absent = futures[future]
            try:
                frame = future.result()
                frame = frame[frame["date"].dt.date.isin(absent)]
                if set(frame["date"].dt.date) != absent:
                    raise RuntimeError(f"缺少日期 {sorted(absent-set(frame['date'].dt.date))}")
                hk_frames.append(frame)
            except Exception as exc:
                failures[f"{market}.{code}"] = f"{type(exc).__name__}: {exc}"
    if hk_frames:
        write_parquet(pd.concat(hk_frames, ignore_index=True), OUT / "hk_stock_stage.parquet")
    (OUT / "stage_summary.json").write_text(json.dumps({"index_missing":len(missing), "index_staged":sum(map(len,frames)), "hk_needed":len(needed), "hk_staged":sum(map(len,hk_frames)), "failures":failures}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"阶段完成：指数{sum(map(len,frames))}行，港股{sum(map(len,hk_frames))}行，失败{len(failures)}", flush=True)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest().upper()


def current_members_sql() -> str:
    return (
        "WITH latest AS (SELECT index_code,MAX(effective_date) AS effective_date FROM index_constituents GROUP BY 1), "
        "members AS (SELECT DISTINCT x.market,x.stock_code FROM index_constituents x "
        "JOIN latest l USING(index_code,effective_date) JOIN index_registry r USING(index_code) "
        "WHERE r.status IN ('active','temperature_pending')) "
    )


def load_a_stock_stage(connection) -> pd.DataFrame:
    cache = ROOT / "data" / "source_migration_validation_20260925" / "hithink_daily_k_10d.parquet"
    query = current_members_sql() + (
        "SELECT m.market,m.stock_code,CAST((to_timestamp(h.date_ms/1000) AT TIME ZONE 'Asia/Shanghai') AS DATE) date, "
        "h.open_price AS \"open\",h.high_price AS \"high\",h.low_price AS \"low\",h.close_price AS \"close\",h.volume,h.turnover AS amount, "
        "p.close prior_close "
        "FROM members m JOIN stock_daily p ON p.market=m.market AND p.stock_code=m.stock_code AND p.date=DATE '2026-09-22' "
        "JOIN read_parquet(?) h ON h.thscode=m.stock_code||'.'||m.market "
        "WHERE m.market IN ('SH','SZ','BJ') AND p.source LIKE '同花顺金融数据服务%' "
        "AND CAST((to_timestamp(h.date_ms/1000) AT TIME ZONE 'Asia/Shanghai') AS DATE) BETWEEN DATE '2026-09-23' AND DATE '2026-09-24'"
    )
    data = connection.execute(query, [str(cache)]).df()
    if data.duplicated(["market", "stock_code", "date"]).any():
        raise RuntimeError("A股缓存重复业务键")
    data = data.sort_values(["market", "stock_code", "date"])
    previous = data.groupby(["market", "stock_code"])["close"].shift(1).fillna(data["prior_close"])
    data["pct_change"] = data["close"] / previous - 1
    data["source"] = "同花顺金融数据服务 REST A股日K（未复权）"
    data["fetched_at"] = datetime.now()
    existing = connection.execute(
        "SELECT market,stock_code,date FROM stock_daily WHERE date BETWEEN DATE '2026-09-23' AND DATE '2026-09-24'"
    ).df()
    data = data.merge(existing.assign(existing=True), on=["market", "stock_code", "date"], how="left")
    data = data[data["existing"].isna()]
    return data[STOCK_FIELDS]


def assert_no_old_changes(connection, backup_alias: str, table: str, key: list[str]) -> None:
    columns = [r[0] for r in connection.execute(f"DESCRIBE {table}").fetchall()]
    conditions = " OR ".join(f"s.{c} IS DISTINCT FROM b.{c}" for c in columns if c not in key)
    join = " AND ".join(f"s.{c}=b.{c}" for c in key)
    mismatch = connection.execute(
        f"SELECT COUNT(*) FROM {backup_alias}.{table} b LEFT JOIN {table} s ON {join} "
        f"WHERE s.{key[0]} IS NULL OR {conditions}"
    ).fetchone()[0]
    if mismatch:
        raise RuntimeError(f"{table}原有记录发生变化：{mismatch}")
    old_count = connection.execute(f"SELECT COUNT(*) FROM {backup_alias}.{table}").fetchone()[0]
    new_count = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    if new_count < old_count:
        raise RuntimeError(f"{table}原有行数减少")


def apply():
    summary = json.loads((OUT / "stage_summary.json").read_text(encoding="utf-8"))
    if summary["failures"] or summary["index_staged"] != 32 or summary["hk_staged"] != 144:
        raise RuntimeError("暂存不完整，不写正式库")
    connection = duckdb.connect(str(DB), read_only=True)
    registry = registered(connection)
    missing_codes = [code for code, _, _ in registry if not connection.execute(
        "SELECT 1 FROM index_daily WHERE index_code=? AND date=DATE '2026-09-24'", [code]
    ).fetchone()]
    staged_index = connection.execute("SELECT * FROM read_parquet(?)", [str(OUT / "index_stage.parquet")]).df()
    staged_hk = connection.execute("SELECT * FROM read_parquet(?)", [str(OUT / "hk_stock_stage.parquet")]).df()
    staged_a = load_a_stock_stage(connection)
    staged_index["date"] = pd.to_datetime(staged_index["date"]).dt.date
    staged_hk["date"] = pd.to_datetime(staged_hk["date"]).dt.date
    staged_a["date"] = pd.to_datetime(staged_a["date"]).dt.date
    if set(staged_index["index_code"]) != set(missing_codes) or staged_index.duplicated(["index_code", "date"]).any():
        raise RuntimeError("指数暂存与当前正式库缺口不一致")
    if staged_hk.duplicated(["market", "stock_code", "date"]).any() or staged_a.duplicated(["market", "stock_code", "date"]).any():
        raise RuntimeError("股票暂存重复业务键")
    if staged_index["close"].isna().any() or staged_a["close"].isna().any() or staged_hk["close"].isna().any():
        raise RuntimeError("暂存行情收盘价为空")
    if set(staged_index["date"]) != set(DATES):
        raise RuntimeError("暂存指数日期不完整")
    if not all(len(staged_index[staged_index.index_code == code]) == 2 for code in missing_codes):
        raise RuntimeError("各指数不是恰好两个缺口日")
    connection.close()
    backup_dir = ROOT / "data" / "backups"
    backup_dir.mkdir(exist_ok=True)
    backup = backup_dir / f"market_before_update_20260924_{datetime.now():%Y%m%d_%H%M%S}.duckdb"
    original_hash = sha256(DB)
    shutil.copy2(DB, backup)
    if sha256(backup) != original_hash:
        raise RuntimeError("正式库备份SHA256不一致")
    print(f"备份 {backup} SHA256 {original_hash}", flush=True)
    connection = duckdb.connect(str(DB))
    try:
        connection.execute(f"ATTACH '{backup.as_posix()}' AS baseline (READ_ONLY)")
        connection.execute("BEGIN TRANSACTION")
        for frame, table, columns in ((staged_index, "index_daily", INDEX_FIELDS), (staged_a, "stock_daily", STOCK_FIELDS), (staged_hk, "stock_daily", STOCK_FIELDS)):
            connection.register("incoming_rows", frame[columns])
            try:
                connection.execute(f"INSERT INTO {table} ({','.join(columns)}) SELECT {','.join(columns)} FROM incoming_rows")
            finally:
                connection.unregister("incoming_rows")
        for code in missing_codes:
            connection.execute("UPDATE index_registry SET last_data_date=DATE '2026-09-24' WHERE index_code=?", [code])
        print(f"写入指数{len(staged_index)}行，A股{len(staged_a)}行，港股{len(staged_hk)}行；计算新增模型", flush=True)
        models.CONSTITUENTS = ROOT / "data" / "index_update_20260922" / "model_constituents"
        models.validate_fast_correlation()
        models.frozen_v10.average_pairwise_correlation = models.fast_average_pairwise_correlation
        benchmark = models.load_index(connection, "000300")
        model_results = {}
        for code in missing_codes:
            model_results[code] = models.write_new_index(connection, code, benchmark)
            print(f"{code}: 模型新增{model_results[code].get('inserted_indicator_rows')}行", flush=True)
        for table, key in (("index_daily", ["index_code", "date"]), ("stock_daily", ["market", "stock_code", "date"]), ("indicator_daily", ["index_code", "date", "formula_version"])):
            assert_no_old_changes(connection, "baseline", table, key)
            duplicate = connection.execute(f"SELECT COUNT(*) FROM (SELECT {','.join(key)},COUNT(*) n FROM {table} GROUP BY {','.join(key)} HAVING n>1)").fetchone()[0]
            if duplicate:
                raise RuntimeError(f"{table}存在重复业务键")
        counts = connection.execute(
            "SELECT index_code,COUNT(*) FROM index_daily WHERE date=DATE '2026-09-24' AND index_code IN "
            "(SELECT index_code FROM index_registry WHERE status IN ('active','temperature_pending')) GROUP BY 1"
        ).fetchall()
        if len(counts) != len(registry) or any(n != 1 for _, n in counts):
            raise RuntimeError("9月24日已接入指数覆盖不完整")
        connection.execute("COMMIT")
        result = {"backup":str(backup), "backup_sha256":original_hash, "index_rows":len(staged_index), "a_stock_rows":len(staged_a), "hk_stock_rows":len(staged_hk), "models":model_results}
        (OUT / "apply_summary.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
        print("提交完成，旧行情和旧模型逐值不变，无重复业务键", flush=True)
    except Exception:
        connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["stage", "apply"])
    args = parser.parse_args()
    if args.mode == "stage":
        stage()
    else:
        apply()
