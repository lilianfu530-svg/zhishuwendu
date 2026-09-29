from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from urllib.parse import urlencode

import akshare as ak
import duckdb
import pandas as pd

import main
import phase2_compute_models as models
from phase2_fetch_index_history import fetch_hstech_range
from phase2_fetch_stock_history import fetch_one
from update_missing_20260924 import INDEX_FIELDS, STOCK_FIELDS, assert_no_old_changes, write_parquet


ROOT = Path(__file__).resolve().parent
DB = ROOT / "data" / "market.duckdb"
OUT = ROOT / "data" / "update_20260928"
TARGET = pd.Timestamp("2026-09-28")
HK_EXTRA = pd.Timestamp("2026-09-25")
DUMP = OUT / "hithink_daily_k_10d.parquet"


def get_dump() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    if DUMP.exists():
        probe = duckdb.connect()
        try:
            latest = probe.execute(
                "SELECT MAX(CAST((to_timestamp(date_ms/1000) AT TIME ZONE 'Asia/Shanghai') AS DATE)) FROM read_parquet(?)",
                [str(DUMP)],
            ).fetchone()[0]
        finally:
            probe.close()
        if latest is not None and latest >= TARGET.date():
            print(f"复用本地10日行情包，最大日期 {latest}", flush=True)
            return
    key = os.getenv("HITHINK_FINANCE_API_KEY")
    if not key:
        raise RuntimeError("缺少 HITHINK_FINANCE_API_KEY")
    signed = None
    for attempt in range(3):
        try:
            request = Request(
                "https://fuyao.aicubes.cn/api/dump/market-dumps/daily-k-10d/download-url",
                headers={"X-api-key": key},
            )
            with urlopen(request, timeout=30) as response:
                signed = json.load(response)
            if signed.get("code") != 0:
                raise RuntimeError(f"日线包签名失败：code={signed.get('code')}")
            break
        except HTTPError as exc:
            if exc.code != 429 or attempt == 2:
                raise
            time.sleep(15 * (attempt + 1))
    temporary = OUT / "hithink_daily_k_10d.download"
    with urlopen(signed["data"]["presigned_url"], timeout=120) as response, temporary.open("wb") as output:
        shutil.copyfileobj(response, output)
    probe = duckdb.connect()
    try:
        latest = probe.execute(
            "SELECT MAX(CAST((to_timestamp(date_ms/1000) AT TIME ZONE 'Asia/Shanghai') AS DATE)) FROM read_parquet(?)",
            [str(temporary)],
        ).fetchone()[0]
    finally:
        probe.close()
    temporary.replace(DUMP)
    print(f"下载10日行情包完成，最大日期 {latest}", flush=True)


def api_json(url: str, *, timeout: int = 30) -> dict:
    key = os.getenv("HITHINK_FINANCE_API_KEY")
    if not key:
        raise RuntimeError("缺少 HITHINK_FINANCE_API_KEY")
    for attempt in range(3):
        try:
            with urlopen(Request(url, headers={"X-api-key": key}), timeout=timeout) as response:
                payload = json.load(response)
            if payload.get("code") != 0:
                raise RuntimeError(f"同花顺返回业务错误 code={payload.get('code')}")
            return payload["data"]
        except HTTPError as exc:
            if exc.code != 429 or attempt == 2:
                raise
            time.sleep(10 * (attempt + 1))
    raise RuntimeError("同花顺请求未完成")


def source_rows(connection):
    registered = connection.execute(
        "SELECT index_code,market,primary_source,source_symbol FROM index_registry "
        "WHERE status IN ('active','temperature_pending') ORDER BY index_code"
    ).fetchall()
    benchmark = connection.execute(
        "SELECT primary_source,source_symbol FROM data_source_lock "
        "WHERE entity_type='index_benchmark' AND canonical_code='000300'"
    ).fetchone()
    if not benchmark:
        raise RuntimeError("000300缺少基准来源锁")
    return [("000300", "Ashare", *benchmark)] + registered


def daily_scale(connection, table: str, where: str, args: list) -> float:
    rows = connection.execute(
        f"SELECT date,close,pct_change FROM {table} WHERE {where} ORDER BY date DESC LIMIT 3", args
    ).fetchall()
    for i in range(len(rows) - 1):
        _, close, stored = rows[i]
        previous = rows[i + 1][1]
        if close is None or previous is None or previous == 0 or stored is None:
            continue
        calculated = close / previous - 1
        if abs(calculated) < 1e-8:
            continue
        ratio = stored / calculated
        if abs(ratio - 1) < 0.02:
            return 1.0
        if abs(ratio - 100) < 2:
            return 100.0
        raise RuntimeError(f"{table}历史涨跌幅单位无法确认")
    return 1.0


def fetch_index(code: str, source: str, symbol: str, dates: list, previous_close: float, scale: float) -> pd.DataFrame:
    start = pd.Timestamp(min(dates))
    end = pd.Timestamp(max(dates))
    if source == "同花顺金融数据服务":
        raw = main.fetch_hithink_index_range(symbol, start, end, previous_close)
        if not raw.empty:
            raw = raw.rename(columns={"日期": "date", "开盘": "open", "最高": "high", "最低": "low", "收盘": "close", "成交量": "volume", "成交额": "amount"})
    elif code == "399006" and source.startswith("国证指数官网"):
        raw = ak.index_hist_cni(symbol=code, start_date=start.strftime("%Y%m%d"), end_date=end.strftime("%Y%m%d"))
        raw = raw.rename(columns={"日期": "date", "开盘价": "open", "最高价": "high", "最低价": "low", "收盘价": "close", "成交量": "volume", "成交额": "amount"})
        if not raw.empty:
            raw["volume"] = pd.to_numeric(raw["volume"], errors="coerce") * 1_000_000
            raw["amount"] = pd.to_numeric(raw["amount"], errors="coerce") * 100_000_000
    elif source.startswith("中证指数官网"):
        raw = ak.stock_zh_index_hist_csindex(symbol=code, start_date=start.strftime("%Y%m%d"), end_date=end.strftime("%Y%m%d"))
        raw = raw.rename(columns={"日期": "date", "开盘": "open", "最高": "high", "最低": "low", "收盘": "close", "成交量": "volume", "成交金额": "amount"})
        if not raw.empty:
            raw["amount"] = pd.to_numeric(raw["amount"], errors="coerce") * 100_000_000
    elif code == "HSTECH" and source == "腾讯财经区间指数日K":
        raw = fetch_hstech_range(start.strftime("%Y%m%d"), end.strftime("%Y%m%d"))
    else:
        raise RuntimeError(f"{code}来源锁与取数路由不一致")
    if raw.empty:
        return pd.DataFrame(columns=INDEX_FIELDS)
    raw = raw.copy()
    raw["date"] = pd.to_datetime(raw["date"]).dt.date
    raw = raw[raw["date"].isin(dates)].sort_values("date").drop_duplicates("date")
    for field in ("open", "high", "low", "close", "volume", "amount"):
        raw[field] = pd.to_numeric(raw[field], errors="coerce")
    if code in ("HSTECH", "931787"):
        raw = raw[raw["amount"] > 0]
    prior = raw["close"].shift(1)
    if not raw.empty:
        prior.iloc[0] = previous_close
    raw["pct_change"] = (raw["close"] / prior - 1) * scale
    raw["index_code"] = code
    raw["source"] = source
    raw["source_symbol"] = symbol
    raw["fetched_at"] = datetime.now()
    return raw[INDEX_FIELDS]


def stage_indices() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(DB), read_only=True)
    sources = source_rows(connection)
    frames, failures = [], {}
    staged_path = OUT / "index_stage.parquet"
    staged_dates = set()
    if staged_path.exists():
        cached = connection.execute("SELECT * FROM read_parquet(?)", [str(staged_path)]).df()
        cached["date"] = pd.to_datetime(cached["date"]).dt.date
        frames.append(cached)
        staged_dates = set(zip(cached["index_code"], cached["date"]))
    for code, market, source, symbol in sources:
        expected = [TARGET.date()] if market == "Ashare" else [HK_EXTRA.date(), TARGET.date()]
        existing = {row[0] for row in connection.execute(
            "SELECT date FROM index_daily WHERE index_code=? AND date BETWEEN ? AND ?",
            [code, min(expected), max(expected)],
        ).fetchall()}
        missing = sorted(day for day in set(expected) - existing if (code, day) not in staged_dates)
        if not missing:
            continue
        prior = connection.execute(
            "SELECT close FROM index_daily WHERE index_code=? AND date<? ORDER BY date DESC LIMIT 1",
            [code, min(missing)],
        ).fetchone()
        if prior is None or prior[0] is None or not symbol:
            failures[code] = "缺少已锁定代码或有效前收盘"
            continue
        scale = daily_scale(connection, "index_daily", "index_code=?", [code])
        try:
            frame = fetch_index(code, source, symbol, missing, prior[0], scale)
            if frame[["open", "high", "low", "close"]].isna().any().any():
                raise RuntimeError("OHLC存在空值")
            if not frame.empty:
                frames.append(frame)
                print(f"{code}：已暂存 {len(frame)} 日", flush=True)
            if set(frame["date"]) != set(missing):
                failures[code] = f"来源尚缺日期 {sorted(set(missing)-set(frame['date']))}"
                print(f"{code}：{failures[code]}", flush=True)
        except Exception as exc:
            failures[code] = f"{type(exc).__name__}: {exc}"
            print(f"{code}：取数失败 {failures[code]}", flush=True)
        if source == "同花顺金融数据服务":
            time.sleep(0.7)
    connection.close()
    if frames:
        staged = pd.concat(frames, ignore_index=True)
        if staged.duplicated(["index_code", "date"]).any():
            raise RuntimeError("指数暂存重复业务键")
        write_parquet(staged, staged_path)
    (OUT / "index_stage_summary.json").write_text(
        json.dumps({"expected_rows":sum(1 if market == "Ashare" else 2 for _, market, _, _ in sources),
                    "staged_rows":sum(map(len, frames)), "failures":failures}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"指数暂存完成：{sum(map(len, frames))}行，失败{len(failures)}", flush=True)


def stage_a_stocks() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(DB), read_only=True)
    members = connection.execute(
        "WITH latest AS (SELECT index_code,MAX(effective_date) AS effective_date FROM index_constituents GROUP BY 1) "
        "SELECT DISTINCT c.market,c.stock_code FROM index_constituents c "
        "JOIN latest l USING(index_code,effective_date) JOIN index_registry r USING(index_code) "
        "WHERE r.status IN ('active','temperature_pending') AND c.market IN ('SH','SZ','BJ')"
    ).df()
    prior = connection.execute(
        "SELECT s.market,s.stock_code,s.close prior_close,s.source,p.close prior2_close,s.pct_change prior_change "
        "FROM stock_daily s LEFT JOIN stock_daily p ON p.market=s.market AND p.stock_code=s.stock_code "
        "AND p.date=DATE '2026-09-23' WHERE s.date=DATE '2026-09-24'"
    ).df()
    wanted = members.merge(prior, on=["market", "stock_code"])
    wanted = wanted[wanted["source"].str.startswith("同花顺金融数据服务", na=False)]
    wanted = wanted[~wanted.set_index(["market", "stock_code"]).index.isin(
        connection.execute("SELECT market,stock_code FROM stock_daily WHERE date=DATE '2026-09-28'").df().set_index(["market", "stock_code"]).index
    )]
    connection.close()
    if wanted.empty:
        print("9月28日A股成分行情无缺口", flush=True)
        return
    snapshots = []
    limit, offset, total = 1000, 0, None
    while total is None or offset < total:
        url = "https://fuyao.aicubes.cn/api/a-share/prices/snapshot?" + urlencode({"limit":limit,"offset":offset})
        data = api_json(url)
        timestamp = data.get("timestamp")
        if timestamp is None:
            raise RuntimeError("A股行情快照缺少上游时间")
        local = pd.to_datetime(timestamp, unit="ms", utc=True).tz_convert("Asia/Shanghai")
        if local.date() != TARGET.date() or local.hour < 15:
            raise RuntimeError(f"A股行情快照不是9月28日收市后数据：{local}")
        if total is None:
            total = data["total"]
        elif total != data["total"]:
            raise RuntimeError("A股行情快照分页期间总数变化")
        items = data.get("item") or []
        if not items:
            raise RuntimeError(f"A股行情快照第{offset}页为空")
        snapshots.extend(items)
        offset += len(items)
        print(f"A股快照已读取 {offset}/{total}", flush=True)
        time.sleep(0.35)
    snapshot = pd.DataFrame(snapshots)
    if snapshot.duplicated("thscode").any():
        raise RuntimeError("A股行情快照重复代码")
    wanted["thscode"] = wanted["stock_code"] + "." + wanted["market"]
    data = wanted.merge(snapshot, on="thscode", how="left", indicator=True)
    absent = data.loc[data["_merge"] != "both", "thscode"].tolist()
    if absent:
        raise RuntimeError(f"A股快照缺少已锁定股票 {len(absent)}只")
    for field in ("last_price", "open_price", "high_price", "low_price", "volume", "turnover"):
        data[field] = pd.to_numeric(data[field], errors="coerce")
    # 停牌或无当日价格的快照不作为交易日K线写入。
    data = data[(data["volume"] > 0) & data[["open_price","high_price","low_price","last_price"]].notna().all(axis=1)]
    if data.empty:
        raise RuntimeError("9月28日A股快照均无有效成交")
    scale_ratio = data["prior_change"] / (data["prior_close"] / data["prior2_close"] - 1)
    scale = pd.Series(1.0, index=data.index)
    scale.loc[(scale_ratio - 100).abs() < 2] = 100.0
    unknown = scale_ratio.notna() & (scale_ratio.abs() > 0.02) & ((scale_ratio-1).abs() >= 0.02) & ((scale_ratio-100).abs() >= 2)
    if unknown.any():
        raise RuntimeError(f"{unknown.sum()}只A股历史涨跌幅口径无法确认")
    incoming = pd.DataFrame({
        "market":data["market"], "stock_code":data["stock_code"], "date":TARGET.date(),
        "open":data["open_price"], "high":data["high_price"], "low":data["low_price"],
        "close":data["last_price"], "pct_change":(data["last_price"]/data["prior_close"]-1)*scale,
        "volume":data["volume"], "amount":data["turnover"],
        "source":"同花顺金融数据服务 REST A股行情快照（未复权）", "fetched_at":datetime.now(),
    })
    if incoming.duplicated(["market","stock_code","date"]).any():
        raise RuntimeError("A股快照暂存重复业务键")
    write_parquet(incoming[STOCK_FIELDS], OUT / "a_stock_stage.parquet")
    (OUT / "a_stock_summary.json").write_text(
        json.dumps({"wanted":len(wanted),"staged":len(incoming),"no_trade_or_null":len(wanted)-len(incoming),
                    "snapshot_total":total,"market_counts":incoming.groupby("market").size().to_dict()},
                   ensure_ascii=False, indent=2), encoding="utf-8",
    )
    print(f"A股成分暂存 {len(incoming)}行，无成交或缺价 {len(wanted)-len(incoming)}只", flush=True)


def stage_hk_stocks() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(DB), read_only=True)
    rows = connection.execute(
        "WITH latest AS (SELECT index_code,MAX(effective_date) AS effective_date FROM index_constituents GROUP BY 1), "
        "members AS (SELECT DISTINCT c.market,c.stock_code FROM index_constituents c "
        "JOIN latest l USING(index_code,effective_date) JOIN index_registry r USING(index_code) "
        "WHERE r.status IN ('active','temperature_pending') AND c.market='HK') "
        "SELECT m.market,m.stock_code,s.close FROM members m JOIN stock_daily s USING(market,stock_code) "
        "WHERE s.date=DATE '2026-09-24' AND s.source='腾讯财经区间日K' ORDER BY m.stock_code"
    ).fetchall()
    needed = []
    for market, code, prior in rows:
        existing = {r[0] for r in connection.execute(
            "SELECT date FROM stock_daily WHERE market=? AND stock_code=? AND date BETWEEN DATE '2026-09-25' AND DATE '2026-09-28'",
            [market,code],
        ).fetchall()}
        missing = {HK_EXTRA.date(),TARGET.date()}-existing
        if missing:
            needed.append((market,code,prior,missing))
    connection.close()
    frames, failures = [], {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        jobs = {pool.submit(fetch_one,market,code,HK_EXTRA,TARGET,prior):(market,code,missing) for market,code,prior,missing in needed}
        for future in as_completed(jobs):
            market,code,missing = jobs[future]
            try:
                frame = future.result()
                frame = frame[frame["date"].dt.date.isin(missing)]
                if set(frame["date"].dt.date) != missing:
                    raise RuntimeError(f"缺少日期 {sorted(missing-set(frame['date'].dt.date))}")
                frames.append(frame)
            except Exception as exc:
                failures[f"{market}.{code}"] = f"{type(exc).__name__}: {exc}"
    if frames:
        incoming = pd.concat(frames,ignore_index=True)
        if incoming.duplicated(["market","stock_code","date"]).any():
            raise RuntimeError("港股暂存重复业务键")
        write_parquet(incoming[STOCK_FIELDS], OUT / "hk_stock_stage.parquet")
    (OUT / "hk_stock_summary.json").write_text(
        json.dumps({"needed":len(needed),"staged":sum(map(len,frames)),"failures":failures},ensure_ascii=False,indent=2),
        encoding="utf-8",
    )
    print(f"港股成分暂存 {sum(map(len,frames))}行，失败{len(failures)}只", flush=True)


def verify_snapshot_sample() -> None:
    connection = duckdb.connect()
    staged = connection.execute("SELECT * FROM read_parquet(?)", [str(OUT / "a_stock_stage.parquet")]).df()
    connection.close()
    begin = int(pd.Timestamp("2026-09-28", tz="Asia/Shanghai").timestamp() * 1000)
    finish = int(pd.Timestamp("2026-09-29", tz="Asia/Shanghai").timestamp() * 1000)
    sample = staged.groupby("market",sort=True).head(1)
    result = []
    for row in sample.itertuples(index=False):
        symbol = f"{row.stock_code}.{row.market}"
        query = urlencode({"thscode":symbol,"interval":"1d","adjust":"none","start":begin,"end":finish})
        bars = api_json("https://fuyao.aicubes.cn/api/a-share/prices/historical?" + query)["item"]
        if len(bars) != 1:
            raise RuntimeError(f"{symbol}历史日K尚未提供9月28日")
        bar = bars[0]
        for source_field, staged_value in (("open_price",row.open),("high_price",row.high),
                                           ("low_price",row.low),("close_price",row.close),
                                           ("volume",row.volume)):
            if abs(float(bar[source_field])-float(staged_value)) > 1e-8:
                raise RuntimeError(f"{symbol}快照与日K的{source_field}不一致")
        if abs(float(bar["turnover"])-float(row.amount)) > max(10.0, abs(float(bar["turnover"])) * 1e-7):
            raise RuntimeError(f"{symbol}快照与日K成交额超出可接受舍入差")
        result.append({"symbol":symbol,"close":row.close,"amount_rounding_difference":float(bar["turnover"])-float(row.amount)})
    (OUT / "snapshot_sample_validation.json").write_text(
        json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8",
    )
    print(f"A股快照与未复权日K抽样一致：{len(result)}只", flush=True)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest().upper()


def apply() -> None:
    summaries = {
        "index":json.loads((OUT / "index_stage_summary.json").read_text(encoding="utf-8")),
        "a":json.loads((OUT / "a_stock_summary.json").read_text(encoding="utf-8")),
        "hk":json.loads((OUT / "hk_stock_summary.json").read_text(encoding="utf-8")),
    }
    if summaries["hk"]["failures"] or summaries["a"]["staged"] == 0:
        raise RuntimeError("必要股票暂存不完整")
    if not (OUT / "snapshot_sample_validation.json").exists():
        raise RuntimeError("A股快照与日K抽样尚未验证")
    connection = duckdb.connect(str(DB), read_only=True)
    sources = {code:(market,source,symbol) for code,market,source,symbol in source_rows(connection)}
    staged_index = connection.execute("SELECT * FROM read_parquet(?)",[str(OUT / "index_stage.parquet")]).df()
    staged_a = connection.execute("SELECT * FROM read_parquet(?)",[str(OUT / "a_stock_stage.parquet")]).df()
    staged_hk = connection.execute("SELECT * FROM read_parquet(?)",[str(OUT / "hk_stock_stage.parquet")]).df()
    for frame in (staged_index,staged_a,staged_hk):
        frame["date"] = pd.to_datetime(frame["date"]).dt.date
    if staged_index.duplicated(["index_code","date"]).any():
        raise RuntimeError("指数暂存重复业务键")
    for frame in (staged_a,staged_hk):
        if frame.duplicated(["market","stock_code","date"]).any():
            raise RuntimeError("股票暂存重复业务键")
    for row in staged_index.itertuples(index=False):
        expected = sources.get(row.index_code)
        if expected is None or row.source != expected[1] or row.source_symbol != expected[2]:
            raise RuntimeError(f"{row.index_code}暂存来源与锁定值不一致")
        expected_days = {TARGET.date()} if expected[0] == "Ashare" else {HK_EXTRA.date(),TARGET.date()}
        if row.date not in expected_days:
            raise RuntimeError(f"{row.index_code}暂存日期不属于当前交易日缺口")
    if staged_index[staged_index.index_code == "000300"]["date"].tolist() != [TARGET.date()]:
        raise RuntimeError("沪深300基准未优先取得9月28日日线")
    if set(staged_a["date"]) != {TARGET.date()} or set(staged_hk["date"]) != {HK_EXTRA.date(),TARGET.date()}:
        raise RuntimeError("股票暂存日期不符")
    for frame in (staged_index,staged_a,staged_hk):
        if frame[["open","high","low","close"]].isna().any().any():
            raise RuntimeError("暂存价格存在空值")
    if connection.execute("SELECT COUNT(*) FROM index_daily WHERE index_code='000300' AND date=DATE '2026-09-28'").fetchone()[0]:
        raise RuntimeError("沪深300已存在9月28日，拒绝重复写入")
    connection.close()
    backup_dir = ROOT / "data" / "backups"
    backup_dir.mkdir(exist_ok=True)
    backup = backup_dir / f"market_before_update_20260928_{datetime.now():%Y%m%d_%H%M%S}.duckdb"
    old_hash = file_sha256(DB)
    shutil.copy2(DB, backup)
    if file_sha256(backup) != old_hash:
        raise RuntimeError("备份SHA256不一致")
    print(f"正式库备份 {backup} SHA256 {old_hash}", flush=True)
    connection = duckdb.connect(str(DB))
    committed = False
    try:
        connection.execute(f"ATTACH '{backup.as_posix()}' AS baseline (READ_ONLY)")
        connection.execute("BEGIN TRANSACTION")
        # 先写沪深300，再写其他指数与必要股票，最后计算新增温度。
        for frame,table,columns in (
            (staged_index[staged_index.index_code=="000300"],"index_daily",INDEX_FIELDS),
            (staged_index[staged_index.index_code!="000300"],"index_daily",INDEX_FIELDS),
            (staged_a,"stock_daily",STOCK_FIELDS),
            (staged_hk,"stock_daily",STOCK_FIELDS),
        ):
            connection.register("incoming_rows",frame[columns])
            try:
                connection.execute(f"INSERT INTO {table} ({','.join(columns)}) SELECT {','.join(columns)} FROM incoming_rows")
            finally:
                connection.unregister("incoming_rows")
        for code,group in staged_index[staged_index.index_code!="000300"].groupby("index_code"):
            latest = max(group["date"])
            connection.execute("UPDATE index_registry SET last_data_date=? WHERE index_code=?",[latest,code])
        for code in ("000001","000688","000300"):
            if staged_index[(staged_index.index_code==code)&(staged_index.date==TARGET.date())].empty:
                continue
            transition = connection.execute(
                "SELECT old_source,new_source FROM data_source_transition "
                "WHERE entity_type='index_daily' AND canonical_code=? ORDER BY source_transition_date DESC LIMIT 1",[code]
            ).fetchone()
            if transition is None:
                raise RuntimeError(f"{code}缺少已批准来源迁移记录")
            day_number = connection.execute(
                "SELECT COALESCE(MAX(transition_day_number),0)+1 FROM source_verification WHERE index_code=?",[code]
            ).fetchone()[0]
            connection.execute(
                "INSERT INTO source_verification "
                "(index_code,date,transition_day_number,status,old_source,new_source,checked_at,note) "
                "VALUES (?,DATE '2026-09-28',?,'pending',?,?,NULL,?)",
                [code,day_number,transition[0],transition[1],"同花顺9月28日日线已写库；待原主源发布后后台逐字段核验"],
            )
        models.CONSTITUENTS = ROOT / "data" / "index_update_20260922" / "model_constituents"
        models.validate_fast_correlation()
        models.frozen_v10.average_pairwise_correlation = models.fast_average_pairwise_correlation
        benchmark = models.load_index(connection,"000300")
        model_results = {}
        for code in sorted(set(staged_index["index_code"])-{"000300"}):
            model_results[code] = models.write_new_index(connection,code,benchmark)
            print(f"{code}：新增指标 {model_results[code].get('inserted_indicator_rows')} 行", flush=True)
        for table,key in (("index_daily",["index_code","date"]),
                          ("stock_daily",["market","stock_code","date"]),
                          ("indicator_daily",["index_code","date","formula_version"])):
            assert_no_old_changes(connection,"baseline",table,key)
            duplicate = connection.execute(
                f"SELECT COUNT(*) FROM (SELECT {','.join(key)},COUNT(*) n FROM {table} "
                f"GROUP BY {','.join(key)} HAVING n>1)"
            ).fetchone()[0]
            if duplicate:
                raise RuntimeError(f"{table}发现重复业务键")
        if connection.execute("SELECT COUNT(*) FROM index_daily WHERE index_code='000300' AND date=DATE '2026-09-28'").fetchone()[0] != 1:
            raise RuntimeError("沪深300基准未正确写入")
        connection.execute("COMMIT")
        committed = True
        result = {"backup":str(backup),"backup_sha256":old_hash,
                  "index_rows":len(staged_index),"a_stock_rows":len(staged_a),"hk_stock_rows":len(staged_hk),
                  "index_failures":summaries["index"]["failures"],"models":model_results}
        (OUT / "apply_summary.json").write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
        print("提交完成：旧行情、旧指标逐值不变，无重复业务键", flush=True)
    finally:
        if not committed:
            connection.execute("ROLLBACK")
        connection.close()


def apply_remaining_indices() -> None:
    connection = duckdb.connect(str(DB),read_only=True)
    sources = {code:(market,source,symbol) for code,market,source,symbol in source_rows(connection)}
    staged = connection.execute("SELECT * FROM read_parquet(?)",[str(OUT / "index_stage.parquet")]).df()
    staged["date"] = pd.to_datetime(staged["date"]).dt.date
    existing = connection.execute(
        "SELECT index_code,date FROM index_daily WHERE date BETWEEN DATE '2026-09-25' AND DATE '2026-09-28'"
    ).df()
    existing["date"] = pd.to_datetime(existing["date"]).dt.date
    incoming = staged.merge(existing.assign(existing=True),on=["index_code","date"],how="left")
    incoming = incoming[incoming["existing"].isna()][INDEX_FIELDS]
    if incoming.empty:
        print("指数缺口暂无新增可写数据",flush=True)
        connection.close()
        return
    if incoming.duplicated(["index_code","date"]).any():
        raise RuntimeError("暂存指数重复业务键")
    for row in incoming.itertuples(index=False):
        locked = sources.get(row.index_code)
        if locked is None or row.source != locked[1] or row.source_symbol != locked[2] or row.date != TARGET.date():
            raise RuntimeError(f"{row.index_code}来源锁或交易日不符")
    if incoming[["open","high","low","close"]].isna().any().any():
        raise RuntimeError("暂存指数OHLC存在空值")
    if connection.execute("SELECT COUNT(*) FROM index_daily WHERE index_code='000300' AND date=DATE '2026-09-28'").fetchone()[0] != 1:
        raise RuntimeError("9月28日沪深300基准缺失")
    connection.close()
    backup_dir = ROOT / "data" / "backups"
    backup_dir.mkdir(exist_ok=True)
    backup = backup_dir / f"market_before_update_20260928_remaining_{datetime.now():%Y%m%d_%H%M%S}.duckdb"
    old_hash = file_sha256(DB)
    shutil.copy2(DB,backup)
    if file_sha256(backup) != old_hash:
        raise RuntimeError("正式库备份SHA256不一致")
    print(f"正式库备份 {backup} SHA256 {old_hash}",flush=True)
    connection = duckdb.connect(str(DB))
    committed = False
    try:
        connection.execute(f"ATTACH '{backup.as_posix()}' AS baseline (READ_ONLY)")
        connection.execute("BEGIN TRANSACTION")
        connection.register("incoming_index",incoming)
        try:
            connection.execute(
                f"INSERT INTO index_daily ({','.join(INDEX_FIELDS)}) "
                f"SELECT {','.join(INDEX_FIELDS)} FROM incoming_index"
            )
        finally:
            connection.unregister("incoming_index")
        models.CONSTITUENTS = ROOT / "data" / "index_update_20260922" / "model_constituents"
        models.validate_fast_correlation()
        models.frozen_v10.average_pairwise_correlation = models.fast_average_pairwise_correlation
        benchmark = models.load_index(connection,"000300")
        models_written = {}
        for code in sorted(set(incoming["index_code"])):
            connection.execute("UPDATE index_registry SET last_data_date=DATE '2026-09-28' WHERE index_code=?",[code])
            models_written[code] = models.write_new_index(connection,code,benchmark)
            print(f"{code}：补入指数1行，新增指标{models_written[code].get('inserted_indicator_rows')}行",flush=True)
        for table,key in (("index_daily",["index_code","date"]),
                          ("stock_daily",["market","stock_code","date"]),
                          ("indicator_daily",["index_code","date","formula_version"])):
            assert_no_old_changes(connection,"baseline",table,key)
            duplicate = connection.execute(
                f"SELECT COUNT(*) FROM (SELECT {','.join(key)},COUNT(*) n FROM {table} "
                f"GROUP BY {','.join(key)} HAVING n>1)"
            ).fetchone()[0]
            if duplicate:
                raise RuntimeError(f"{table}存在重复业务键")
        connection.execute("COMMIT")
        committed = True
        result = {"backup":str(backup),"backup_sha256":old_hash,"index_codes":sorted(set(incoming["index_code"])),
                  "index_rows":len(incoming),"models":models_written}
        (OUT / "remaining_apply_summary.json").write_text(
            json.dumps(result,ensure_ascii=False,indent=2,default=str),encoding="utf-8",
        )
        print("补缺提交完成，原行情与冻结指标逐值不变",flush=True)
    finally:
        if not committed:
            connection.execute("ROLLBACK")
        connection.close()


def verify_migrated_sources() -> None:
    connection = duckdb.connect(str(DB),read_only=True)
    pending = connection.execute(
        "SELECT index_code,date,status,transition_day_number FROM source_verification "
        "WHERE date=DATE '2026-09-28' AND index_code IN ('000001','000688','000300') "
        "AND status IN ('pending','pending_old_source') ORDER BY index_code"
    ).fetchall()
    if not pending:
        print("9月28日迁移源无待核验记录",flush=True)
        connection.close()
        return
    staged = []
    for code,day,status,day_number in pending:
        transition = connection.execute(
            "SELECT old_source,new_source,old_source_symbol,new_source_symbol "
            "FROM data_source_transition WHERE entity_type='index_daily' AND canonical_code=? "
            "ORDER BY source_transition_date DESC LIMIT 1",[code]
        ).fetchone()
        formal = connection.execute(
            "SELECT open,high,low,close,volume,amount,source,source_symbol "
            "FROM index_daily WHERE index_code=? AND date=?",[code,day]
        ).fetchone()
        if transition is None or formal is None or (formal[6],formal[7]) != (transition[1],transition[3]):
            raise RuntimeError(f"{code}正式同花顺值与迁移锁不一致")
        old_raw = ak.stock_zh_index_hist_csindex(
            symbol=code,start_date=day.strftime("%Y%m%d"),end_date=day.strftime("%Y%m%d")
        )
        if old_raw.empty:
            print(f"{code}原主源尚未提供{day}，保持待核验",flush=True)
            continue
        old_raw["日期"] = pd.to_datetime(old_raw["日期"]).dt.date
        row = old_raw[old_raw["日期"]==day]
        if len(row) != 1:
            raise RuntimeError(f"{code}原主源当日记录不是唯一一条")
        row = row.iloc[0]
        old = {"open":row["开盘"],"high":row["最高"],"low":row["最低"],"close":row["收盘"],
               "volume":row["成交量"],"amount":float(row["成交金额"])*100_000_000}
        new = dict(zip(("open","high","low","close","volume","amount"),formal[:6]))
        comparison = {}
        for field in old:
            old_value = float(old[field]) if pd.notna(old[field]) else None
            new_value = float(new[field]) if new[field] is not None else None
            if old_value is None or new_value is None or old_value == 0:
                raise RuntimeError(f"{code} {field}原源或正式值无法比较")
            difference = old_value-new_value
            relative_pct = abs(difference)/abs(old_value)*100
            threshold = 0.001 if field in ("open","high","low","close") else 0.1
            comparison[field] = {"old":old_value,"hithink":new_value,"difference":difference,
                                 "relative_difference_pct":relative_pct,"material":relative_pct>threshold}
        staged.append({"index_code":code,"date":str(day),"transition_day_number":day_number,
                       "old_source":transition[0],"new_source":transition[1],"comparison":comparison})
        print(f"{code}原主源当日日线已取，非零差异字段{sum(v['difference']!=0 for v in comparison.values())}个",flush=True)
    connection.close()
    if not staged:
        return
    OUT.mkdir(exist_ok=True)
    (OUT / "source_verification_stage_20260928.json").write_text(
        json.dumps(staged,ensure_ascii=False,indent=2),encoding="utf-8",
    )
    backup_dir = ROOT / "data" / "backups"
    backup_dir.mkdir(exist_ok=True)
    backup = backup_dir / f"market_before_source_verification_20260928_{datetime.now():%Y%m%d_%H%M%S}.duckdb"
    old_hash = file_sha256(DB)
    shutil.copy2(DB,backup)
    if file_sha256(backup) != old_hash:
        raise RuntimeError("来源核验备份SHA256不一致")
    print(f"来源核验前备份 {backup} SHA256 {old_hash}",flush=True)
    connection = duckdb.connect(str(DB))
    committed = False
    report = {"backup":str(backup),"backup_sha256":old_hash,"verified":0,"material_conflicts":0,
              "conflict_fields":0,"by_index":{}}
    try:
        connection.execute(f"ATTACH '{backup.as_posix()}' AS baseline (READ_ONLY)")
        connection.execute("BEGIN TRANSACTION")
        for item in staged:
            code,day = item["index_code"],item["date"]
            status = connection.execute(
                "SELECT status FROM source_verification WHERE index_code=? AND date=?",[code,day]
            ).fetchone()
            if status is None or status[0] not in ("pending","pending_old_source"):
                raise RuntimeError(f"{code} {day}不再是待核验状态")
            differences = {}
            for field,values in item["comparison"].items():
                if values["difference"] == 0:
                    continue
                note = "超出迁移验证容差" if values["material"] else "统一单位后微小舍入差，处于迁移验证容差内"
                connection.execute(
                    "INSERT INTO source_conflict "
                    "(index_code,date,field_name,old_value,new_value,difference,detected_at,note) "
                    "VALUES (?,?,?,?,?,?,?,?)",
                    [code,day,field,values["old"],values["hithink"],values["difference"],datetime.now(),note],
                )
                differences[field] = {"relative_difference_pct":values["relative_difference_pct"],
                                      "material":values["material"]}
                report["conflict_fields"] += 1
                report["material_conflicts"] += int(values["material"])
            result = "source_conflict" if any(v["material"] for v in item["comparison"].values()) else "verified"
            note = json.dumps({"thresholds":{"ohlc_relative_pct":0.001,"volume_amount_relative_pct":0.1},
                               "nonzero_differences":differences},ensure_ascii=False)
            connection.execute(
                "UPDATE source_verification SET status=?,checked_at=?,note=? WHERE index_code=? AND date=?",
                [result,datetime.now(),note,code,day],
            )
            report["verified"] += int(result == "verified")
            report["by_index"][code] = {"status":result,"transition_day_number":item["transition_day_number"],
                                        "nonzero_difference_fields":list(differences)}
        for table,key in (("index_daily",["index_code","date"]),
                          ("stock_daily",["market","stock_code","date"]),
                          ("indicator_daily",["index_code","date","formula_version"])):
            assert_no_old_changes(connection,"baseline",table,key)
        duplicate = connection.execute(
            "SELECT COUNT(*) FROM (SELECT index_code,date,field_name,COUNT(*) n FROM source_conflict "
            "GROUP BY 1,2,3 HAVING n>1)"
        ).fetchone()[0]
        if duplicate:
            raise RuntimeError("来源差异审计出现重复业务键")
        connection.execute("COMMIT")
        committed = True
        (OUT / "source_verification_summary_20260928.json").write_text(
            json.dumps(report,ensure_ascii=False,indent=2),encoding="utf-8",
        )
        print(f"来源核验已提交：verified={report['verified']}，差异字段={report['conflict_fields']}，重大差异={report['material_conflicts']}",flush=True)
    finally:
        if not committed:
            connection.execute("ROLLBACK")
        connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["dump", "indices", "a-stocks", "hk-stocks", "verify-snapshot", "apply", "apply-remaining", "verify-migrations"])
    args = parser.parse_args()
    if args.mode == "dump":
        get_dump()
    elif args.mode == "indices":
        stage_indices()
    elif args.mode == "a-stocks":
        stage_a_stocks()
    elif args.mode == "hk-stocks":
        stage_hk_stocks()
    elif args.mode == "verify-snapshot":
        verify_snapshot_sample()
    elif args.mode == "apply":
        apply()
    elif args.mode == "apply-remaining":
        apply_remaining_indices()
    elif args.mode == "verify-migrations":
        verify_migrated_sources()
