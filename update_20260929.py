from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

import duckdb
import pandas as pd

import phase2_compute_models as models
from phase2_fetch_stock_history import fetch_one
import update_20260928 as previous
from update_missing_20260924 import INDEX_FIELDS, STOCK_FIELDS, assert_no_old_changes, write_parquet


ROOT = Path(__file__).resolve().parent
DB = ROOT / "data" / "market.duckdb"
OUT = ROOT / "data" / "update_20260929"
TARGET = pd.Timestamp("2026-09-29")


def set_previous_context() -> None:
    previous.OUT = OUT
    previous.TARGET = TARGET


def stage_indices() -> None:
    set_previous_context()
    previous.stage_indices()
    connection = duckdb.connect(str(DB),read_only=True)
    if connection.execute("SELECT 1 FROM index_daily WHERE index_code='399006' AND date=DATE '2026-09-28'").fetchone():
        connection.close()
        return
    stage_path = OUT / "index_stage.parquet"
    existing = connection.execute("SELECT * FROM read_parquet(?)",[str(stage_path)]).df()
    existing["date"] = pd.to_datetime(existing["date"]).dt.date
    if not existing[(existing.index_code=="399006") & (existing.date==pd.Timestamp("2026-09-28").date())].empty:
        connection.close()
        return
    locked = connection.execute("SELECT primary_source,source_symbol FROM index_registry WHERE index_code='399006'").fetchone()
    prior = connection.execute("SELECT close FROM index_daily WHERE index_code='399006' AND date=DATE '2026-09-24'").fetchone()
    if locked is None or prior is None:
        raise RuntimeError("399006缺少锁定源或9月24日前收盘")
    scale = previous.daily_scale(connection,"index_daily","index_code=?",["399006"])
    connection.close()
    frame = previous.fetch_index("399006",locked[0],locked[1],[pd.Timestamp("2026-09-28").date()],prior[0],scale)
    if set(frame["date"]) != {pd.Timestamp("2026-09-28").date()}:
        raise RuntimeError("399006锁定国证源仍未返回9月28日日线")
    combined = pd.concat([existing,frame],ignore_index=True)
    if combined.duplicated(["index_code","date"]).any():
        raise RuntimeError("指数暂存重复业务键")
    write_parquet(combined[INDEX_FIELDS],stage_path)
    print("399006：9月28日遗留缺口已暂存",flush=True)


def stage_a_stocks() -> None:
    OUT.mkdir(exist_ok=True)
    connection = duckdb.connect(str(DB),read_only=True)
    members = connection.execute(
        "WITH latest AS (SELECT index_code,MAX(effective_date) effective_date FROM index_constituents GROUP BY 1) "
        "SELECT DISTINCT c.market,c.stock_code FROM index_constituents c "
        "JOIN latest l USING(index_code,effective_date) JOIN index_registry r USING(index_code) "
        "WHERE r.status IN ('active','temperature_pending') AND c.market IN ('SH','SZ','BJ')"
    ).df()
    prior = connection.execute(
        "SELECT s.market,s.stock_code,s.close prior_close,s.source,p.close prior2_close,s.pct_change prior_change "
        "FROM stock_daily s LEFT JOIN stock_daily p ON p.market=s.market AND p.stock_code=s.stock_code "
        "AND p.date=DATE '2026-09-24' WHERE s.date=DATE '2026-09-28'"
    ).df()
    wanted = members.merge(prior,on=["market","stock_code"])
    wanted = wanted[wanted["source"].str.startswith("同花顺金融数据服务",na=False)]
    already = connection.execute("SELECT market,stock_code FROM stock_daily WHERE date=DATE '2026-09-29'").df()
    wanted = wanted[~wanted.set_index(["market","stock_code"]).index.isin(already.set_index(["market","stock_code"]).index)]
    connection.close()
    if wanted.empty:
        print("9月29日A股成分无缺口",flush=True)
        return
    snapshots,total,offset = [],None,0
    while total is None or offset < total:
        url = "https://fuyao.aicubes.cn/api/a-share/prices/snapshot?"+urlencode({"limit":1000,"offset":offset})
        page = previous.api_json(url)
        timestamp = page.get("timestamp")
        if timestamp is None:
            raise RuntimeError("同花顺快照缺少上游时间")
        local = pd.to_datetime(timestamp,unit="ms",utc=True).tz_convert("Asia/Shanghai")
        if local.date()!=TARGET.date() or local.hour<15:
            raise RuntimeError(f"同花顺快照不是9月29日收市后数据：{local}")
        if total is None:
            total = page["total"]
        elif total != page["total"]:
            raise RuntimeError("快照分页期间总数变化")
        items = page.get("item") or []
        if not items:
            raise RuntimeError(f"快照分页{offset}为空")
        snapshots.extend(items)
        offset += len(items)
        print(f"A股快照 {offset}/{total}",flush=True)
        time.sleep(.35)
    snapshot = pd.DataFrame(snapshots)
    if snapshot.duplicated("thscode").any():
        raise RuntimeError("快照重复证券代码")
    wanted["thscode"] = wanted["stock_code"]+"."+wanted["market"]
    data = wanted.merge(snapshot,on="thscode",how="left",indicator=True)
    if (data["_merge"]!="both").any():
        raise RuntimeError(f"快照缺少{(data['_merge']!='both').sum()}只锁定同花顺股票")
    for field in ("last_price","open_price","high_price","low_price","volume","turnover"):
        data[field] = pd.to_numeric(data[field],errors="coerce")
    data = data[(data["volume"]>0)&data[["open_price","high_price","low_price","last_price"]].notna().all(axis=1)]
    if data.empty:
        raise RuntimeError("快照无有效当日成交")
    ratios = data["prior_change"]/(data["prior_close"]/data["prior2_close"]-1)
    scale = pd.Series(1.0,index=data.index)
    scale.loc[(ratios-100).abs()<2] = 100.0
    invalid = ratios.notna() & (ratios.abs()>.02) & ((ratios-1).abs()>=.02) & ((ratios-100).abs()>=2)
    if invalid.any():
        raise RuntimeError(f"{invalid.sum()}只股票历史涨跌幅单位无法确认")
    incoming = pd.DataFrame({
        "market":data["market"],"stock_code":data["stock_code"],"date":TARGET.date(),
        "open":data["open_price"],"high":data["high_price"],"low":data["low_price"],
        "close":data["last_price"],"pct_change":(data["last_price"]/data["prior_close"]-1)*scale,
        "volume":data["volume"],"amount":data["turnover"],
        "source":"同花顺金融数据服务 REST A股行情快照（未复权）","fetched_at":datetime.now(),
    })
    if incoming.duplicated(["market","stock_code","date"]).any():
        raise RuntimeError("A股暂存重复业务键")
    write_parquet(incoming[STOCK_FIELDS],OUT/"a_stock_stage.parquet")
    (OUT/"a_stock_summary.json").write_text(
        json.dumps({"wanted":len(wanted),"staged":len(incoming),"no_trade_or_null":len(wanted)-len(incoming),
                    "snapshot_total":total},ensure_ascii=False,indent=2),encoding="utf-8",
    )
    print(f"A股成分暂存{len(incoming)}行",flush=True)


def stage_hk_stocks() -> None:
    OUT.mkdir(exist_ok=True)
    connection = duckdb.connect(str(DB),read_only=True)
    rows = connection.execute(
        "WITH latest AS (SELECT index_code,MAX(effective_date) effective_date FROM index_constituents GROUP BY 1), "
        "members AS (SELECT DISTINCT c.market,c.stock_code FROM index_constituents c "
        "JOIN latest l USING(index_code,effective_date) JOIN index_registry r USING(index_code) "
        "WHERE r.status IN ('active','temperature_pending') AND c.market='HK') "
        "SELECT m.market,m.stock_code,s.close FROM members m JOIN stock_daily s USING(market,stock_code) "
        "WHERE s.date=DATE '2026-09-28' AND s.source='腾讯财经区间日K' "
        "AND NOT EXISTS (SELECT 1 FROM stock_daily n WHERE n.market=m.market AND n.stock_code=m.stock_code "
        "AND n.date=DATE '2026-09-29') ORDER BY m.stock_code"
    ).fetchall()
    connection.close()
    frames,failures = [],{}
    with ThreadPoolExecutor(max_workers=4) as pool:
        jobs = {pool.submit(fetch_one,market,code,TARGET,TARGET,prior):(market,code) for market,code,prior in rows}
        for future in as_completed(jobs):
            market,code = jobs[future]
            try:
                frame = future.result()
                if set(frame["date"].dt.date)!={TARGET.date()}:
                    raise RuntimeError("原源缺少9月29日日线")
                frames.append(frame)
            except Exception as exc:
                failures[f"{market}.{code}"] = f"{type(exc).__name__}: {exc}"
    if frames:
        incoming = pd.concat(frames,ignore_index=True)
        if incoming.duplicated(["market","stock_code","date"]).any():
            raise RuntimeError("港股暂存重复业务键")
        write_parquet(incoming[STOCK_FIELDS],OUT/"hk_stock_stage.parquet")
    (OUT/"hk_stock_summary.json").write_text(
        json.dumps({"needed":len(rows),"staged":sum(map(len,frames)),"failures":failures},ensure_ascii=False,indent=2),
        encoding="utf-8",
    )
    print(f"港股成分暂存{sum(map(len,frames))}行，失败{len(failures)}只",flush=True)


def verify_snapshot_sample() -> None:
    connection = duckdb.connect()
    staged = connection.execute("SELECT * FROM read_parquet(?)",[str(OUT/"a_stock_stage.parquet")]).df()
    connection.close()
    begin = int(pd.Timestamp("2026-09-29",tz="Asia/Shanghai").timestamp()*1000)
    finish = int(pd.Timestamp("2026-09-30",tz="Asia/Shanghai").timestamp()*1000)
    result = []
    for row in staged.groupby("market",sort=True).head(1).itertuples(index=False):
        symbol = f"{row.stock_code}.{row.market}"
        query = urlencode({"thscode":symbol,"interval":"1d","adjust":"none","start":begin,"end":finish})
        bars = previous.api_json("https://fuyao.aicubes.cn/api/a-share/prices/historical?"+query)["item"]
        if len(bars)!=1:
            raise RuntimeError(f"{symbol}未复权日K尚未发布")
        bar = bars[0]
        for field,value in (("open_price",row.open),("high_price",row.high),("low_price",row.low),
                            ("close_price",row.close),("volume",row.volume)):
            if abs(float(bar[field])-float(value))>1e-8:
                raise RuntimeError(f"{symbol}快照与未复权日K的{field}不一致")
        difference = float(bar["turnover"])-float(row.amount)
        if abs(difference)>max(10.0,abs(float(bar["turnover"]))*1e-7):
            raise RuntimeError(f"{symbol}成交额超出舍入容差")
        result.append({"symbol":symbol,"amount_rounding_difference":difference})
    (OUT/"snapshot_sample_validation.json").write_text(
        json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8",
    )
    print(f"A股快照与日K抽样一致：{len(result)}只",flush=True)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda:source.read(8*1024*1024),b""):
            digest.update(block)
    return digest.hexdigest().upper()


def apply() -> None:
    if not (OUT/"snapshot_sample_validation.json").exists():
        raise RuntimeError("快照尚未与未复权日K抽样核对")
    hk_summary = json.loads((OUT/"hk_stock_summary.json").read_text(encoding="utf-8"))
    if hk_summary["failures"]:
        raise RuntimeError("港股必要成分存在失败，暂不写库")
    connection = duckdb.connect(str(DB),read_only=True)
    sources = {code:(market,source,symbol) for code,market,source,symbol in previous.source_rows(connection)}
    index = connection.execute("SELECT * FROM read_parquet(?)",[str(OUT/"index_stage.parquet")]).df()
    a_stock = connection.execute("SELECT * FROM read_parquet(?)",[str(OUT/"a_stock_stage.parquet")]).df()
    hk_stock = connection.execute("SELECT * FROM read_parquet(?)",[str(OUT/"hk_stock_stage.parquet")]).df()
    for frame in (index,a_stock,hk_stock):
        frame["date"] = pd.to_datetime(frame["date"]).dt.date
        if frame[["open","high","low","close"]].isna().any().any():
            raise RuntimeError("暂存OHLC存在空值")
    if index.duplicated(["index_code","date"]).any():
        raise RuntimeError("指数暂存重复业务键")
    for frame in (a_stock,hk_stock):
        if frame.duplicated(["market","stock_code","date"]).any():
            raise RuntimeError("股票暂存重复业务键")
    for row in index.itertuples(index=False):
        locked = sources.get(row.index_code)
        if locked is None or (row.source,row.source_symbol)!=(locked[1],locked[2]):
            raise RuntimeError(f"{row.index_code}暂存来源与正式锁不符")
        if row.date!=TARGET.date() and not (row.index_code=="399006" and row.date==pd.Timestamp("2026-09-28").date()):
            raise RuntimeError(f"{row.index_code}暂存日期越界")
    if index[(index.index_code=="000300")&(index.date==TARGET.date())].shape[0]!=1:
        raise RuntimeError("9月29日沪深300基准未取得")
    if set(a_stock["date"])!={TARGET.date()} or set(hk_stock["date"])!={TARGET.date()}:
        raise RuntimeError("必要股票日期不符")
    existing_index = connection.execute(
        "SELECT index_code,date FROM index_daily WHERE date BETWEEN DATE '2026-09-28' AND DATE '2026-09-29'"
    ).df()
    existing_index["date"] = pd.to_datetime(existing_index["date"]).dt.date
    if not index.merge(existing_index,on=["index_code","date"]).empty:
        raise RuntimeError("暂存指数包含正式库已有业务键")
    existing_stock = connection.execute("SELECT market,stock_code,date FROM stock_daily WHERE date=DATE '2026-09-29'").df()
    existing_stock["date"] = pd.to_datetime(existing_stock["date"]).dt.date
    if not a_stock.merge(existing_stock,on=["market","stock_code","date"]).empty or not hk_stock.merge(existing_stock,on=["market","stock_code","date"]).empty:
        raise RuntimeError("暂存股票包含正式库已有业务键")
    connection.close()
    backup_dir = ROOT/"data"/"backups"
    backup_dir.mkdir(exist_ok=True)
    backup = backup_dir/f"market_before_update_20260929_{datetime.now():%Y%m%d_%H%M%S}.duckdb"
    before_hash = sha256(DB)
    shutil.copy2(DB,backup)
    if sha256(backup)!=before_hash:
        raise RuntimeError("备份SHA256不一致")
    print(f"正式库备份 {backup} SHA256 {before_hash}",flush=True)
    connection = duckdb.connect(str(DB))
    committed = False
    try:
        connection.execute(f"ATTACH '{backup.as_posix()}' AS baseline (READ_ONLY)")
        connection.execute("BEGIN TRANSACTION")
        for frame,table,fields in (
            (index[index.index_code=="000300"],"index_daily",INDEX_FIELDS),
            (index[index.index_code!="000300"],"index_daily",INDEX_FIELDS),
            (a_stock,"stock_daily",STOCK_FIELDS),(hk_stock,"stock_daily",STOCK_FIELDS),
        ):
            connection.register("incoming_rows",frame[fields])
            try:
                connection.execute(f"INSERT INTO {table} ({','.join(fields)}) SELECT {','.join(fields)} FROM incoming_rows")
            finally:
                connection.unregister("incoming_rows")
        for code,group in index[index.index_code!="000300"].groupby("index_code"):
            connection.execute("UPDATE index_registry SET last_data_date=? WHERE index_code=?",[max(group["date"]),code])
        for code in ("000001","000688","000300"):
            if index[(index.index_code==code)&(index.date==TARGET.date())].empty:
                continue
            transition = connection.execute(
                "SELECT old_source,new_source FROM data_source_transition WHERE entity_type='index_daily' "
                "AND canonical_code=? ORDER BY source_transition_date DESC LIMIT 1",[code]
            ).fetchone()
            if transition is None:
                raise RuntimeError(f"{code}缺少迁移审计")
            number = connection.execute(
                "SELECT COALESCE(MAX(transition_day_number),0)+1 FROM source_verification WHERE index_code=?",[code]
            ).fetchone()[0]
            connection.execute(
                "INSERT INTO source_verification "
                "(index_code,date,transition_day_number,status,old_source,new_source,checked_at,note) "
                "VALUES (?,DATE '2026-09-29',?,'pending',?,?,NULL,?)",
                [code,number,transition[0],transition[1],"同花顺9月29日日线已写库；待原主源发布后后台核验"],
            )
        models.CONSTITUENTS = ROOT/"data"/"index_update_20260922"/"model_constituents"
        models.validate_fast_correlation()
        models.frozen_v10.average_pairwise_correlation = models.fast_average_pairwise_correlation
        benchmark = models.load_index(connection,"000300")
        model_results = {}
        for code in sorted(set(index["index_code"])-{"000300"}):
            model_results[code] = models.write_new_index(connection,code,benchmark)
            print(f"{code}：新增指标{model_results[code].get('inserted_indicator_rows')}行",flush=True)
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
        result = {"backup":str(backup),"backup_sha256":before_hash,
                  "index_rows":len(index),"a_stock_rows":len(a_stock),"hk_stock_rows":len(hk_stock),
                  "index_dates":{str(day):int(count) for day,count in index.groupby("date").size().items()},
                  "models":model_results}
        (OUT/"apply_summary.json").write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str),encoding="utf-8")
        print("提交完成：旧行情和冻结指标逐值未变，无重复业务键",flush=True)
    finally:
        if not committed:
            connection.execute("ROLLBACK")
        connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mode",choices=["indices","a-stocks","hk-stocks","verify-snapshot","apply"])
    args = parser.parse_args()
    if args.mode == "indices":
        stage_indices()
    elif args.mode == "a-stocks":
        stage_a_stocks()
    elif args.mode == "hk-stocks":
        stage_hk_stocks()
    elif args.mode == "verify-snapshot":
        verify_snapshot_sample()
    elif args.mode == "apply":
        apply()
