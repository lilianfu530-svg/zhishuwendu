from __future__ import annotations

import argparse
import json
import math
from datetime import date, datetime
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parent
DATABASE = ROOT / "data" / "market.duckdb"
OUTPUT = ROOT / "output" / "index_temperature_dashboard.html"
REGISTRY_STATUSES = ("active", "temperature_pending")
VERSIONS = ("V1.0", "V1.1")
SCORES = (
    "RET20_score", "BIAS20_score", "RS20_score", "VolumeStrength_score",
    "BreadthMA20_score", "HLBreadth_score", "Sync_score",
    "PriceMomentum_score", "Volume_score", "Breadth_score", "Sync_score_v11",
)


def clean(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()[:10]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def page_group(index_code: str, primary_source: str) -> str:
    if index_code in {"000001", "000688"}:
        return "broad"
    if "同花顺" in primary_source or index_code in {"930986", "931787"}:
        return "industry"
    return "broad"


def read_dashboard_data(as_of_date: date | None = None) -> dict:
    if not DATABASE.is_file():
        raise FileNotFoundError(DATABASE)
    connection = duckdb.connect(str(DATABASE), read_only=True)
    try:
        registry_rows = connection.execute(
            """
            SELECT index_code, display_name, market, primary_source, source_symbol,
                   status, constituent_mode, last_data_date, temperature_status
            FROM index_registry
            WHERE status IN (?, ?)
            ORDER BY initialized_at, index_code
            """,
            REGISTRY_STATUSES,
        ).fetchall()
        if not registry_rows:
            raise ValueError("index_registry 中没有可展示的正式接入指数")
        updated_at = connection.execute(
            "SELECT MAX(fetched_at) FROM index_daily WHERE index_code IN ("
            "SELECT index_code FROM index_registry WHERE status IN (?, ?))",
            REGISTRY_STATUSES,
        ).fetchone()[0]
        registry_columns = (
            "code", "name", "market", "primary_source", "source_symbol",
            "status", "constituent_mode", "last_data_date", "temperature_status",
        )
        registry = [
            dict(zip(registry_columns, map(clean, row)))
            for row in registry_rows
        ]
        codes = [item["code"] for item in registry]
        fields = [
            "i.index_code", "i.date", "i.formula_version", "i.constituent_mode",
            "i.temperature", "i.temperature_change_1d", "i.temperature_change_5d",
            "i.coverage_ratio", "p.close", "p.pct_change",
            *(f"i.{field}" for field in SCORES),
        ]
        date_filter = "" if as_of_date is None else "\n                  AND i.date <= ?"
        query = f"""
            WITH numbered AS (
                SELECT {', '.join(fields)},
                       ROW_NUMBER() OVER (
                           PARTITION BY i.index_code, i.formula_version
                           ORDER BY i.date DESC
                       ) AS row_number
                FROM indicator_daily AS i
                JOIN index_daily AS p
                  ON p.index_code = i.index_code AND p.date = i.date
                WHERE i.index_code IN ({', '.join('?' for _ in codes)})
                  AND i.formula_version IN ('V1.0', 'V1.1')
                  {date_filter}
            )
            SELECT * FROM numbered WHERE row_number <= 125
            ORDER BY index_code, formula_version, date
        """
        query_params = codes if as_of_date is None else [*codes, as_of_date]
        cursor = connection.execute(query, query_params)
        columns = [column[0] for column in cursor.description]
        records = [dict(zip(columns, map(clean, row))) for row in cursor.fetchall()]
    finally:
        connection.close()

    data = {version: {code: [] for code in codes} for version in VERSIONS}
    for record in records:
        code = record.pop("index_code")
        version = record.pop("formula_version")
        record.pop("row_number")
        if record["pct_change"] is not None and code != "930986":
            record["pct_change"] = record["pct_change"] * 100
        data[version][code].append(record)

    for version in VERSIONS:
        for code in codes:
            rows = data[version][code]
            if len(rows) < 120:
                raise ValueError(f"{code} {version} 有效温度不足120个交易日")
            for offset, row in enumerate(rows):
                if row["close"] is None or row["coverage_ratio"] is None:
                    raise ValueError(f"{code} {version} {row['date']} 缺少价格或覆盖率")
                if (row["temperature"] is not None and row["temperature_change_5d"] is None
                        and offset >= 5 and rows[offset - 5]["temperature"] is not None):
                    row["temperature_change_5d"] = round(
                        row["temperature"] - rows[offset - 5]["temperature"], 1
                    )
            data[version][code] = rows[-120:]
    for item in registry:
        item["group"] = page_group(item["code"], item["primary_source"])
    return {"updated_at": updated_at.strftime("%Y-%m-%d %H:%M") if updated_at else None,
            "indices": registry,
            "versions": data}


HTML = r'''<!doctype html>
<html lang="zh-CN">
<head>
<!-- 自动发布链路验证 -->
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="light">
<title>指数温度计 · 盘后观察版</title>
<style>
:root{--ink:#182438;--muted:#66758a;--line:#e5e9ef;--paper:#f7f8fb;--white:#fff;--accent:#365ec9;--soft:#edf2ff;--up:#bf5b35;--down:#267893}
*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;background:var(--paper);color:var(--ink);font-family:"Microsoft YaHei","PingFang SC","Noto Sans CJK SC",Arial,sans-serif;font-size:14px;line-height:1.5}
button,select{font:inherit}button{cursor:pointer}button:focus-visible,select:focus-visible,.index-card:focus-visible{outline:3px solid #8aa9ff;outline-offset:2px}
.shell{max-width:1420px;margin:auto;padding:28px 32px 64px}.top{display:flex;justify-content:space-between;align-items:flex-start;gap:24px;padding:7px 0 26px;border-bottom:1px solid var(--line)}
.eyebrow{font-size:11px;font-weight:800;letter-spacing:.18em;color:var(--accent);text-transform:uppercase}.brand h1{font-size:32px;letter-spacing:-.035em;margin:4px 0 0;line-height:1.15}.subtitle{font-size:15px;color:var(--muted);margin:6px 0 0}
.top-right{display:flex;align-items:center;gap:15px;flex-wrap:wrap;justify-content:flex-end}.version-control{display:flex;align-items:center;gap:5px;background:#e9edf5;border-radius:10px;padding:4px}.version-control button{border:0;background:transparent;border-radius:7px;padding:7px 16px;color:var(--muted);font-weight:700}.version-control button.active{background:var(--white);color:var(--ink);box-shadow:0 1px 5px #1722381c}.snapshot{font-size:12px;color:var(--muted);max-width:240px;text-align:right}
.temperature-key{margin:22px 0 0;padding:15px 18px 13px;background:#fff;border:1px solid var(--line);border-radius:12px}.key-title{display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;color:var(--muted);font-size:11px}.key-title strong{color:var(--ink);font-size:12px}.key-track,.key-labels{display:grid;grid-template-columns:1fr 1fr 2fr 2fr 2fr 1fr 1fr}.key-track{height:9px;overflow:hidden;border-radius:10px}.key-labels{margin-top:6px}.key-labels span{text-align:center;color:var(--muted);font-size:11px;white-space:nowrap}.sheet-tabs{display:flex;gap:24px;margin-top:19px;border-bottom:1px solid var(--line)}.sheet-tabs button{position:relative;border:0;background:transparent;padding:11px 3px 13px;color:var(--muted);font-weight:650}.sheet-tabs button.active{color:var(--accent)}.sheet-tabs button.active:after{content:"";position:absolute;left:0;right:0;bottom:-1px;height:3px;border-radius:3px;background:var(--accent)}.sheet-tabs small{margin-left:5px;font-size:11px;font-weight:500;color:#8b97a8}
.section-head{display:flex;align-items:end;justify-content:space-between;gap:18px;margin:29px 0 16px}.section-head h2{font-size:20px;letter-spacing:-.025em;margin:0}.section-head p{margin:4px 0 0;color:var(--muted);font-size:12px}.sort{display:flex;align-items:center;gap:9px;color:var(--muted);font-size:12px}.sort select{padding:9px 30px 9px 11px;border:1px solid var(--line);border-radius:8px;background:#fff;color:var(--ink)}
.card-grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px}.index-card{min-width:0;background:#fff;border:1px solid var(--line);border-radius:13px;padding:18px 19px 16px;text-align:left;box-shadow:0 4px 18px #1c315108;transition:transform .15s,border-color .15s,box-shadow .15s}.index-card:hover{transform:translateY(-2px);border-color:#a8bbf0;box-shadow:0 9px 24px #1c315115}.card-top{display:flex;align-items:start;justify-content:space-between;gap:8px}.card-name{font-size:17px;font-weight:750;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.code{font-size:11px;color:var(--muted);letter-spacing:.035em;font-variant-numeric:tabular-nums}.card-date{font-size:11px;color:var(--muted);white-space:nowrap}.temperature{font-size:42px;font-weight:750;line-height:1;letter-spacing:-.055em;margin:21px 0 6px;font-variant-numeric:tabular-nums}.temperature small{font-size:13px;font-weight:500;color:var(--muted);letter-spacing:0;margin-left:5px}.temp-track{height:6px;border-radius:8px;background:linear-gradient(90deg,#2a7995 0%,#63a5bd 20%,#9aa5a8 40%,#cf8a6b 60%,#e05b43 80%,#c62828 90%,#8b0000 100%);position:relative;margin:15px 0 17px}.temp-track:after{content:"";position:absolute;left:var(--position);top:50%;width:10px;height:10px;border:2px solid #fff;border-radius:50%;background:var(--tone);transform:translate(-50%,-50%);box-shadow:0 1px 4px #0005;opacity:var(--marker-opacity,1)}.level{font-size:12px;font-weight:700;color:var(--tone)}.changes{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-top:9px}.changes strong{font-size:15px;font-variant-numeric:tabular-nums}.change-tag{font-size:11px;font-weight:700;border-radius:5px;padding:2px 6px;background:var(--tag-bg);color:var(--tag-color)}.card-meta{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:17px;padding-top:12px;border-top:1px solid var(--line)}.card-meta span{display:block;font-size:11px;color:var(--muted)}.card-meta b{display:block;margin-top:3px;font-size:13px;font-weight:650;font-variant-numeric:tabular-nums}
.panel{background:#fff;border:1px solid var(--line);border-radius:13px;overflow:hidden;box-shadow:0 4px 18px #1c315108}.table-wrap{overflow-x:auto}table{width:100%;border-collapse:collapse;min-width:760px}th{text-align:left;color:var(--muted);font-size:11px;font-weight:700;letter-spacing:.04em;background:#fafbfe}th,td{padding:12px 18px;border-bottom:1px solid #edf0f4;white-space:nowrap}tbody tr:last-child td{border-bottom:0}tbody tr:hover{background:#f8faff}td{font-variant-numeric:tabular-nums}.rank{color:#8390a1;font-weight:750;width:50px}.table-index{font-weight:700}.table-index small{display:block;font-weight:400;color:var(--muted);font-size:11px}.click-row{cursor:pointer}.num{font-weight:750}.positive{color:var(--up)}.negative{color:var(--down)}.muted{color:var(--muted)}
.detail{margin-top:32px;scroll-margin-top:18px}.detail[hidden]{display:none}.detail-header{display:flex;justify-content:space-between;gap:18px;align-items:start;margin-bottom:17px}.detail-title h2{margin:0;font-size:24px}.detail-title p{color:var(--muted);margin:3px 0 0}.close-btn{border:1px solid var(--line);background:#fff;color:var(--muted);border-radius:8px;padding:7px 13px}.detail-grid{display:grid;grid-template-columns:minmax(0,1fr) 310px;gap:14px}.chart-panel,.score-panel,.recent-panel{padding:19px}.chart-panel{min-width:0}.panel-title{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-bottom:13px}.panel-title h3{margin:0;font-size:15px}.segment{display:flex;background:#eef1f6;border-radius:8px;padding:3px;gap:2px}.segment button{border:0;border-radius:6px;background:transparent;color:var(--muted);padding:5px 10px;font-size:11px}.segment button.active{background:#fff;color:var(--ink);font-weight:700;box-shadow:0 1px 4px #0001}.chart-label{font-size:11px;color:var(--muted);margin:14px 0 4px}.chart-slot{position:relative;width:100%;height:230px}.chart-slot.small{height:198px}.chart-slot svg{width:100%;height:100%;display:block;cursor:crosshair}.chart-focus{pointer-events:none}.chart-tooltip{position:absolute;z-index:2;min-width:185px;padding:9px 11px;border:1px solid #d9e0ea;border-radius:8px;background:#fff;box-shadow:0 5px 18px #18243824;pointer-events:none;font-size:11px;line-height:1.55;white-space:nowrap}.chart-tooltip strong{display:block;margin-bottom:3px;font-size:12px;color:var(--ink)}.chart-tooltip span{display:block;color:var(--muted)}.chart-tooltip b{color:var(--ink);font-variant-numeric:tabular-nums}.chart-note{color:var(--muted);font-size:11px;margin:6px 0 0}.legend{display:flex;align-items:center;gap:15px;color:var(--muted);font-size:11px}.legend span:before{content:"";display:inline-block;width:13px;height:3px;border-radius:5px;background:var(--swatch);vertical-align:middle;margin-right:5px}.score-panel h3{font-size:15px;margin:0 0 3px}.score-panel .sub{font-size:11px;color:var(--muted);margin:0 0 20px}.score-row{margin:0 0 18px}.score-label{display:flex;justify-content:space-between;gap:8px;font-size:12px}.score-label b{font-variant-numeric:tabular-nums}.score-track{height:7px;background:#edf1f6;border-radius:8px;margin-top:7px;overflow:hidden}.score-fill{height:100%;background:#5678ce;border-radius:8px}.quality{border-top:1px solid var(--line);margin-top:26px;padding-top:14px;color:var(--muted);font-size:11px}.quality dl{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin:10px 0}.quality dl>div:nth-child(3){grid-column:1/-1}.quality dt{color:#8792a2}.quality dd{margin:1px 0 0;color:var(--ink);overflow-wrap:anywhere}.quality p{margin:11px 0 0}.recent-panel{margin-top:14px;padding:0}.recent-panel .panel-title{padding:17px 18px 0;margin-bottom:8px}.temp-cell{font-weight:750}.footer{border-top:1px solid var(--line);margin-top:37px;padding-top:17px;font-size:12px;color:var(--muted)}
@media(max-width:1100px){.card-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.detail-grid{grid-template-columns:1fr}.score-panel{display:grid;grid-template-columns:repeat(2,1fr);gap:0 20px}.score-panel h3,.score-panel .sub,.quality{grid-column:1/-1}}
@media(max-width:650px){.shell{padding:18px 14px 38px}.top{display:block}.top-right{justify-content:space-between;margin-top:18px}.snapshot{text-align:left}.temperature-key{padding:13px 10px}.key-labels span{font-size:10px}.section-head{align-items:start}.sort{align-items:start;flex-direction:column}.card-grid{grid-template-columns:1fr;gap:10px}.index-card{padding:15px}.temperature{margin-top:13px;font-size:38px}.chart-panel,.score-panel{padding:14px}.score-panel{display:block}.chart-slot{height:205px}.chart-slot.small{height:185px}.detail-title h2{font-size:20px}}
</style>
</head>
<body>
<main class="shell">
  <header class="top"><div class="brand"><div class="eyebrow">MARKET OBSERVATION · V1</div><h1>指数温度计</h1><p class="subtitle">盘后市场拥挤度观察</p></div><div class="top-right"><div class="version-control" aria-label="模型版本"><button type="button" data-version="V1.0">V1.0</button><button type="button" data-version="V1.1">V1.1</button></div><div class="snapshot" id="snapshot"></div></div></header>
  <div class="temperature-key" aria-label="温度等级区间"><div class="key-title"><strong>温度区间</strong><span>0—100</span></div><div class="key-track" aria-hidden="true"><span style="background:#2a7995"></span><span style="background:#4d94ad"></span><span style="background:#84a9b6"></span><span style="background:#a6aeb0"></span><span style="background:#cf8a6b"></span><span style="background:linear-gradient(90deg,#e05b43,#c62828)"></span><span style="background:linear-gradient(90deg,#c62828,#8b0000)"></span></div><div class="key-labels"><span>冰点</span><span>恐惧</span><span>偏冷</span><span>中性</span><span>偏热</span><span>贪婪</span><span>狂热</span></div></div>
  <nav class="sheet-tabs" role="tablist" aria-label="指数分类"><button type="button" id="tab-broad" role="tab" data-sheet="broad" aria-controls="sheet-content">大指数<small id="broad-count"></small></button><button type="button" id="tab-industry" role="tab" data-sheet="industry" aria-controls="sheet-content">行业指数<small id="industry-count"></small></button></nav>
  <div id="sheet-content" role="tabpanel">
  <section aria-labelledby="cards-heading"><div class="section-head"><div><h2 id="cards-heading">指数概览</h2><p>按各指数最新有效交易日展示 · 点击卡片查看详情</p></div><label class="sort">排序<select id="sort"><option value="default">默认自选顺序</option><option value="high">温度从高到低</option><option value="low">温度从低到高</option><option value="rise">5日升温最快</option><option value="fall">5日降温最快</option></select></label></div><div id="cards" class="card-grid"></div></section>
  <section aria-labelledby="rank-heading"><div class="section-head"><div><h2 id="rank-heading">今日温度排行</h2><p>按所选版本的最新温度排序，日期以各指数实际数据为准</p></div></div><div class="panel table-wrap"><table><thead><tr><th>排名</th><th>指数</th><th>数据日期</th><th>温度</th><th>等级</th><th>1日变化</th><th>5日变化</th></tr></thead><tbody id="ranking"></tbody></table></div></section>
  <section id="detail" class="detail" hidden aria-labelledby="detail-name"><div class="detail-header"><div class="detail-title"><h2 id="detail-name"></h2><p id="detail-summary"></p></div><button type="button" id="close-detail" class="close-btn">关闭详情</button></div><div class="detail-grid"><div class="panel chart-panel"><div class="panel-title"><h3>温度走势</h3><div class="segment" aria-label="走势时间范围"><button type="button" data-range="30">30日</button><button type="button" data-range="60">60日</button><button type="button" data-range="120">120日</button></div></div><div class="chart-slot" id="temperature-chart"></div><p class="chart-note">灰色虚线为温度区间参考线：20、40、60、80。移动鼠标或点击曲线可查看当天数据。</p><div class="chart-label">指数收盘价与 Temperature · 同期走势</div><div class="legend"><span style="--swatch:#365ec9">Temperature（左轴）</span><span style="--swatch:#bb7558">指数收盘价（右轴）</span></div><div class="chart-slot small" id="price-chart"></div><p class="chart-note">两条曲线按各自坐标轴显示，用于观察同期变化。</p></div><aside class="panel score-panel"><h3 id="score-title"></h3><p class="sub">当天各项 score · 0—100</p><div id="scores"></div><div class="quality"><strong>数据质量</strong><dl id="quality-fields"></dl><p id="quality-note"></p></div></aside></div><div class="panel recent-panel"><div class="panel-title"><h3>最近10个交易日</h3></div><div class="table-wrap"><table><thead><tr><th>日期</th><th>Temperature</th><th>1日变化</th><th>5日变化</th><th>温度等级</th><th>指数涨跌幅</th></tr></thead><tbody id="recent"></tbody></table></div></div></section>
  </div>
  <footer class="footer">温度反映指数在价格、动量、成交、市场广度及成分一致性等维度的历史相对状态，仅用于市场状态观察。</footer>
</main>
<script id="dashboard-data" type="application/json">__DATA__</script>
<script>
(() => {
  'use strict';
  const source = JSON.parse(document.getElementById('dashboard-data').textContent);
  const byCode = Object.fromEntries(source.indices.map((item, index) => [item.code, {...item, order: index}]));
  const state = {version: 'V1.1', sheet: 'broad', sort: 'default', code: null, range: 30};
  const scoreNames = {RET20_score:'RET20', BIAS20_score:'BIAS20', RS20_score:'RS20', VolumeStrength_score:'VolumeStrength', BreadthMA20_score:'BreadthMA20', HLBreadth_score:'HLBreadth', Sync_score:'Sync', PriceMomentum_score:'PriceMomentum', Volume_score:'Volume', Breadth_score:'Breadth', Sync_score_v11:'Sync'};
  const scoreKeys = {'V1.0':['RET20_score','BIAS20_score','RS20_score','VolumeStrength_score','BreadthMA20_score','HLBreadth_score','Sync_score'], 'V1.1':['PriceMomentum_score','Volume_score','Breadth_score','Sync_score_v11']};
  const $ = id => document.getElementById(id);
  const rows = code => source.versions[state.version][code];
  const latest = code => rows(code).at(-1);
  const number = (value, digits=1) => value == null ? '—' : Number(value).toFixed(digits);
  const signed = (value, suffix='') => value == null ? '—' : `${value > 0 ? '+' : ''}${number(value)}${suffix}`;
  const classOf = value => value == null ? 'muted' : value > 0 ? 'positive' : value < 0 ? 'negative' : 'muted';
  const arrow = value => value == null || value === 0 ? '' : value > 0 ? ' ↑' : ' ↓';
  const level = value => value == null ? '—' : value < 10 ? '冰点' : value < 20 ? '恐惧' : value < 40 ? '偏冷' : value < 60 ? '中性' : value < 80 ? '偏热' : value < 90 ? '贪婪' : '狂热';
  const temperatureLabel = (value, item) => value == null ? item?.temperature_status === 'pending' ? '待补样本' : '暂无数据' : level(value);
  const tone = value => value == null ? '#8d97a6' : value < 20 ? '#2a7995' : value < 40 ? '#658caa' : value < 60 ? '#687990' : value < 80 ? '#c05a45' : value < 90 ? '#c62828' : value < 100 ? '#b71c1c' : '#8b0000';
  const temperatureBg = value => value == null ? '#f0f2f5' : value < 10 ? '#dceff3' : value < 20 ? '#e4f2f5' : value < 40 ? '#eaf1f8' : value < 60 ? '#eef1f5' : value < 80 ? '#faebe5' : value < 90 ? '#f8d9d7' : value < 100 ? '#f3caca' : '#8b0000';
  const changeLabel = value => value == null ? '暂无变化数据' : Math.abs(value) < 3 ? '变化不大' : Math.abs(value) < 10 ? value > 0 ? '升温' : '降温' : value > 0 ? '快速升温' : '快速降温';
  const esc = value => String(value ?? '').replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
  const belongsToSheet = code => byCode[code]?.group === state.sheet;
  const itemList = () => source.indices.filter(item => belongsToSheet(item.code)).map(item => ({...item, row: latest(item.code)}));

  function renderCards() {
    const list = itemList();
    const sorts = {high:(a,b)=>(b.row.temperature??-Infinity)-(a.row.temperature??-Infinity), low:(a,b)=>(a.row.temperature??Infinity)-(b.row.temperature??Infinity), rise:(a,b)=>(b.row.temperature_change_5d??-Infinity)-(a.row.temperature_change_5d??-Infinity), fall:(a,b)=>(a.row.temperature_change_5d??Infinity)-(b.row.temperature_change_5d??Infinity)};
    if (sorts[state.sort]) list.sort((a,b) => sorts[state.sort](a,b) || byCode[a.code].order-byCode[b.code].order);
    $('cards').innerHTML = list.map(item => {
      const r=item.row, change=r.temperature_change_5d;
      const position = r.temperature == null ? 0 : r.temperature;
      const markerOpacity = r.temperature == null ? 0 : 1;
      return `<button type="button" class="index-card" data-code="${esc(item.code)}" aria-label="查看${esc(item.name)}详情"><div class="card-top"><div><div class="card-name">${esc(item.name)}</div><div class="code">${esc(item.code)}</div></div><div class="card-date">${esc(r.date)}</div></div><div class="temperature">${number(r.temperature)}<small>/ 100</small></div><div class="level" style="--tone:${tone(r.temperature)}">${temperatureLabel(r.temperature,item)}</div><div class="temp-track" style="--position:${position}%;--marker-opacity:${markerOpacity};--tone:${tone(r.temperature)}"></div><div class="changes"><strong class="${classOf(change)}">5日 ${signed(change)}${arrow(change)}</strong><span class="change-tag" style="--tag-bg:${change > 0 ? '#fff0e9' : change < 0 ? '#e7f4f7' : '#f0f2f5'};--tag-color:${change > 0 ? '#ac5535' : change < 0 ? '#267893' : '#66758a'}">${changeLabel(change)}</span></div><div class="card-meta"><div><span>1日温度变化</span><b class="${classOf(r.temperature_change_1d)}">${signed(r.temperature_change_1d)}${arrow(r.temperature_change_1d)}</b></div><div><span>指数涨跌幅</span><b class="${classOf(r.pct_change)}">${signed(r.pct_change,'%')}</b></div><div><span>成分股覆盖率</span><b>${number(r.coverage_ratio*100)}%</b></div><div><span>温度等级</span><b>${temperatureLabel(r.temperature,item)}</b></div></div></button>`;
    }).join('');
    $('cards').querySelectorAll('[data-code]').forEach(button => button.addEventListener('click', () => openDetail(button.dataset.code)));
  }

  function renderRanking() {
    const list=itemList().sort((a,b)=>(b.row.temperature??-Infinity)-(a.row.temperature??-Infinity) || byCode[a.code].order-byCode[b.code].order);
    $('ranking').innerHTML=list.map((item,index)=>{const r=item.row;return `<tr class="click-row" data-code="${esc(item.code)}" tabindex="0" aria-label="查看${esc(item.name)}详情"><td class="rank">${String(index+1).padStart(2,'0')}</td><td class="table-index">${esc(item.name)}<small>${esc(item.code)}</small></td><td>${esc(r.date)}</td><td class="num">${number(r.temperature)}</td><td>${temperatureLabel(r.temperature,item)}</td><td class="${classOf(r.temperature_change_1d)}">${signed(r.temperature_change_1d)}${arrow(r.temperature_change_1d)}</td><td class="${classOf(r.temperature_change_5d)}">${signed(r.temperature_change_5d)}${arrow(r.temperature_change_5d)}</td></tr>`}).join('');
    $('ranking').querySelectorAll('[data-code]').forEach(row => {row.addEventListener('click',()=>openDetail(row.dataset.code));row.addEventListener('keydown',event=>{if(event.key==='Enter'||event.key===' '){event.preventDefault();openDetail(row.dataset.code)}})});
  }

  function path(points) {return points.map((point,index)=>`${index?'L':'M'}${point[0].toFixed(1)},${point[1].toFixed(1)}`).join(' ')}
  function chart(records, paired) {
    const width=820, height=paired?194:225, left=39, right=paired?55:18, top=15, bottom=27;
    const plotW=width-left-right, plotH=height-top-bottom;
    const x=index=>left+index*plotW/Math.max(records.length-1,1);
    const yTemp=value=>top+(100-value)*plotH/100;
    const temperaturePoints=records.map((r,index)=>r.temperature==null?null:[x(index),yTemp(r.temperature)]).filter(Boolean);
    const grid=[20,40,60,80].map(value=>`<line x1="${left}" x2="${width-right}" y1="${yTemp(value)}" y2="${yTemp(value)}" stroke="#d8dee8" stroke-dasharray="4 5"/><text x="${left-9}" y="${yTemp(value)+4}" text-anchor="end" fill="#8995a5" font-size="10">${value}</text>`).join('');
    const ticks=[0,Math.floor((records.length-1)/2),records.length-1].map(index=>`<text x="${x(index)}" y="${height-6}" text-anchor="${index===0?'start':index===records.length-1?'end':'middle'}" fill="#8995a5" font-size="10">${records[index].date.slice(5)}</text>`).join('');
    let price='', low=0, high=0;
    if(paired){const values=records.map(r=>r.close), min=Math.min(...values), max=Math.max(...values), pad=Math.max((max-min)*.08,max*.001);low=min-pad;high=max+pad;const yPrice=value=>top+(high-value)*plotH/(high-low);price=`<path d="${path(values.map((value,index)=>[x(index),yPrice(value)]))}" fill="none" stroke="#bb7558" stroke-width="2" stroke-linejoin="round"/><text x="${width-right+7}" y="${top+4}" fill="#a46b50" font-size="10">${number(high,0)}</text><text x="${width-right+7}" y="${top+plotH}" fill="#a46b50" font-size="10">${number(low,0)}</text>`}
    const final=temperaturePoints.at(-1);
    const temperaturePath=temperaturePoints.length>1?`<path d="${path(temperaturePoints)}" fill="none" stroke="#365ec9" stroke-width="2.8" stroke-linejoin="round"/>`:'';
    const temperatureMarker=final?`<circle cx="${final[0]}" cy="${final[1]}" r="4" fill="#365ec9" stroke="#fff" stroke-width="2"/>`:'';
    const emptyNotice=temperaturePoints.length===0?`<text x="${left}" y="${top+plotH/2}" fill="#8995a5" font-size="11">暂无温度有效样本</text>`:'';
    return `<svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" data-price-low="${low}" data-price-high="${high}" role="img" aria-label="${paired?'指数收盘价与温度':'温度'}走势，${records[0].date}至${records.at(-1).date}">${grid}<line x1="${left}" x2="${width-right}" y1="${top+plotH}" y2="${top+plotH}" stroke="#d8dee8"/>${ticks}${price}${temperaturePath}${temperatureMarker}${emptyNotice}<g class="chart-focus" style="display:none"><line class="focus-line" y1="${top}" y2="${top+plotH}" stroke="#8fa2c2" stroke-dasharray="3 4"/><circle class="focus-temp" r="5" fill="#365ec9" stroke="#fff" stroke-width="2"/>${paired?'<circle class="focus-price" r="5" fill="#bb7558" stroke="#fff" stroke-width="2"/>':''}</g></svg><div class="chart-tooltip" hidden></div>`;
  }

  function bindChart(slot, records, paired) {
    const svg=slot.querySelector('svg'), tooltip=slot.querySelector('.chart-tooltip'), focus=svg.querySelector('.chart-focus');
    const tempMarker=focus.querySelector('.focus-temp');
    const width=820, height=paired?194:225, left=39, right=paired?55:18, top=15, bottom=27;
    const plotW=width-left-right, plotH=height-top-bottom;
    const show=event=>{
      const rect=svg.getBoundingClientRect();
      const pointerX=event.clientX-rect.left, pointerY=event.clientY-rect.top;
      const svgX=pointerX*width/rect.width;
      const index=Math.max(0,Math.min(records.length-1,Math.round((svgX-left)*(records.length-1)/plotW)));
      const day=records[index], x=left+index*plotW/(records.length-1), tempY=day.temperature==null?null:top+(100-day.temperature)*plotH/100;
      focus.style.display='';
      focus.querySelector('.focus-line').setAttribute('x1',x);
      focus.querySelector('.focus-line').setAttribute('x2',x);
      tempMarker.style.display=tempY==null?'none':'';
      if(tempY!=null){tempMarker.setAttribute('cx',x);tempMarker.setAttribute('cy',tempY)}
      if(paired){const low=Number(svg.dataset.priceLow), high=Number(svg.dataset.priceHigh), marker=focus.querySelector('.focus-price');marker.setAttribute('cx',x);marker.setAttribute('cy',top+(high-day.close)*plotH/(high-low))}
      tooltip.innerHTML=`<strong>${esc(day.date)}</strong><span>当天温度：<b>${number(day.temperature)}</b></span><span>温度1日变化：<b>${signed(day.temperature_change_1d)}</b></span><span>指数收盘价：<b>${number(day.close,2)}</b></span><span>指数涨跌幅：<b>${signed(day.pct_change,'%')}</b></span>`;
      tooltip.hidden=false;
      tooltip.style.left=`${Math.max(0,Math.min(slot.clientWidth-tooltip.offsetWidth,pointerX+12))}px`;
      tooltip.style.top=`${Math.max(0,Math.min(slot.clientHeight-tooltip.offsetHeight,pointerY-tooltip.offsetHeight-12))}px`;
    };
    svg.addEventListener('pointermove',show);
    svg.addEventListener('pointerdown',show);
    svg.addEventListener('pointerleave',()=>{focus.style.display='none';tooltip.hidden=true});
  }

  function renderDetail() {
    if (!state.code) return;
    const item=byCode[state.code], all=rows(state.code), r=all.at(-1), recent=all.slice(-state.range);
    $('detail-name').textContent=`${item.name} · ${item.code}`;
    $('detail-summary').textContent=`${r.date} · ${state.version} · 当前温度 ${number(r.temperature)} · ${temperatureLabel(r.temperature,item)}`;
    $('temperature-chart').innerHTML=chart(recent,false);
    $('price-chart').innerHTML=chart(recent,true);
    bindChart($('temperature-chart'),recent,false);
    bindChart($('price-chart'),recent,true);
    $('score-title').textContent=state.version==='V1.1'?'V1.1 四维度':'V1.0 七项 score';
    $('scores').innerHTML=scoreKeys[state.version].map(key=>{const value=r[key];return `<div class="score-row"><div class="score-label"><span>${scoreNames[key]}</span><b>${number(value)}</b></div><div class="score-track"><div class="score-fill" style="width:${value==null?0:Math.max(0,Math.min(100,value))}%"></div></div></div>`}).join('');
    $('quality-fields').innerHTML=`<div><dt>数据日期</dt><dd>${esc(r.date)}</dd></div><div><dt>formula_version</dt><dd>${esc(state.version)}</dd></div><div><dt>数据状态</dt><dd>${item.temperature_status==='pending'?'温度待补样本':'温度可用'}</dd></div><div><dt>source_symbol</dt><dd>${esc(item.source_symbol)}</dd></div><div><dt>constituent_mode</dt><dd>${esc(r.constituent_mode)}</dd></div><div><dt>coverage_ratio</dt><dd>${number(r.coverage_ratio*100)}%</dd></div>`;
    $('quality-note').textContent=r.temperature==null?(item.temperature_status==='pending'?'温度样本不足，当前保留指数行情，温度相关字段显示为 NA。':'当前没有可用温度样本。'):r.constituent_mode==='current_constituents_proxy'?'历史成分指标基于当前成分股回算，仅用于市场状态观察。':'';
    $('recent').innerHTML=all.slice(-10).reverse().map(day=>`<tr><td>${esc(day.date)}</td><td class="temp-cell" style="background:${temperatureBg(day.temperature)};color:${day.temperature >= 100 ? '#fff' : tone(day.temperature)}">${number(day.temperature)}</td><td class="${classOf(day.temperature_change_1d)}">${signed(day.temperature_change_1d)}</td><td class="${classOf(day.temperature_change_5d)}">${signed(day.temperature_change_5d)}</td><td>${temperatureLabel(day.temperature,item)}</td><td class="${classOf(day.pct_change)}">${signed(day.pct_change,'%')}</td></tr>`).join('');
    document.querySelectorAll('[data-range]').forEach(button=>{button.classList.toggle('active',Number(button.dataset.range)===state.range);button.setAttribute('aria-pressed',Number(button.dataset.range)===state.range?'true':'false')});
  }

  function openDetail(code) {state.code=code;$('detail').hidden=false;renderDetail();$('detail').scrollIntoView({behavior:'smooth',block:'start'})}
  function render() {
    document.querySelectorAll('[data-version]').forEach(button=>{button.classList.toggle('active',button.dataset.version===state.version);button.setAttribute('aria-pressed',button.dataset.version===state.version?'true':'false')});
    document.querySelectorAll('[data-sheet]').forEach(button=>{const active=button.dataset.sheet===state.sheet;button.classList.toggle('active',active);button.setAttribute('aria-selected',active?'true':'false')});
    $('sheet-content').setAttribute('aria-labelledby',`tab-${state.sheet}`);
    const groupCounts=source.indices.reduce((counts,item)=>{counts[item.group]=(counts[item.group]||0)+1;return counts},{broad:0,industry:0});
    $('broad-count').textContent=groupCounts.broad;
    $('industry-count').textContent=groupCounts.industry;
    const list=itemList(), dates=[...new Set(list.map(item=>item.row.date))];
    const dateText=dates.length===1?`数据日期 ${dates[0]}`:'各指数数据日期以卡片为准';
    $('snapshot').textContent=`${list.length} 个指数 · ${dateText} · 数据更新时间 ${source.updated_at || '未知'}`;
    renderCards();renderRanking();renderDetail();
  }
  document.querySelectorAll('[data-version]').forEach(button=>button.addEventListener('click',()=>{state.version=button.dataset.version;render()}));
  document.querySelectorAll('[data-sheet]').forEach(button=>button.addEventListener('click',()=>{state.sheet=button.dataset.sheet;if(state.code&&!belongsToSheet(state.code)){state.code=null;$('detail').hidden=true}render()}));
  document.querySelectorAll('[data-range]').forEach(button=>button.addEventListener('click',()=>{state.range=Number(button.dataset.range);renderDetail()}));
  $('sort').addEventListener('change',event=>{state.sort=event.target.value;renderCards()});
  $('close-detail').addEventListener('click',()=>{$('detail').hidden=true;state.code=null});
  render();
})();
</script>
</body>
</html>'''


def main() -> None:
    parser = argparse.ArgumentParser(description="生成指数温度计 HTML 页面")
    parser.add_argument(
        "--as-of",
        dest="as_of_date",
        help="只使用该日期及之前的数据，例如 2026-09-15",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT,
        help="输出 HTML 文件路径",
    )
    args = parser.parse_args()
    as_of_date = date.fromisoformat(args.as_of_date) if args.as_of_date else None
    output_path = args.output if args.output.is_absolute() else ROOT / args.output
    payload = json.dumps(read_dashboard_data(as_of_date), ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    payload = payload.replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(HTML.replace("__DATA__", payload), encoding="utf-8")
    print(f"已生成：{output_path}")
    print(f"文件大小：{output_path.stat().st_size:,} 字节")


if __name__ == "__main__":
    main()
