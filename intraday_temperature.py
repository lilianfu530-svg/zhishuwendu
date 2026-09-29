from __future__ import annotations

import argparse
import hashlib
import html
import json
import math
import os
import re
import shutil
import time as time_module
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import duckdb
import numpy as np
import pandas as pd

from build_dashboard import HTML as EXISTING_DASHBOARD_HTML
from main import average_pairwise_correlation, rolling_percentile, temperature_level


ROOT = Path(__file__).resolve().parent
DATABASE = ROOT / "data" / "market.duckdb"
OUTPUT_ROOT = ROOT / "output" / "intraday"
SHANGHAI = ZoneInfo("Asia/Shanghai")
HITHINK_BASE_URL = "https://fuyao.aicubes.cn"
HITHINK_INDEX_SNAPSHOT = "/api/a-share-index/prices/snapshot"
HITHINK_INDEX_CONSTITUENTS = "/api/a-share-index/constituents/ths-stock-list"
HITHINK_STOCK_SNAPSHOT = "/api/a-share/prices/snapshot"
HITHINK_INDEX_CATALOG = "/api/a-share-index/catalog/ths-index-list"
BENCHMARK_SYMBOL = "000300.SH"
REGISTRY_STATUSES = ("active", "temperature_pending")
ACTIVE_STATUS = "active"
PENDING_STATUS = "temperature_pending"
FORMAL_VERSION = "V1.1"
TEST_VERSION = "V1.1-INTRADAY-TEST"
FORMAL_INTRADAY_VERSION = "V1.1-I1415"
SNAPSHOT_COLUMNS = (
    "market",
    "index_code",
    "index_name",
    "snapshot_date",
    "snapshot_time",
    "requested_time",
    "actual_snapshot_time",
    "formula_version",
    "index_price",
    "index_pct_change",
    "RET20_raw",
    "RET20_score",
    "BIAS20_raw",
    "BIAS20_score",
    "RS20_raw",
    "RS20_score",
    "PriceMomentum_score",
    "Volume_score",
    "BreadthMA20_raw",
    "BreadthMA20_score",
    "HLBreadth_raw",
    "HLBreadth_score",
    "Breadth_score",
    "Sync_raw",
    "Sync_score",
    "temperature_intraday",
    "temperature_level",
    "previous_snapshot_temperature",
    "change_vs_previous_snapshot",
    "previous_close_temperature",
    "change_vs_previous_close",
    "valid_constituent_count",
    "total_constituent_count",
    "coverage_ratio",
    "today_intraday_amount",
    "source",
    "source_symbol",
    "fetched_at",
    "constituent_mode",
    "data_status",
    "failure_reason",
)
RAW_FIELD_BY_NAME = {
    "RET20": "RET20_raw",
    "BIAS20": "BIAS20_raw",
    "RS20": "RS20_raw",
    "BreadthMA20": "BreadthMA20_raw",
    "HLBreadth": "HLBreadth_raw",
    "Sync": "Sync_raw",
}


class HithinkError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: int | None = None,
        request_id: str | None = None,
        retryable: bool = False,
        empty: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.request_id = request_id
        self.retryable = retryable
        self.empty = empty


def now_local() -> datetime:
    return datetime.now(SHANGHAI).replace(tzinfo=None)


def iso_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if pd.isna(value):
        return None
    return value


def safe_float(value: Any) -> float | None:
    value = iso_value(value)
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def rounded(value: Any, digits: int = 1) -> float | None:
    number = safe_float(value)
    return None if number is None else round(number, digits)


def json_default(value: Any) -> Any:
    value = iso_value(value)
    if value is not None:
        return value
    raise TypeError(f"无法序列化类型：{type(value).__name__}")


def normalize_name(value: str) -> str:
    return re.sub(r"指数|主题|概念|[（）()\s_\-]", "", value or "").lower()


def as_local_datetime(timestamp: Any) -> datetime | None:
    number = safe_float(timestamp)
    if number is None:
        return None
    try:
        return datetime.fromtimestamp(number / 1000, tz=SHANGHAI).replace(tzinfo=None)
    except (OverflowError, OSError, ValueError):
        return None


def read_api_key() -> str:
    key = os.getenv("HITHINK_FINANCE_API_KEY")
    if key:
        return key.strip()
    legacy_key = os.getenv("FUYAO_TOKEN") or os.getenv("API_KEY")
    if legacy_key:
        return legacy_key.strip()
    credentials_path = Path(os.getenv("APPDATA", "")) / "hithink-finance" / "credentials.env"
    if credentials_path.is_file():
        for line in credentials_path.read_text(encoding="utf-8").splitlines():
            if line.startswith("HITHINK_FINANCE_API_KEY="):
                value = line.partition("=")[2].strip().strip('"').strip("'")
                if value:
                    return value
    raise RuntimeError("缺少 HITHINK_FINANCE_API_KEY，无法请求同花顺盘中接口")


class HithinkClient:
    def __init__(self, api_key: str) -> None:
        self.api_key = api_key
        self._last_request_at = 0.0

    def _request(self, path: str, params: dict[str, str]) -> tuple[dict[str, Any], datetime | None]:
        query = urllib.parse.urlencode(params)
        url = f"{HITHINK_BASE_URL}{path}?{query}"
        retry_codes = {4001, 5001, 5002, 5003}
        for attempt in range(4):
            elapsed = time_module.monotonic() - self._last_request_at
            if elapsed < 0.08:
                time_module.sleep(0.08 - elapsed)
            request = urllib.request.Request(
                url,
                headers={"X-api-key": self.api_key, "Accept": "application/json"},
                method="GET",
            )
            self._last_request_at = time_module.monotonic()
            try:
                with urllib.request.urlopen(request, timeout=35) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                if attempt < 3 and exc.code >= 500:
                    time_module.sleep(0.6 * (2**attempt))
                    continue
                raise HithinkError(f"同花顺 HTTP 请求失败：{exc.code}", retryable=exc.code >= 500) from exc
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                if attempt < 3:
                    time_module.sleep(0.6 * (2**attempt))
                    continue
                raise HithinkError(f"同花顺网络或响应解析失败：{type(exc).__name__}", retryable=True) from exc

            code = payload.get("code")
            request_id = payload.get("request_id")
            if code != 0:
                message = str(payload.get("message") or "未提供错误说明")
                if code in retry_codes and attempt < 3:
                    time_module.sleep(0.6 * (2**attempt))
                    continue
                raise HithinkError(
                    f"同花顺业务错误 code={code}：{message}",
                    code=code,
                    request_id=request_id,
                    retryable=code in retry_codes,
                )
            data = payload.get("data") or {}
            if not isinstance(data, dict):
                raise HithinkError("同花顺响应 data 不是对象")
            return data, as_local_datetime(data.get("timestamp"))
        raise HithinkError("同花顺请求重试次数耗尽", retryable=True)

    def index_snapshot(self, thscode: str) -> tuple[dict[str, Any], datetime | None]:
        data, fetched_at = self._request(HITHINK_INDEX_SNAPSHOT, {"thscodes": thscode})
        items = data.get("item") or []
        if not items:
            raise HithinkError("同花顺指数快照为空", empty=True)
        wanted = thscode.upper()
        for item in items:
            if str(item.get("thscode", "")).upper() == wanted:
                return item, fetched_at
        raise HithinkError("同花顺指数快照未返回请求标的", empty=True)

    def constituents(self, thscode: str) -> tuple[list[dict[str, Any]], datetime | None]:
        data, fetched_at = self._request(HITHINK_INDEX_CONSTITUENTS, {"thscode": thscode})
        items = data.get("item") or []
        if not items:
            raise HithinkError("同花顺当前成分股为空", empty=True)
        return items, fetched_at

    def stock_snapshots(
        self, thscodes: list[str], chunk_size: int = 100
    ) -> tuple[dict[str, dict[str, Any]], dict[str, str], datetime | None]:
        quotes: dict[str, dict[str, Any]] = {}
        failures: dict[str, str] = {}
        latest_fetched_at: datetime | None = None
        for start in range(0, len(thscodes), chunk_size):
            chunk = thscodes[start : start + chunk_size]
            try:
                data, fetched_at = self._request(
                    HITHINK_STOCK_SNAPSHOT,
                    {"thscodes": ",".join(chunk)},
                )
                latest_fetched_at = max_datetime(latest_fetched_at, fetched_at)
                for item in data.get("item") or []:
                    symbol = str(item.get("thscode", "")).upper()
                    if symbol:
                        quotes[symbol] = item
                returned = {str(item.get("thscode", "")).upper() for item in data.get("item") or []}
                for symbol in chunk:
                    if symbol.upper() not in returned:
                        failures[symbol.upper()] = "同花顺股票快照未返回该成分股"
            except HithinkError as exc:
                reason = str(exc)
                for symbol in chunk:
                    failures[symbol.upper()] = reason
        return quotes, failures, latest_fetched_at

    def catalog(self) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for tag in ("cn_concept", "region", "tszs", "industry"):
            data, _ = self._request(HITHINK_INDEX_CATALOG, {"tag": tag})
            for item in data.get("item") or []:
                symbol = str(item.get("thscode", "")).upper()
                name = str(item.get("name", ""))
                if symbol and name:
                    mapping[symbol] = name
        return mapping


def max_datetime(left: datetime | None, right: datetime | None) -> datetime | None:
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


def hithink_symbol_from_registry(item: dict[str, Any], catalog: dict[str, str]) -> tuple[str | None, str | None]:
    source_symbol = str(item.get("source_symbol") or "").strip().upper()
    if re.fullmatch(r"SH\d{6}", source_symbol):
        return f"{source_symbol[2:]}.SH", None
    if re.fullmatch(r"SZ\d{6}", source_symbol):
        return f"{source_symbol[2:]}.SZ", None
    if re.fullmatch(r"\d{6}\.(SH|SZ|TI)", source_symbol):
        return source_symbol, None
    if source_symbol.endswith(".TI"):
        return source_symbol, None
    if source_symbol.startswith("SI."):
        candidate = f"{source_symbol[3:]}.TI"
        catalog_name = catalog.get(candidate)
        if catalog_name and normalize_name(catalog_name) == normalize_name(str(item.get("display_name") or "")):
            return candidate, None
        return None, "无法在同花顺目录中核验 SI 标的的 EXACT 身份"
    if source_symbol.startswith("HK.") or item.get("market") == "HK":
        return None, "同花顺公开 A 股指数接口不支持港股盘中数据"
    return None, "注册表 source_symbol 不符合已核验的同花顺指数代码格式"


def parse_stock_symbol(thscode: str) -> tuple[str, str] | None:
    value = str(thscode or "").strip().upper()
    if "." not in value:
        return None
    ticker, suffix = value.rsplit(".", 1)
    market = {"SH": "SH", "SZ": "SZ", "BJ": "BJ"}.get(suffix)
    if market is None or not ticker:
        return None
    return market, ticker


def load_registry(connection: duckdb.DuckDBPyConnection) -> tuple[list[dict[str, Any]], dict[str, int]]:
    rows = connection.execute(
        """
        SELECT index_code, display_name, market, primary_source, source_symbol,
               status, constituent_mode, last_data_date, temperature_status,
               initialized_at
        FROM index_registry
        WHERE status IN (?, ?)
        ORDER BY initialized_at, index_code
        """,
        REGISTRY_STATUSES,
    ).fetchall()
    columns = (
        "index_code", "index_name", "market", "daily_primary_source", "source_symbol",
        "status", "constituent_mode", "last_data_date", "temperature_status", "initialized_at",
    )
    registry = [dict(zip(columns, row)) for row in rows]
    counts = {
        "registry_total": int(connection.execute("SELECT COUNT(*) FROM index_registry").fetchone()[0]),
        "active_count": int(connection.execute("SELECT COUNT(*) FROM index_registry WHERE status='active'").fetchone()[0]),
        "pending_count": int(connection.execute("SELECT COUNT(*) FROM index_registry WHERE status='temperature_pending'").fetchone()[0]),
        "disabled_count": int(connection.execute("SELECT COUNT(*) FROM index_registry WHERE status='disabled'").fetchone()[0]),
    }
    return registry, counts


def dataframe_hash(frame: pd.DataFrame) -> str:
    normalized = frame.copy()
    for column in normalized.columns:
        if pd.api.types.is_datetime64_any_dtype(normalized[column]):
            normalized[column] = normalized[column].astype(str)
        elif normalized[column].dtype == object:
            normalized[column] = normalized[column].map(lambda value: None if value is None else str(value))
    digest = pd.util.hash_pandas_object(normalized, index=False).to_numpy().tobytes()
    return hashlib.sha256(digest).hexdigest()


def formal_baseline(connection: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    baseline: dict[str, Any] = {}
    for table in ("index_daily", "stock_daily", "indicator_daily"):
        if table == "indicator_daily":
            row = connection.execute(
                """
                SELECT COUNT(*), MIN(date), MAX(date),
                       SUM(COALESCE(temperature, 0)),
                       SUM(COALESCE(PriceMomentum_score, 0)),
                       SUM(COALESCE(Breadth_score, 0))
                FROM indicator_daily
                """
            ).fetchone()
        else:
            row = connection.execute(
                f"""
                SELECT COUNT(*), MIN(date), MAX(date),
                       SUM(COALESCE(close, 0)), SUM(COALESCE(volume, 0)), SUM(COALESCE(amount, 0))
                FROM {table}
                """
            ).fetchone()
        baseline[table] = {
            "row_count": int(row[0]),
            "min_date": iso_value(row[1]),
            "max_date": iso_value(row[2]),
            "close_sum": safe_float(row[3]),
            "volume_sum": safe_float(row[4]),
            "amount_sum": safe_float(row[5]),
        }
    formal = connection.execute(
        """
        SELECT *
        FROM indicator_daily
        WHERE formula_version IN ('V1.0', 'V1.1')
        ORDER BY index_code, date, formula_version
        """
    ).df()
    baseline["formal_indicator_hash"] = dataframe_hash(formal)
    return baseline


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def create_database_backup() -> dict[str, str]:
    backup_time = now_local()
    backup_dir = ROOT / "backup" / f"before_intraday_{backup_time.strftime('%Y%m%d_%H%M%S')}"
    backup_dir.mkdir(parents=True, exist_ok=False)
    backup_path = backup_dir / "market.duckdb"
    shutil.copy2(DATABASE, backup_path)
    digest = sha256_file(backup_path)
    manifest = {
        "created_at": backup_time.isoformat(sep=" "),
        "source": str(DATABASE),
        "backup": str(backup_path),
        "sha256": digest,
    }
    (backup_dir / "backup_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return manifest


def ensure_snapshot_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS intraday_snapshot (
            market VARCHAR NOT NULL,
            index_code VARCHAR NOT NULL,
            index_name VARCHAR NOT NULL,
            snapshot_date DATE NOT NULL,
            snapshot_time VARCHAR NOT NULL,
            requested_time VARCHAR NOT NULL,
            actual_snapshot_time TIMESTAMP,
            formula_version VARCHAR NOT NULL,
            index_price DOUBLE,
            index_pct_change DOUBLE,
            RET20_raw DOUBLE,
            RET20_score DOUBLE,
            BIAS20_raw DOUBLE,
            BIAS20_score DOUBLE,
            RS20_raw DOUBLE,
            RS20_score DOUBLE,
            PriceMomentum_score DOUBLE,
            Volume_score DOUBLE,
            BreadthMA20_raw DOUBLE,
            BreadthMA20_score DOUBLE,
            HLBreadth_raw DOUBLE,
            HLBreadth_score DOUBLE,
            Breadth_score DOUBLE,
            Sync_raw DOUBLE,
            Sync_score DOUBLE,
            temperature_intraday DOUBLE,
            temperature_level VARCHAR,
            previous_snapshot_temperature DOUBLE,
            change_vs_previous_snapshot DOUBLE,
            previous_close_temperature DOUBLE,
            change_vs_previous_close DOUBLE,
            valid_constituent_count INTEGER,
            total_constituent_count INTEGER,
            coverage_ratio DOUBLE,
            today_intraday_amount DOUBLE,
            source VARCHAR,
            source_symbol VARCHAR,
            fetched_at TIMESTAMP,
            constituent_mode VARCHAR,
            data_status VARCHAR,
            failure_reason VARCHAR,
            PRIMARY KEY (market, index_code, snapshot_date, snapshot_time, formula_version)
        )
        """
    )


def load_index_history(
    connection: duckdb.DuckDBPyConnection,
    codes: list[str],
    snapshot_date: date,
) -> dict[str, pd.DataFrame]:
    if not codes:
        return {}
    placeholders = ",".join("?" for _ in codes)
    data = connection.execute(
        f"""
        SELECT index_code, date, close, pct_change
        FROM index_daily
        WHERE index_code IN ({placeholders}) AND date < ?
        ORDER BY index_code, date
        """,
        [*codes, snapshot_date],
    ).df()
    result: dict[str, pd.DataFrame] = {}
    for code, group in data.groupby("index_code", sort=False):
        result[str(code)] = group.drop(columns="index_code").reset_index(drop=True)
    return result


def load_indicator_history(
    connection: duckdb.DuckDBPyConnection,
    codes: list[str],
    snapshot_date: date,
) -> dict[str, pd.DataFrame]:
    if not codes:
        return {}
    placeholders = ",".join("?" for _ in codes)
    data = connection.execute(
        f"""
        SELECT index_code, date, RET20_raw, BIAS20_raw, RS20_raw,
               BreadthMA20_raw, HLBreadth_raw, Sync_raw,
               Volume_score, VolumeStrength_score, temperature
        FROM indicator_daily
        WHERE index_code IN ({placeholders})
          AND formula_version = ?
          AND date < ?
        ORDER BY index_code, date
        """,
        [*codes, FORMAL_VERSION, snapshot_date],
    ).df()
    result: dict[str, pd.DataFrame] = {}
    for code, group in data.groupby("index_code", sort=False):
        result[str(code)] = group.drop(columns="index_code").reset_index(drop=True)
    return result


def load_stock_history(
    connection: duckdb.DuckDBPyConnection,
    members_by_index: dict[str, list[tuple[str, str, str]]],
    snapshot_date: date,
) -> dict[tuple[str, str], dict[date, float]]:
    members = sorted({(market, stock_code) for rows in members_by_index.values() for market, stock_code, _ in rows})
    if not members:
        return {}
    member_frame = pd.DataFrame(members, columns=["market", "stock_code"])
    connection.register("intraday_members", member_frame)
    try:
        data = connection.execute(
            """
            WITH ranked AS (
                SELECT d.market, d.stock_code, d.date, d.close,
                       ROW_NUMBER() OVER (
                           PARTITION BY d.market, d.stock_code ORDER BY d.date DESC
                       ) AS row_number
                FROM stock_daily AS d
                JOIN intraday_members AS m
                  ON d.market = m.market AND d.stock_code = m.stock_code
                WHERE d.date < ?
            )
            SELECT market, stock_code, date, close
            FROM ranked
            WHERE row_number <= 30
            ORDER BY market, stock_code, date
            """,
            [snapshot_date],
        ).fetchall()
    finally:
        connection.unregister("intraday_members")
    history: dict[tuple[str, str], dict[date, float]] = defaultdict(dict)
    for market, stock_code, row_date, close in data:
        value = safe_float(close)
        if value is not None:
            history[(str(market), str(stock_code))][row_date] = value
    return dict(history)


def local_formal_dates(history: pd.DataFrame, count: int = 20) -> list[date]:
    if history.empty:
        return []
    dates = [value.date() if hasattr(value, "date") else value for value in history["date"].tolist()]
    return dates[-count:]


def quote_price(quote: dict[str, Any] | None) -> float | None:
    if quote is None:
        return None
    return safe_float(quote.get("last_price"))


def provider_pct_change(quote: dict[str, Any] | None) -> float | None:
    if quote is None:
        return None
    return safe_float(quote.get("price_change_ratio_pct"))


def provider_amount(quote: dict[str, Any] | None) -> float | None:
    if quote is None:
        return None
    return safe_float(quote.get("turnover"))


def current_score(
    historical_values: pd.Series | None,
    current_value: float | None,
) -> float | None:
    if current_value is None or historical_values is None:
        return None
    series = pd.concat(
        [pd.to_numeric(historical_values, errors="coerce"), pd.Series([current_value], dtype=float)],
        ignore_index=True,
    )
    value = rolling_percentile(series).iloc[-1]
    return safe_float(value)


def base_snapshot_row(
    item: dict[str, Any],
    snapshot_date: date,
    requested_time: str,
    actual_snapshot_time: datetime,
    formula_version: str,
    source_symbol: str | None,
) -> dict[str, Any]:
    row = {column: None for column in SNAPSHOT_COLUMNS}
    row.update(
        {
            "market": item["market"],
            "index_code": item["index_code"],
            "index_name": item["index_name"],
            "snapshot_date": snapshot_date,
            "snapshot_time": requested_time,
            "requested_time": requested_time,
            "actual_snapshot_time": actual_snapshot_time,
            "formula_version": formula_version,
            "source": "hithink",
            "source_symbol": source_symbol or item.get("source_symbol"),
            "constituent_mode": item.get("constituent_mode"),
        }
    )
    return row


def calculate_breadth_and_sync(
    members: list[tuple[str, str, str]],
    stock_quotes: dict[str, dict[str, Any]],
    stock_history: dict[tuple[str, str], dict[date, float]],
    formal_dates: list[date],
) -> tuple[float | None, float | None, float | None, int, int, float | None, str | None]:
    total = len(members)
    if total == 0 or len(formal_dates) < 20:
        return None, None, None, 0, total, None, "当前成分股或正式历史不足"

    valid_count = 0
    breadth_values: list[float] = []
    high_values: list[float] = []
    return_columns: dict[str, list[float]] = {}
    for market, stock_code, thscode in members:
        quote = stock_quotes.get(thscode.upper())
        current = quote_price(quote)
        history_by_date = stock_history.get((market, stock_code), {})
        previous_20 = [history_by_date.get(day) for day in formal_dates[-20:]]
        if current is None or len(previous_20) != 20 or any(value is None for value in previous_20):
            continue
        previous_19 = previous_20[-19:]
        ma20 = (sum(previous_19) + current) / 20
        breadth_values.append(1.0 if current > ma20 else 0.0)
        high = 1.0 if current >= max(previous_19) else 0.0
        low = 1.0 if current <= min(previous_19) else 0.0
        high_values.append(high - low)
        previous_returns = np.diff(np.asarray(previous_20, dtype=float)) / np.asarray(previous_20[:-1], dtype=float)
        current_return = (current / previous_20[-1]) - 1
        return_columns[thscode] = [*previous_returns.tolist(), current_return]
        valid_count += 1

    coverage_ratio = valid_count / total if total else None
    if coverage_ratio is None or coverage_ratio < 0.80:
        return None, None, None, valid_count, total, coverage_ratio, "coverage_ratio 低于 0.80"
    if not breadth_values or not high_values or len(return_columns) < 2:
        return None, None, None, valid_count, total, coverage_ratio, "有效成分不足以计算广度或同步"

    returns = pd.DataFrame(return_columns)
    sync = average_pairwise_correlation(returns)
    return (
        float(np.mean(breadth_values)),
        float(np.mean(high_values)),
        safe_float(sync),
        valid_count,
        total,
        coverage_ratio,
        None,
    )


def calculate_index_row(
    item: dict[str, Any],
    source_symbol: str | None,
    index_quote: dict[str, Any] | None,
    index_fetched_at: datetime | None,
    benchmark_quote: dict[str, Any] | None,
    index_history: pd.DataFrame | None,
    benchmark_history: pd.DataFrame | None,
    indicator_history: pd.DataFrame | None,
    members: list[tuple[str, str, str]] | None,
    stock_quotes: dict[str, dict[str, Any]],
    stock_history: dict[tuple[str, str], dict[date, float]],
    snapshot_date: date,
    requested_time: str,
    actual_snapshot_time: datetime,
    formula_version: str,
    previous_close_temperature: float | None,
    previous_snapshot_temperature: float | None,
    constituent_failure: str | None = None,
) -> dict[str, Any]:
    row = base_snapshot_row(
        item,
        snapshot_date,
        requested_time,
        actual_snapshot_time,
        formula_version,
        source_symbol,
    )
    reasons: list[str] = []
    row["previous_close_temperature"] = previous_close_temperature
    row["previous_snapshot_temperature"] = previous_snapshot_temperature
    if previous_close_temperature is not None:
        row["change_vs_previous_close"] = None
    if index_quote is None:
        row["data_status"] = "source_unavailable"
        row["failure_reason"] = constituent_failure or "同花顺指数盘中快照不可用"
        return row

    row["index_price"] = quote_price(index_quote)
    row["index_pct_change"] = provider_pct_change(index_quote)
    row["today_intraday_amount"] = provider_amount(index_quote)
    row["fetched_at"] = index_fetched_at
    if row["index_price"] is None:
        row["data_status"] = "source_unavailable"
        row["failure_reason"] = "同花顺指数快照缺少 last_price"
        return row
    if index_history is None or benchmark_history is None or benchmark_quote is None:
        if index_history is None:
            reasons.append("本地正式指数历史不足或缺失")
        if benchmark_history is None or benchmark_quote is None:
            reasons.append("沪深300同时间盘中快照或正式历史不可用")
    else:
        index_closes = pd.to_numeric(index_history["close"], errors="coerce").dropna().tolist()
        benchmark_closes = pd.to_numeric(benchmark_history["close"], errors="coerce").dropna().tolist()
        benchmark_price = quote_price(benchmark_quote)
        if len(index_closes) >= 20:
            row["RET20_raw"] = row["index_price"] / index_closes[-20] - 1
        else:
            reasons.append("指数 RET20 正式历史不足 20 个交易日")
        if len(index_closes) >= 19:
            row["BIAS20_raw"] = row["index_price"] / ((sum(index_closes[-19:]) + row["index_price"]) / 20) - 1
        else:
            reasons.append("指数 BIAS20 正式历史不足 19 个交易日")
        if benchmark_price is not None and len(benchmark_closes) >= 20 and len(index_closes) >= 20:
            index_ret20 = row["index_price"] / index_closes[-20] - 1
            benchmark_ret20 = benchmark_price / benchmark_closes[-20] - 1
            row["RS20_raw"] = index_ret20 - benchmark_ret20
        else:
            reasons.append("RS20 缺少同时间沪深300价格或 20 日正式历史")

    historical = indicator_history if indicator_history is not None else pd.DataFrame()
    for metric_name, raw_column in RAW_FIELD_BY_NAME.items():
        current = safe_float(row.get(raw_column))
        row[f"{metric_name}_score"] = current_score(
            historical[raw_column] if raw_column in historical else None,
            current,
        )

    if historical.empty:
        reasons.append("正式 V1.1 指标历史缺失")
    else:
        latest_formal = historical.iloc[-1]
        row["Volume_score"] = safe_float(latest_formal.get("Volume_score"))
        if row["Volume_score"] is None:
            row["Volume_score"] = safe_float(latest_formal.get("VolumeStrength_score"))
    row["fetched_at"] = max_datetime(row["fetched_at"], index_fetched_at)

    if members is None:
        reasons.append(constituent_failure or "同花顺当前成分股不可用")
    else:
        formal_dates = local_formal_dates(index_history if index_history is not None else pd.DataFrame())
        breadth, hl_breadth, sync, valid_count, total_count, coverage, coverage_reason = calculate_breadth_and_sync(
            members,
            stock_quotes,
            stock_history,
            formal_dates,
        )
        row["valid_constituent_count"] = valid_count
        row["total_constituent_count"] = total_count
        row["coverage_ratio"] = coverage
        if coverage_reason:
            reasons.append(coverage_reason)
        if coverage is not None and coverage >= 0.80:
            row["BreadthMA20_raw"] = breadth
            row["HLBreadth_raw"] = hl_breadth
            row["Sync_raw"] = sync
            row["BreadthMA20_score"] = current_score(
                historical["BreadthMA20_raw"] if "BreadthMA20_raw" in historical else None,
                breadth,
            )
            row["HLBreadth_score"] = current_score(
                historical["HLBreadth_raw"] if "HLBreadth_raw" in historical else None,
                hl_breadth,
            )
            row["Sync_score"] = current_score(
                historical["Sync_raw"] if "Sync_raw" in historical else None,
                sync,
            )
        else:
            row["BreadthMA20_raw"] = None
            row["HLBreadth_raw"] = None
            row["Sync_raw"] = None
            row["BreadthMA20_score"] = None
            row["HLBreadth_score"] = None
            row["Sync_score"] = None

    price_scores = [row.get("RET20_score"), row.get("BIAS20_score"), row.get("RS20_score")]
    breadth_scores = [row.get("BreadthMA20_score"), row.get("HLBreadth_score")]
    row["PriceMomentum_score"] = float(np.mean(price_scores)) if all(value is not None for value in price_scores) else None
    row["Breadth_score"] = float(np.mean(breadth_scores)) if all(value is not None for value in breadth_scores) else None
    dimensions = [row.get("PriceMomentum_score"), row.get("Volume_score"), row.get("Breadth_score"), row.get("Sync_score")]
    row["temperature_intraday"] = rounded(np.mean(dimensions), 1) if all(value is not None for value in dimensions) else None
    row["temperature_level"] = temperature_level(row["temperature_intraday"]) if row["temperature_intraday"] is not None else None
    if row["temperature_intraday"] is not None and previous_close_temperature is not None:
        row["change_vs_previous_close"] = rounded(row["temperature_intraday"] - previous_close_temperature)
    if row["temperature_intraday"] is not None and previous_snapshot_temperature is not None:
        row["change_vs_previous_snapshot"] = rounded(row["temperature_intraday"] - previous_snapshot_temperature)
    if row["temperature_intraday"] is not None:
        row["data_status"] = "ok"
    elif row["data_status"] is None:
        row["data_status"] = "insufficient_data"
    row["failure_reason"] = "; ".join(dict.fromkeys(reason for reason in reasons if reason)) or None
    return row


def previous_formal_temperatures(
    connection: duckdb.DuckDBPyConnection,
    codes: list[str],
    snapshot_date: date,
) -> dict[str, float | None]:
    if not codes:
        return {}
    placeholders = ",".join("?" for _ in codes)
    rows = connection.execute(
        f"""
        WITH latest AS (
            SELECT index_code, MAX(date) AS date
            FROM indicator_daily
            WHERE index_code IN ({placeholders})
              AND formula_version = ? AND date < ?
            GROUP BY index_code
        )
        SELECT latest.index_code, i.temperature
        FROM latest
        JOIN indicator_daily AS i
          ON i.index_code = latest.index_code
         AND i.date = latest.date
         AND i.formula_version = ?
        """,
        [*codes, FORMAL_VERSION, snapshot_date, FORMAL_VERSION],
    ).fetchall()
    return {str(code): safe_float(value) for code, value in rows}


def previous_snapshot_temperatures(
    connection: duckdb.DuckDBPyConnection,
    codes: list[str],
    snapshot_date: date,
    snapshot_time: str,
    formula_version: str,
) -> dict[str, float | None]:
    if not codes:
        return {}
    placeholders = ",".join("?" for _ in codes)
    rows = connection.execute(
        f"""
        WITH latest AS (
            SELECT index_code, MAX(snapshot_date) AS snapshot_date
            FROM intraday_snapshot
            WHERE index_code IN ({placeholders})
              AND snapshot_date < ?
              AND snapshot_time = ?
              AND formula_version = ?
            GROUP BY index_code
        )
        SELECT s.index_code, s.temperature_intraday
        FROM latest
        JOIN intraday_snapshot AS s
          ON s.index_code = latest.index_code
         AND s.snapshot_date = latest.snapshot_date
         AND s.snapshot_time = ?
         AND s.formula_version = ?
        """,
        [*codes, snapshot_date, snapshot_time, formula_version, snapshot_time, formula_version],
    ).fetchall()
    return {str(code): safe_float(value) for code, value in rows}


def row_to_db_tuple(row: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(row.get(column) for column in SNAPSHOT_COLUMNS)


def insert_snapshot_rows(
    connection: duckdb.DuckDBPyConnection,
    rows: list[dict[str, Any]],
    replace_existing: bool = False,
) -> int:
    if not rows:
        return 0
    before_counts = [] if replace_existing else None
    if before_counts is None:
        before_counts = []
        for row in rows:
            before_counts.append(
                int(
                    connection.execute(
                        """
                        SELECT COUNT(*) FROM intraday_snapshot
                        WHERE market = ? AND index_code = ? AND snapshot_date = ?
                          AND snapshot_time = ? AND formula_version = ?
                        """,
                        [row["market"], row["index_code"], row["snapshot_date"], row["snapshot_time"], row["formula_version"]],
                    ).fetchone()[0]
                )
            )
    connection.execute("BEGIN TRANSACTION")
    try:
        if replace_existing:
            for row in rows:
                connection.execute(
                    """
                    DELETE FROM intraday_snapshot
                    WHERE market = ? AND index_code = ? AND snapshot_date = ?
                      AND snapshot_time = ? AND formula_version = ?
                    """,
                    [row["market"], row["index_code"], row["snapshot_date"], row["snapshot_time"], row["formula_version"]],
                )
            before_counts = [0] * len(rows)
        values_sql = ",".join("?" for _ in SNAPSHOT_COLUMNS)
        connection.executemany(
            f"INSERT OR IGNORE INTO intraday_snapshot ({','.join(SNAPSHOT_COLUMNS)}) VALUES ({values_sql})",
            [row_to_db_tuple(row) for row in rows],
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    inserted = 0
    for before_count, row in zip(before_counts, rows):
        after_count = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM intraday_snapshot
                WHERE market = ? AND index_code = ? AND snapshot_date = ?
                  AND snapshot_time = ? AND formula_version = ?
                """,
                [row["market"], row["index_code"], row["snapshot_date"], row["snapshot_time"], row["formula_version"]],
            ).fetchone()[0]
        )
        inserted += max(0, after_count - before_count)
    return inserted


def output_paths(snapshot_date: date, mode: str, snapshot_time: str) -> dict[str, Path]:
    if mode == "test":
        directory = OUTPUT_ROOT / "test"
        stem = f"{snapshot_date.isoformat()}_intraday_test"
    else:
        directory = OUTPUT_ROOT / snapshot_time.replace(":", "")
        stem = f"{snapshot_date.isoformat()}_{snapshot_time.replace(':', '')}"
    return {
        "directory": directory,
        "html": directory / f"{stem}_temperature.html",
        "csv": directory / f"{stem}_temperature.csv",
        "summary": directory / f"{stem}_summary.json",
    }


def tone(value: float | None) -> str:
    if value is None:
        return "#8d97a6"
    if value < 20:
        return "#2a7995"
    if value < 40:
        return "#658caa"
    if value < 60:
        return "#687990"
    if value < 80:
        return "#c05a45"
    if value < 90:
        return "#c62828"
    return "#8b0000"


def display_number(value: Any, digits: int = 1) -> str:
    number = safe_float(value)
    return "NA" if number is None else f"{number:.{digits}f}"


def display_signed(value: Any, suffix: str = "") -> str:
    number = safe_float(value)
    return "NA" if number is None else f"{'+' if number > 0 else ''}{number:.1f}{suffix}"


def status_label(status: str | None) -> str:
    return {
        "ok": "实时有效",
        "temperature_pending": "待启用",
        "source_unavailable": "数据异常",
        "insufficient_data": "数据不足",
        "insufficient_realtime_input": "实时输入不足",
    }.get(status or "", status or "NA")


def render_html(
    rows: list[dict[str, Any]],
    registry_counts: dict[str, int],
    requested_time: str,
    actual_snapshot_time: datetime,
    mode: str,
    paths: dict[str, Path],
) -> dict[str, Any]:
    style_match = re.search(r"<style>(.*?)</style>", EXISTING_DASHBOARD_HTML, re.DOTALL)
    if not style_match:
        raise RuntimeError("无法提取现有 build_dashboard.py 的内嵌 CSS")
    style = style_match.group(1)
    extra_style = """
.intraday-notice{margin-top:18px;padding:13px 16px;border:1px solid #e7c4b9;border-radius:10px;background:#fff8f5;color:#8e4e3d;font-weight:700}.summary-panel{margin-top:14px;padding:16px 18px}.summary-grid{display:grid;grid-template-columns:repeat(6,minmax(0,1fr));gap:12px}.summary-item{min-width:0}.summary-item span{display:block;color:var(--muted);font-size:11px}.summary-item strong{display:block;margin-top:3px;font-size:17px;font-variant-numeric:tabular-nums}.summary-item small{display:block;color:var(--muted);font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.status-chip{display:inline-block;padding:2px 6px;border-radius:5px;background:#eef1f6;color:var(--muted);font-size:11px;font-weight:700}.status-chip.ok{background:#e9f5ee;color:#27734b}.status-chip.pending{background:#f3f0e4;color:#86702a}.status-chip.failed{background:#fff0ec;color:#a65039}.metric-list{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:13px;padding-top:12px;border-top:1px solid var(--line)}.metric-list span{display:block;color:var(--muted);font-size:11px}.metric-list b{display:block;margin-top:2px;font-size:13px;font-variant-numeric:tabular-nums}.pending-panel{margin-top:14px;padding:0}.pending-panel .panel-title{padding:17px 18px 0;margin-bottom:8px}.pending-note{color:var(--muted);font-size:12px;margin:5px 0 0}.rank-note{color:var(--muted);font-size:12px;margin:4px 0 0}.actual-time{display:block;margin-top:4px;color:var(--muted);font-size:11px}.data-reason{max-width:250px;overflow:hidden;text-overflow:ellipsis;color:var(--muted)}
@media(max-width:1100px){.summary-grid{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media(max-width:650px){.summary-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.metric-list{grid-template-columns:repeat(2,minmax(0,1fr))}}
"""
    rows = sorted(rows, key=lambda row: (row.get("status") != ACTIVE_STATUS, -(safe_float(row.get("temperature_intraday")) or -math.inf)))
    active_rows = [row for row in rows if row.get("status") == ACTIVE_STATUS]
    pending_rows = [row for row in rows if row.get("status") == PENDING_STATUS]
    valid_rows = [row for row in active_rows if safe_float(row.get("temperature_intraday")) is not None]
    high = max(valid_rows, key=lambda row: float(row["temperature_intraday"])) if valid_rows else None
    low = min(valid_rows, key=lambda row: float(row["temperature_intraday"])) if valid_rows else None
    rising = sum(1 for row in active_rows if (safe_float(row.get("change_vs_previous_close")) or 0) > 0)
    falling = sum(1 for row in active_rows if (safe_float(row.get("change_vs_previous_close")) or 0) < 0)
    high_text = f"{high['index_name']} {display_number(high['temperature_intraday'])}" if high else "NA"
    low_text = f"{low['index_name']} {display_number(low['temperature_intraday'])}" if low else "NA"
    title_label = "盘中测试快照" if mode == "test" else "盘中快照"
    date_label = actual_snapshot_time.strftime("%Y年%-m月%-d日") if os.name != "nt" else actual_snapshot_time.strftime("%Y年%m月%d日").replace("年0", "年").replace("月0", "月")

    def esc(value: Any) -> str:
        return html.escape("" if value is None else str(value), quote=True)

    def change_class(value: Any) -> str:
        number = safe_float(value)
        return "positive" if number is not None and number > 0 else "negative" if number is not None and number < 0 else "muted"

    def card(row: dict[str, Any]) -> str:
        temperature = safe_float(row.get("temperature_intraday"))
        position = "0" if temperature is None else f"{temperature:.1f}"
        opacity = "0" if temperature is None else "1"
        status = row.get("data_status")
        status_class = "ok" if status == "ok" else "pending" if status == "temperature_pending" else "failed"
        reason_html = ""
        if status == "source_unavailable" and row.get("failure_reason"):
            reason_html = f"<div class='data-reason'>原因：{esc(row.get('failure_reason'))}</div>"
        return f"""
        <article class="index-card" data-temperature="{esc(temperature if temperature is not None else -1)}">
          <div class="card-top"><div><div class="card-name">{esc(row['index_name'])}</div><div class="code">{esc(row['index_code'])}</div></div><div class="card-date">{esc(row.get('snapshot_date'))}</div></div>
          <div class="temperature" style="color:{tone(temperature)}">{display_number(temperature)}<small>/ 100</small></div>
          <div class="level" style="--tone:{tone(temperature)}">{esc(row.get('temperature_level') or 'NA')}</div>
          <div class="temp-track" style="--position:{position}%;--marker-opacity:{opacity};--tone:{tone(temperature)}"></div>
          <div class="changes"><strong class="{change_class(row.get('change_vs_previous_snapshot'))}">同时间 {display_signed(row.get('change_vs_previous_snapshot'))}</strong><span class="change-tag" style="--tag-bg:#f0f2f5;--tag-color:#66758a">收盘 {display_signed(row.get('change_vs_previous_close'))}</span></div>
          <div class="metric-list"><div><span>PriceMomentum</span><b>{display_number(row.get('PriceMomentum_score'))}</b></div><div><span>Volume</span><b>{display_number(row.get('Volume_score'))}</b></div><div><span>Breadth</span><b>{display_number(row.get('Breadth_score'))}</b></div><div><span>Sync</span><b>{display_number(row.get('Sync_score'))}</b></div><div><span>覆盖率</span><b>{display_number((safe_float(row.get('coverage_ratio')) or 0) * 100) if row.get('coverage_ratio') is not None else 'NA'}%</b></div><div><span>状态</span><b><span class="status-chip {status_class}">{esc(status_label(status))}</span></b></div></div>
          <div class="card-meta"><div><span>指数涨跌幅</span><b class="{change_class(row.get('index_pct_change'))}">{display_signed(row.get('index_pct_change'), '%')}</b></div><div><span>实时来源</span><b>{esc(row.get('source') or 'NA')}</b></div></div>
          {reason_html}
        </article>
        """

    def pending_row(row: dict[str, Any]) -> str:
        return f"<tr><td class='table-index'>{esc(row['index_name'])}<small>{esc(row['index_code'])}</small></td><td>{esc(row['market'])}</td><td><span class='status-chip pending'>待启用</span></td><td class='data-reason'>{esc(row.get('failure_reason') or 'temperature_pending')}</td></tr>"

    ranking_rows = []
    for position, row in enumerate(valid_rows, start=1):
        ranking_rows.append(
            f"<tr><td class='rank'>{position:02d}</td><td class='table-index'>{esc(row['index_name'])}<small>{esc(row['index_code'])}</small></td><td class='num'>{display_number(row.get('temperature_intraday'))}</td><td>{esc(row.get('temperature_level') or 'NA')}</td><td class='{change_class(row.get('index_pct_change'))}'>{display_signed(row.get('index_pct_change'), '%')}</td><td class='{change_class(row.get('change_vs_previous_snapshot'))}'>{display_signed(row.get('change_vs_previous_snapshot'))}</td><td class='{change_class(row.get('change_vs_previous_close'))}'>{display_signed(row.get('change_vs_previous_close'))}</td><td>{display_number((safe_float(row.get('coverage_ratio')) or 0) * 100) if row.get('coverage_ratio') is not None else 'NA'}%</td></tr>"
        )
    invalid_active_rows = [row for row in active_rows if row not in valid_rows]
    for row in invalid_active_rows:
        ranking_rows.append(
            f"<tr><td class='rank'>--</td><td class='table-index'>{esc(row['index_name'])}<small>{esc(row['index_code'])}</small></td><td class='num'>NA</td><td>NA</td><td>{display_signed(row.get('index_pct_change'), '%')}</td><td>NA</td><td>{display_signed(row.get('change_vs_previous_close'))}</td><td>{display_number((safe_float(row.get('coverage_ratio')) or 0) * 100) if row.get('coverage_ratio') is not None else 'NA'}%</td></tr>"
        )

    html_text = f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="color-scheme" content="light">
<title>指数温度计 · {esc(title_label)}</title>
<style>{style}{extra_style}</style>
</head>
<body>
<main class="shell">
  <header class="top"><div class="brand"><div class="eyebrow">MARKET OBSERVATION · INTRADAY</div><h1>指数温度计</h1><p class="subtitle">{esc(title_label)}</p></div><div class="top-right"><div class="snapshot"><strong>{esc(actual_snapshot_time.strftime('%Y-%m-%d'))} {esc(title_label)}</strong><span class="actual-time">实际采集时间 {esc(actual_snapshot_time.strftime('%H:%M:%S'))}</span><span class="actual-time">请求时间 {esc(requested_time)}</span></div></div></header>
  <div class="intraday-notice">盘中估算值，正式温度以收盘后 V1.1 为准。成交维度暂沿用上一交易日正式收盘值。</div>
  <div class="temperature-key" aria-label="温度等级区间"><div class="key-title"><strong>温度区间</strong><span>0—100</span></div><div class="key-track" aria-hidden="true"><span style="background:#2a7995"></span><span style="background:#4d94ad"></span><span style="background:#84a9b6"></span><span style="background:#a6aeb0"></span><span style="background:#cf8a6b"></span><span style="background:linear-gradient(90deg,#e05b43,#c62828)"></span><span style="background:linear-gradient(90deg,#c62828,#8b0000)"></span></div><div class="key-labels"><span>冰点</span><span>恐惧</span><span>偏冷</span><span>中性</span><span>偏热</span><span>贪婪</span><span>狂热</span></div></div>
  <section class="panel summary-panel" aria-label="盘中摘要"><div class="summary-grid"><div class="summary-item"><span>注册指数总数</span><strong>{registry_counts['registry_total']}</strong><small>active {registry_counts['active_count']} · pending {registry_counts['pending_count']}</small></div><div class="summary-item"><span>有效温度数量</span><strong>{len(valid_rows)}</strong><small>active 可计算结果</small></div><div class="summary-item"><span>最高温指数</span><strong>{esc(high_text)}</strong><small>按盘中温度</small></div><div class="summary-item"><span>最低温指数</span><strong>{esc(low_text)}</strong><small>按盘中温度</small></div><div class="summary-item"><span>升温数量</span><strong>{rising}</strong><small>相对上一交易日收盘</small></div><div class="summary-item"><span>降温数量</span><strong>{falling}</strong><small>相对上一交易日收盘</small></div></div></section>
  <section aria-labelledby="cards-heading"><div class="section-head"><div><h2 id="cards-heading">全部指数盘中温度</h2><p class="rank-note">active 指数按当前温度从高到低；首次即时测试没有上一交易日同时间快照，变化基准为上一交易日收盘。</p></div></div><div class="card-grid">{''.join(card(row) for row in active_rows)}</div></section>
  <section aria-labelledby="rank-heading"><div class="section-head"><div><h2 id="rank-heading">盘中温度排名</h2><p>同时间变化暂无时显示 NA；收盘变化使用上一交易日正式 V1.1。</p></div></div><div class="panel table-wrap"><table><thead><tr><th>排名</th><th>指数</th><th>当前温度</th><th>等级</th><th>指数涨跌幅</th><th>同时间变化</th><th>收盘变化</th><th>覆盖率</th></tr></thead><tbody>{''.join(ranking_rows)}</tbody></table></div></section>
  {f'''<section class="panel pending-panel" aria-labelledby="pending-heading"><div class="panel-title"><div><h2 id="pending-heading">待启用指数</h2><p class="pending-note">temperature_pending 不参与盘中温度计算和排名。</p></div></div><div class="table-wrap"><table><thead><tr><th>指数</th><th>市场</th><th>状态</th><th>说明</th></tr></thead><tbody>{''.join(pending_row(row) for row in pending_rows)}</tbody></table></div></section>''' if pending_rows else ''}
  <footer class="footer">1. 本页面为盘中温度估算；2. 今日价格、广度和同步使用同花顺盘中实时数据；3. 历史基准读取本地正式数据库；4. 成交维度当前暂沿用上一交易日正式收盘值，今天累计成交额仅作记录；5. 正式温度以收盘后 V1.1 结果为准；6. current_constituents_proxy 等现有模型限制继续保留；7. 页面不构成买入、卖出、加仓或减仓建议。</footer>
</main>
<script>
(() => {{
  const cards = document.querySelector('.card-grid');
  if (!cards) return;
  const children = Array.from(cards.children);
  children.forEach(card => card.addEventListener('click', () => card.classList.toggle('selected')));
}})();
</script>
</body>
</html>"""
    paths["html"].parent.mkdir(parents=True, exist_ok=True)
    paths["html"].write_text(html_text, encoding="utf-8")
    return {
        "path": str(paths["html"]),
        "bytes": paths["html"].stat().st_size,
        "external_resource_count": len(re.findall(r"<link\b|<script[^>]+src=|@import|url\(", html_text, re.IGNORECASE)),
        "replacement_character_count": html_text.count("�"),
        "title_date_label": date_label,
    }


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = __import__("csv").DictWriter(handle, fieldnames=list(SNAPSHOT_COLUMNS), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({column: "NA" if row.get(column) is None else iso_value(row.get(column)) for column in SNAPSHOT_COLUMNS})


def validate_formal_unchanged(
    connection: duckdb.DuckDBPyConnection,
    before: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    after = formal_baseline(connection)
    unchanged = before.get("formal_indicator_hash") == after.get("formal_indicator_hash")
    for table in ("index_daily", "stock_daily", "indicator_daily"):
        before_table = before[table]
        after_table = after[table]
        if before_table["row_count"] != after_table["row_count"]:
            unchanged = False
        if before_table["min_date"] != after_table["min_date"] or before_table["max_date"] != after_table["max_date"]:
            unchanged = False
        for field in ("close_sum", "volume_sum", "amount_sum"):
            left = before_table[field]
            right = after_table[field]
            if left is None or right is None:
                if left != right:
                    unchanged = False
            elif not math.isclose(left, right, rel_tol=1e-12, abs_tol=1e-6):
                unchanged = False
    return after, unchanged


def idempotent_outputs_exist(paths: dict[str, Path]) -> bool:
    return all(paths[name].is_file() for name in ("html", "csv", "summary"))


def read_existing_summary(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def run(args: argparse.Namespace) -> dict[str, Any]:
    if not DATABASE.is_file():
        raise FileNotFoundError(DATABASE)
    snapshot_date = date.fromisoformat(args.date) if args.date else now_local().date()
    paths = output_paths(snapshot_date, args.mode, args.time)
    formula_version = TEST_VERSION if args.mode == "test" else FORMAL_INTRADAY_VERSION
    with duckdb.connect(str(DATABASE), read_only=True) as connection:
        registry, registry_counts = load_registry(connection)
        baseline = formal_baseline(connection)
        has_snapshot_table = "intraday_snapshot" in [row[0] for row in connection.execute("SHOW TABLES").fetchall()]
        if not args.force and has_snapshot_table and idempotent_outputs_exist(paths):
            existing = connection.execute(
                """
                SELECT COUNT(*) FROM intraday_snapshot
                WHERE snapshot_date = ? AND snapshot_time = ? AND formula_version = ?
                """,
                [snapshot_date, args.time, formula_version],
            ).fetchone()[0]
            if existing >= len(registry):
                summary = read_existing_summary(paths["summary"]) or {}
                summary["idempotent_reused"] = True
                print(json.dumps(summary, ensure_ascii=False, indent=2, default=json_default))
                return summary

    backup_manifest = create_database_backup()
    actual_snapshot_time = now_local()
    api_key = read_api_key()
    client = HithinkClient(api_key)
    active = [item for item in registry if item["status"] == ACTIVE_STATUS]
    pending = [item for item in registry if item["status"] == PENDING_STATUS]
    active_codes = [item["index_code"] for item in active]
    local_codes = [*active_codes, "000300"]

    with duckdb.connect(str(DATABASE), read_only=True) as connection:
        index_history = load_index_history(connection, local_codes, snapshot_date)
        indicator_history = load_indicator_history(connection, active_codes, snapshot_date)
        previous_close = previous_formal_temperatures(connection, active_codes, snapshot_date)

    catalog: dict[str, str] = {}
    if any(str(item.get("source_symbol", "")).upper().startswith("SI.") for item in active):
        try:
            catalog = client.catalog()
        except HithinkError:
            catalog = {}

    resolved_symbols: dict[str, str | None] = {}
    symbol_failures: dict[str, str] = {}
    for item in active:
        symbol, reason = hithink_symbol_from_registry(item, catalog)
        resolved_symbols[item["index_code"]] = symbol
        if reason:
            symbol_failures[item["index_code"]] = reason

    index_quotes: dict[str, dict[str, Any]] = {}
    index_fetched: dict[str, datetime | None] = {}
    index_failures: dict[str, str] = {}
    for item in active:
        code = item["index_code"]
        symbol = resolved_symbols.get(code)
        if symbol is None:
            index_failures[code] = symbol_failures.get(code, "无法确定同花顺实时标的")
            continue
        try:
            quote, fetched_at = client.index_snapshot(symbol)
            index_quotes[code] = quote
            index_fetched[code] = fetched_at or actual_snapshot_time
        except HithinkError as exc:
            index_failures[code] = str(exc)

    benchmark_quote: dict[str, Any] | None = None
    benchmark_fetched: datetime | None = None
    ashare_active = [item for item in active if item["market"] == "Ashare" and item["index_code"] in index_quotes]
    if ashare_active:
        try:
            benchmark_quote, benchmark_fetched = client.index_snapshot(BENCHMARK_SYMBOL)
        except HithinkError:
            benchmark_quote = None
            benchmark_fetched = None

    members_by_index: dict[str, list[tuple[str, str, str]]] = {}
    constituent_failures: dict[str, str] = {}
    for item in ashare_active:
        code = item["index_code"]
        symbol = resolved_symbols.get(code)
        if symbol is None:
            constituent_failures[code] = symbol_failures.get(code, "无法确定同花顺成分标的")
            continue
        try:
            constituent_items, _ = client.constituents(symbol)
            normalized: list[tuple[str, str, str]] = []
            seen: set[tuple[str, str]] = set()
            for constituent in constituent_items:
                thscode = str(constituent.get("thscode", "")).upper()
                parsed = parse_stock_symbol(thscode)
                if parsed is None or parsed in seen:
                    continue
                seen.add(parsed)
                normalized.append((parsed[0], parsed[1], thscode))
            if not normalized:
                raise HithinkError("同花顺成分股无法解析为 A 股标的", empty=True)
            members_by_index[code] = normalized
        except HithinkError as exc:
            constituent_failures[code] = str(exc)

    stock_symbols = sorted({thscode for rows in members_by_index.values() for _, _, thscode in rows})
    stock_quotes, stock_quote_failures, stock_fetched = client.stock_snapshots(stock_symbols) if stock_symbols else ({}, {}, None)
    with duckdb.connect(str(DATABASE), read_only=True) as connection:
        stock_history = load_stock_history(connection, members_by_index, snapshot_date)
        previous_snapshots = {}
        if args.mode == "scheduled" and "intraday_snapshot" in [row[0] for row in connection.execute("SHOW TABLES").fetchall()]:
            previous_snapshots = previous_snapshot_temperatures(connection, active_codes, snapshot_date, args.time, formula_version)

    rows: list[dict[str, Any]] = []
    for item in active:
        code = item["index_code"]
        source_symbol = resolved_symbols.get(code)
        index_quote = index_quotes.get(code)
        if item["market"] == "HK":
            index_quote = None
            index_failure = "同花顺公开 A 股指数接口不支持港股盘中数据"
        else:
            index_failure = index_failures.get(code)
        row = calculate_index_row(
            item=item,
            source_symbol=source_symbol,
            index_quote=index_quote,
            index_fetched_at=index_fetched.get(code),
            benchmark_quote=benchmark_quote if item["market"] == "Ashare" else None,
            index_history=index_history.get(code),
            benchmark_history=index_history.get("000300"),
            indicator_history=indicator_history.get(code),
            members=members_by_index.get(code),
            stock_quotes=stock_quotes,
            stock_history=stock_history,
            snapshot_date=snapshot_date,
            requested_time=args.time,
            actual_snapshot_time=actual_snapshot_time,
            formula_version=formula_version,
            previous_close_temperature=previous_close.get(code),
            previous_snapshot_temperature=previous_snapshots.get(code),
            constituent_failure=constituent_failures.get(code) or index_failure,
        )
        row["status"] = item["status"]
        if index_failure and row["data_status"] != "ok":
            row["data_status"] = "source_unavailable"
            row["failure_reason"] = index_failure
        rows.append(row)

    for item in pending:
        row = base_snapshot_row(
            item,
            snapshot_date,
            args.time,
            actual_snapshot_time,
            formula_version,
            None,
        )
        row["data_status"] = "temperature_pending"
        row["status"] = item["status"]
        row["failure_reason"] = "index_registry.status=temperature_pending，不参与盘中温度计算和排名"
        rows.append(row)

    paths["directory"].mkdir(parents=True, exist_ok=True)
    write_csv(rows, paths["csv"])
    html_info = render_html(rows, registry_counts, args.time, actual_snapshot_time, args.mode, paths)

    with duckdb.connect(str(DATABASE)) as connection:
        ensure_snapshot_table(connection)
        inserted_rows = insert_snapshot_rows(connection, rows, replace_existing=args.force)
        after_baseline, formal_unchanged = validate_formal_unchanged(connection, baseline)

    active_realtime_success = sum(1 for code in active_codes if code in index_quotes and code not in symbol_failures)
    source_unavailable_count = sum(1 for row in rows if row.get("data_status") == "source_unavailable")
    temperature_success_count = sum(1 for row in rows if row.get("status") != PENDING_STATUS and row.get("temperature_intraday") is not None)
    na_fields = {
        row["index_code"]: [column for column in SNAPSHOT_COLUMNS if row.get(column) is None]
        for row in rows
        if any(row.get(column) is None for column in SNAPSHOT_COLUMNS)
    }
    summary: dict[str, Any] = {
        "mode": args.mode,
        "snapshot_date": snapshot_date,
        "requested_time": args.time,
        "actual_snapshot_time": actual_snapshot_time,
        "formula_version": formula_version,
        "source": "hithink",
        "registry": registry_counts,
        "active_realtime_success_count": active_realtime_success,
        "source_unavailable_count": source_unavailable_count,
        "temperature_success_count": temperature_success_count,
        "pending_not_calculated_count": len(pending),
        "intraday_snapshot_inserted_rows": inserted_rows,
        "today_stock_snapshot_count": len(stock_quotes),
        "today_stock_snapshot_failure_count": len(stock_quote_failures),
        "html": html_info,
        "outputs": {key: str(value) for key, value in paths.items() if key != "directory"},
        "backup": backup_manifest,
        "formal_baseline_before": baseline,
        "formal_baseline_after": after_baseline,
        "formal_data_unchanged": formal_unchanged,
        "na_fields": na_fields,
        "indices": [
            {
                "index_code": row["index_code"],
                "index_name": row["index_name"],
                "market": row["market"],
                "status": next(item["status"] for item in registry if item["index_code"] == row["index_code"]),
                "data_status": row["data_status"],
                "coverage_ratio": row["coverage_ratio"],
                "temperature_intraday": row["temperature_intraday"],
                "previous_close_temperature": row["previous_close_temperature"],
                "change_vs_previous_close": row["change_vs_previous_close"],
                "previous_snapshot_temperature": row["previous_snapshot_temperature"],
                "change_vs_previous_snapshot": row["change_vs_previous_snapshot"],
                "failure_reason": row["failure_reason"],
            }
            for row in rows
        ],
    }
    paths["summary"].write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=json_default),
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=json_default))
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="指数温度计盘中快照统一入口")
    parser.add_argument("--time", required=True, choices=("13:30", "14:15"), help="请求时间标签")
    parser.add_argument("--mode", required=True, choices=("test", "scheduled"), help="运行模式")
    parser.add_argument("--date", help="测试日期，默认使用 Asia/Shanghai 当前日期")
    parser.add_argument("--force", action="store_true", help="忽略幂等缓存并重新请求实时数据")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.mode == "test" and args.time != "13:30":
        raise SystemExit("test 模式只允许使用 --time 13:30")
    if args.mode == "scheduled" and args.time != "14:15":
        raise SystemExit("scheduled 模式只允许使用 --time 14:15")
    run(args)


if __name__ == "__main__":
    main()
