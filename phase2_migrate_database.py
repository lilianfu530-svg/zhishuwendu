from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parent
DATABASE = ROOT / "data" / "market.duckdb"
BACKUP = ROOT / "backup" / "phase2_before_migration_20260919" / "market.duckdb"
MANIFEST = BACKUP.with_name("backup_manifest.json")
EXCHANGE_MARKET = {
    "上海证券交易所": "SH",
    "深圳证券交易所": "SZ",
    "北京证券交易所": "BJ",
    "香港交易所": "HK",
}
STOCK_COLUMNS = [
    "stock_code", "date", "open", "high", "low", "close", "pct_change",
    "volume", "amount", "source", "fetched_at",
]


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def old_indicator_baseline(connection: duckdb.DuckDBPyConnection) -> list[dict]:
    rows = connection.execute(
        """
        SELECT index_code, formula_version, COUNT(*) AS row_count,
               COUNT(temperature) AS valid_temperature_count,
               MIN(date) AS start_date, MAX(date) AS end_date
        FROM indicator_daily
        WHERE index_code IN ('399006', '930986')
        GROUP BY 1, 2 ORDER BY 1, 2
        """
    ).fetchall()
    return [
        {"index_code": code, "formula_version": version, "row_count": count,
         "valid_temperature_count": valid, "start_date": str(start), "end_date": str(end)}
        for code, version, count, valid, start, end in rows
    ]


def main() -> None:
    if not BACKUP.exists():
        raise RuntimeError("迁移前备份不存在，停止")
    before_hash = sha256(DATABASE)
    backup_hash = sha256(BACKUP)
    if before_hash != backup_hash:
        raise RuntimeError("备份与正式库 SHA256 不一致，停止")
    with duckdb.connect(str(DATABASE)) as connection:
        columns = {row[1] for row in connection.execute("PRAGMA table_info('stock_daily')").fetchall()}
        if "market" in columns:
            raise RuntimeError("stock_daily 已迁移，本脚本不得重复执行")
        baseline = old_indicator_baseline(connection)
        if len(baseline) != 4 or any(row["row_count"] != 1143 or row["valid_temperature_count"] != 872 for row in baseline):
            raise RuntimeError("既有指数基线数量与确认值不符，停止")
        source_count = connection.execute("SELECT COUNT(*) FROM stock_daily").fetchone()[0]
        exchange_rows = connection.execute(
            """
            SELECT DISTINCT s.stock_code, c.exchange
            FROM stock_daily AS s
            LEFT JOIN index_constituents AS c ON c.stock_code = s.stock_code
            ORDER BY 1, 2
            """
        ).fetchall()
        markets: dict[str, str] = {}
        for code, exchange in exchange_rows:
            market = EXCHANGE_MARKET.get(exchange)
            if market is None or (code in markets and markets[code] != market):
                raise RuntimeError(f"旧股票市场无法唯一确认：{code}；停止，不猜测")
            markets[code] = market
        unique_codes = connection.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_daily").fetchone()[0]
        if len(markets) != unique_codes:
            raise RuntimeError("旧股票市场映射数量不完整，停止")
        mapping = pd.DataFrame([{"stock_code": code, "market": market} for code, market in markets.items()])
        connection.register("verified_stock_market", mapping)
        try:
            connection.execute("BEGIN TRANSACTION")
            connection.execute(
                """
                CREATE TABLE stock_daily_market_key (
                    market VARCHAR NOT NULL CHECK (market IN ('SH', 'SZ', 'BJ', 'HK')),
                    stock_code VARCHAR NOT NULL,
                    date DATE NOT NULL,
                    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
                    pct_change DOUBLE, volume DOUBLE, amount DOUBLE,
                    source VARCHAR, fetched_at TIMESTAMP,
                    PRIMARY KEY (market, stock_code, date)
                )
                """
            )
            connection.execute(
                """
                INSERT INTO stock_daily_market_key
                SELECT m.market, s.stock_code, s.date, s.open, s.high, s.low, s.close,
                       s.pct_change, s.volume, s.amount, s.source, s.fetched_at
                FROM stock_daily AS s
                JOIN verified_stock_market AS m USING (stock_code)
                """
            )
            copied = connection.execute("SELECT COUNT(*) FROM stock_daily_market_key").fetchone()[0]
            if copied != source_count:
                raise RuntimeError(f"旧股票复制不完整：{copied}/{source_count}")
            connection.execute("DROP TABLE stock_daily")
            connection.execute("ALTER TABLE stock_daily_market_key RENAME TO stock_daily")
            connection.execute("ALTER TABLE index_constituents ADD COLUMN market VARCHAR")
            for exchange, market in EXCHANGE_MARKET.items():
                connection.execute("UPDATE index_constituents SET market = ? WHERE exchange = ?", [market, exchange])
            unknown = connection.execute("SELECT COUNT(*) FROM index_constituents WHERE market IS NULL").fetchone()[0]
            if unknown:
                raise RuntimeError(f"旧成分市场仍有 {unknown} 条无法确认")
            for field, kind in (
                ("valid_constituent_count", "INTEGER"),
                ("total_constituent_count", "INTEGER"),
                ("coverage_ratio", "DOUBLE"),
            ):
                connection.execute(f"ALTER TABLE indicator_daily ADD COLUMN {field} {kind}")
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.unregister("verified_stock_market")
    manifest = {
        "backup_path": str(BACKUP), "backup_sha256": backup_hash,
        "source_sha256_before_migration": before_hash,
        "migrated_at": datetime.now().isoformat(timespec="seconds"),
        "old_stock_rows": source_count, "old_stock_count": unique_codes,
        "old_indicator_baseline": baseline,
    }
    MANIFEST.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(manifest, ensure_ascii=False))


if __name__ == "__main__":
    main()
