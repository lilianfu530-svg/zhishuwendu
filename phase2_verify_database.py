from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parent
DATABASE = ROOT / "data" / "market.duckdb"
BACKUP = ROOT / "backup" / "phase2_before_migration_20260919" / "market.duckdb"
OUTPUT = ROOT / "data" / "phase2_database_verification.json"
OLD_CODES = ("399006", "930986")
NEW_CODES = ("000001", "000688", "899050", "932000", "HSTECH", "931787")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    with duckdb.connect(str(DATABASE), read_only=True) as connection:
        connection.execute(f"ATTACH '{BACKUP.as_posix()}' AS original (READ_ONLY)")
        columns = [column[0] for column in connection.execute("SELECT * FROM original.indicator_daily LIMIT 0").description]
        projection = ", ".join(f'"{column}"' for column in columns)
        frozen = []
        for code in OLD_CODES:
            for version in ("V1.0", "V1.1"):
                params = [code, version, code, version]
                forward = connection.execute(
                    f"SELECT COUNT(*) FROM (SELECT {projection} FROM indicator_daily WHERE index_code = ? AND formula_version = ? "
                    f"EXCEPT ALL SELECT {projection} FROM original.indicator_daily WHERE index_code = ? AND formula_version = ?)",
                    params,
                ).fetchone()[0]
                backward = connection.execute(
                    f"SELECT COUNT(*) FROM (SELECT {projection} FROM original.indicator_daily WHERE index_code = ? AND formula_version = ? "
                    f"EXCEPT ALL SELECT {projection} FROM indicator_daily WHERE index_code = ? AND formula_version = ?)",
                    params,
                ).fetchone()[0]
                days, valid = connection.execute(
                    "SELECT COUNT(*), COUNT(temperature) FROM indicator_daily WHERE index_code = ? AND formula_version = ?",
                    [code, version],
                ).fetchone()
                frozen.append({"index_code": code, "formula_version": version,
                               "rows": days, "valid_temperature": valid,
                               "changed_rows_forward": forward, "changed_rows_backward": backward})
        old_stock_columns = [column[0] for column in connection.execute("SELECT * FROM original.stock_daily LIMIT 0").description]
        stock_projection = ", ".join(f'"{column}"' for column in old_stock_columns)
        old_stock_missing = connection.execute(
            f"SELECT COUNT(*) FROM (SELECT {stock_projection} FROM original.stock_daily "
            f"EXCEPT ALL SELECT {stock_projection} FROM stock_daily)"
        ).fetchone()[0]
        duplicate_checks = {
            "stock_daily": connection.execute(
                "SELECT COUNT(*) - COUNT(DISTINCT (market, stock_code, date)) FROM stock_daily"
            ).fetchone()[0],
            "index_daily": connection.execute(
                "SELECT COUNT(*) - COUNT(DISTINCT (index_code, date)) FROM index_daily"
            ).fetchone()[0],
            "indicator_daily": connection.execute(
                "SELECT COUNT(*) - COUNT(DISTINCT (index_code, date, formula_version)) FROM indicator_daily"
            ).fetchone()[0],
        }
        markets = connection.execute("SELECT DISTINCT market FROM stock_daily ORDER BY market").fetchall()
        additions = []
        for code in NEW_CODES:
            index_rows = connection.execute("SELECT COUNT(*) FROM index_daily WHERE index_code = ?", [code]).fetchone()[0]
            indicator_rows = connection.execute("SELECT COUNT(*) FROM indicator_daily WHERE index_code = ?", [code]).fetchone()[0]
            additions.append({"index_code": code, "index_rows": index_rows, "indicator_rows": indicator_rows})
        old_stock_count = connection.execute("SELECT COUNT(*) FROM original.stock_daily").fetchone()[0]
        stock_count = connection.execute("SELECT COUNT(*) FROM stock_daily").fetchone()[0]
    result = {
        "verified_at": datetime.now().isoformat(),
        "backup_path": str(BACKUP), "backup_sha256": sha256(BACKUP),
        "original_stock_rows": old_stock_count, "current_stock_rows": stock_count,
        "original_stock_rows_missing_or_changed": old_stock_missing,
        "stock_markets": [row[0] for row in markets],
        "frozen_indicator_checks": frozen,
        "duplicate_primary_keys": duplicate_checks,
        "new_index_persistence": additions,
    }
    OUTPUT.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    assert all(row["rows"] == 1143 and row["valid_temperature"] == 872 and
               row["changed_rows_forward"] == 0 and row["changed_rows_backward"] == 0 for row in frozen)
    assert old_stock_missing == 0
    assert all(value == 0 for value in duplicate_checks.values())
    assert {row[0] for row in markets} == {"SH", "SZ", "BJ", "HK"}
    assert all(row["index_rows"] > 0 and row["indicator_rows"] == 2 * row["index_rows"] for row in additions)
    print(json.dumps({"frozen_checks_passed": True, "old_stock_preserved": True,
                      "duplicate_primary_keys": duplicate_checks, "new_indices": len(additions),
                      "backup_sha256": result["backup_sha256"]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
