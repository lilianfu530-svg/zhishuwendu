from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import date, datetime
from io import BytesIO
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import requests


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "output"
DATABASE_PATH = DATA_DIR / "market.duckdb"
PROJECT_PYTHON = ROOT / ".venv" / "Scripts" / "python.exe"

INDEX_CODES = ("399006", "930986")
SCORE_COLUMNS = [
    "RET20_score",
    "BIAS20_score",
    "RS20_score",
    "VolumeStrength_score",
    "BreadthMA20_score",
    "HLBreadth_score",
    "Sync_score",
]
LEVEL_ORDER = ["冰点", "恐惧", "偏冷", "中性", "偏热", "贪婪", "狂热"]


def json_default(value: object) -> object:
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    if pd.isna(value):
        return None
    raise TypeError(f"无法序列化：{type(value).__name__}")


def write_json(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=json_default),
        encoding="utf-8",
    )


def indicator_schema_sql(table_name: str) -> str:
    return f"""
        CREATE TABLE {table_name} (
            index_code VARCHAR NOT NULL,
            date DATE NOT NULL,
            formula_version VARCHAR NOT NULL DEFAULT 'V1.0',
            constituent_mode VARCHAR NOT NULL DEFAULT 'current_constituents_proxy',
            RET20_raw DOUBLE,
            RET20_score DOUBLE,
            BIAS20_raw DOUBLE,
            BIAS20_score DOUBLE,
            RS20_raw DOUBLE,
            RS20_score DOUBLE,
            VolumeStrength_raw DOUBLE,
            VolumeStrength_score DOUBLE,
            BreadthMA20_raw DOUBLE,
            BreadthMA20_score DOUBLE,
            HLBreadth_raw DOUBLE,
            HLBreadth_score DOUBLE,
            Sync_raw DOUBLE,
            Sync_score DOUBLE,
            temperature DOUBLE,
            temperature_change_1d DOUBLE,
            temperature_level VARCHAR,
            calculated_at TIMESTAMP,
            PRIMARY KEY (index_code, date, formula_version)
        )
    """


def migrate_indicator_daily(connection: duckdb.DuckDBPyConnection) -> dict[str, object]:
    """使用单一事务迁移主键，并在提交前后逐值核对温度。"""
    before = connection.execute(
        """
        SELECT index_code, date, temperature, temperature_change_1d, temperature_level
        FROM indicator_daily ORDER BY index_code, date
        """
    ).df()
    before_counts = {
        code: int((before["index_code"] == code).sum()) for code in INDEX_CODES
    }
    columns = {
        row[0] for row in connection.execute("DESCRIBE indicator_daily").fetchall()
    }
    migrated = "formula_version" not in columns or "constituent_mode" not in columns
    if migrated:
        connection.execute("BEGIN TRANSACTION")
        try:
            connection.execute("DROP TABLE IF EXISTS indicator_daily_versioned")
            connection.execute(indicator_schema_sql("indicator_daily_versioned"))
            connection.execute(
                """
                INSERT INTO indicator_daily_versioned
                SELECT index_code, date, 'V1.0', 'current_constituents_proxy',
                       RET20_raw, RET20_score, BIAS20_raw, BIAS20_score,
                       RS20_raw, RS20_score, VolumeStrength_raw, VolumeStrength_score,
                       BreadthMA20_raw, BreadthMA20_score, HLBreadth_raw,
                       HLBreadth_score, Sync_raw, Sync_score, temperature,
                       temperature_change_1d, temperature_level, calculated_at
                FROM indicator_daily
                """
            )
            connection.execute("DROP TABLE indicator_daily")
            connection.execute(
                "ALTER TABLE indicator_daily_versioned RENAME TO indicator_daily"
            )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise

    after = connection.execute(
        """
        SELECT index_code, date, temperature, temperature_change_1d, temperature_level
        FROM indicator_daily WHERE formula_version = 'V1.0'
        ORDER BY index_code, date
        """
    ).df()
    after_counts = {
        code: int((after["index_code"] == code).sum()) for code in INDEX_CODES
    }
    duplicate_count = int(
        connection.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT index_code, date, formula_version, COUNT(*) AS n
                FROM indicator_daily
                GROUP BY index_code, date, formula_version
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
    )
    merged = before.merge(
        after,
        on=["index_code", "date"],
        how="outer",
        suffixes=("_before", "_after"),
        indicator=True,
    )
    temperatures_equal = bool(
        merged["_merge"].eq("both").all()
        and np.allclose(
            merged["temperature_before"],
            merged["temperature_after"],
            equal_nan=True,
            rtol=0,
            atol=0,
        )
        and np.allclose(
            merged["temperature_change_1d_before"],
            merged["temperature_change_1d_after"],
            equal_nan=True,
            rtol=0,
            atol=0,
        )
        and merged["temperature_level_before"].fillna("<NA>").equals(
            merged["temperature_level_after"].fillna("<NA>")
        )
    )
    modes = connection.execute(
        """
        SELECT formula_version, constituent_mode, COUNT(*)
        FROM indicator_daily GROUP BY formula_version, constituent_mode
        ORDER BY formula_version, constituent_mode
        """
    ).fetchall()
    checks = {
        "399006_1142_rows": after_counts.get("399006") == 1142,
        "930986_1143_rows": after_counts.get("930986") == 1143,
        "temperature_unchanged": temperatures_equal,
        "duplicate_key_count": duplicate_count,
        "v1_count_unchanged": before_counts == after_counts,
    }
    if not all(
        [
            checks["399006_1142_rows"],
            checks["930986_1143_rows"],
            checks["temperature_unchanged"],
            checks["v1_count_unchanged"],
            duplicate_count == 0,
        ]
    ):
        raise RuntimeError(f"指标表迁移验证失败：{checks}")
    return {
        "schema_migrated": migrated,
        "before_counts": before_counts,
        "after_counts": after_counts,
        "version_modes": [
            {"formula_version": row[0], "constituent_mode": row[1], "rows": row[2]}
            for row in modes
        ],
        "checks": checks,
    }


def create_constituent_history_table(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS index_constituent_history (
            index_code VARCHAR NOT NULL,
            stock_code VARCHAR NOT NULL,
            stock_name VARCHAR,
            effective_from DATE NOT NULL,
            effective_to DATE,
            source_type VARCHAR NOT NULL,
            source_url VARCHAR,
            coverage_status VARCHAR NOT NULL,
            fetched_at TIMESTAMP,
            PRIMARY KEY (index_code, stock_code, effective_from, source_type)
        )
        """
    )
    connection.execute(
        """
        INSERT OR IGNORE INTO index_constituent_history
        SELECT index_code, stock_code, stock_name, effective_date, NULL,
               'current_snapshot', source, 'incomplete_history', fetched_at
        FROM index_constituents
        """
    )


def http_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    return session


def probe_constituent_sources() -> dict[str, object]:
    """只记录官方源可验证的覆盖，不根据零散公告猜测持仓。"""
    session = http_session()
    cni_adjust_url = "https://www.cnindex.com.cn/sample-detail/download-adjustment"
    cni_history_url = "https://www.cnindex.com.cn/sample-detail/download-history"
    csi_current_url = (
        "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/"
        "file/autofile/closeweight/930986closeweight.xls"
    )
    csi_methodology_url = (
        "https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/indices/"
        "detail/files/zh_CN/20231208180232-930986_Index_Methodology_cn.pdf"
    )

    adjust_rows = 0
    adjust_status = None
    try:
        response = session.get(cni_adjust_url, params={"indexcode": "399006"}, timeout=30)
        adjust_status = response.status_code
        if response.ok:
            try:
                adjust_rows = len(pd.read_excel(BytesIO(response.content)))
            except Exception:  # 官方空响应并非有效Excel
                adjust_rows = 0
    except Exception:
        adjust_status = None

    history_rows = 0
    history_dates: list[str] = []
    try:
        response = session.get(cni_history_url, params={"indexcode": "399006"}, timeout=30)
        if response.ok:
            frame = pd.read_excel(BytesIO(response.content))
            history_rows = len(frame)
            if not frame.empty:
                dates = pd.to_datetime(frame.iloc[:, 0], errors="coerce").dropna().unique()
                history_dates = sorted(pd.Timestamp(value).strftime("%Y-%m-%d") for value in dates)
    except Exception:
        pass

    csi_rows = 0
    csi_dates: list[str] = []
    try:
        response = session.get(csi_current_url, timeout=30)
        if response.ok:
            frame = pd.read_excel(BytesIO(response.content))
            csi_rows = len(frame)
            if not frame.empty:
                dates = pd.to_datetime(frame.iloc[:, 0].astype(str), format="%Y%m%d", errors="coerce")
                csi_dates = sorted(
                    pd.Timestamp(value).strftime("%Y-%m-%d")
                    for value in dates.dropna().unique()
                )
    except Exception:
        pass

    return {
        "399006": {
            "point_in_time_available": False,
            "constituent_mode": "current_constituents_proxy",
            "official_adjustment_endpoint": cni_adjust_url,
            "adjustment_http_status": adjust_status,
            "adjustment_rows": adjust_rows,
            "official_history_endpoint": cni_history_url,
            "history_rows": history_rows,
            "history_snapshot_dates": history_dates,
            "official_notice_archive": "https://www.cnindex.com.cn/zh_information/notices_news/",
            "reason": (
                "历史调样下载接口无有效记录；历史样本接口仅返回单一最新快照。"
                "官方公告库可找到部分定期调样公告，但无法从一个连续档案验证"
                "过去约3年所有定期及临时变更，不满足连续、可靠重建条件。"
            ),
        },
        "930986": {
            "point_in_time_available": False,
            "constituent_mode": "current_constituents_proxy",
            "official_current_endpoint": csi_current_url,
            "current_rows": csi_rows,
            "current_snapshot_dates": csi_dates,
            "official_methodology": csi_methodology_url,
            "reason": (
                "中证官方公开接口只提供当前成分及权重；编制方案可确认半年调样频率，"
                "但未取得覆盖过去约3年的连续历史成分或全部调入调出档案。"
            ),
        },
    }


def diagnose_399006_date() -> dict[str, object]:
    previous_summary_path = OUTPUT_DIR / "run_summary.json"
    previous_summary = {}
    if previous_summary_path.exists():
        previous_summary = json.loads(previous_summary_path.read_text(encoding="utf-8"))
    session = http_session()
    url = "http://hq.cnindex.com.cn/market/market/getIndexDailyDataWithDataFormat"
    tests = []
    for start_date, end_date in [
        ("2026-09-18", "2026-09-18"),
        ("2026-09-17", "2026-09-18"),
        ("2026-09-18", "2026-09-19"),
    ]:
        response = session.get(
            url,
            params={
                "indexCode": "399006",
                "startDate": start_date,
                "endDate": end_date,
                "frequency": "day",
            },
            timeout=30,
        )
        payload = response.json()
        rows = payload.get("data", {}).get("data", [])
        tests.append(
            {
                "start_date": start_date,
                "end_date": end_date,
                "http_status": response.status_code,
                "row_count": len(rows),
                "returned_dates": [str(row[0]) for row in rows],
            }
        )
    exact_has_target = any(
        test["start_date"] == "2026-09-18"
        and "2026-09-18" in test["returned_dates"]
        for test in tests
    )
    previous_latest = previous_summary.get("各指数更新后最新日期", {}).get("399006")
    return {
        "classification": "A",
        "classification_text": "数据源当时尚未更新",
        "previous_recorded_latest": previous_latest,
        "official_target_now_available": exact_has_target,
        "official_endpoint": url,
        "small_range_tests": tests,
        "reason": (
            "2026-09-18当晚项目记录的官方最新日期仍为2026-09-17；"
            "现在使用相同单日参数可直接返回2026-09-18。"
            "日期逻辑未变即能取得，因此排除程序截止日问题，属于官方发布时滞。"
        ),
    }


def choose_version(connection: duckdb.DuckDBPyConnection, code: str) -> tuple[str, str]:
    pit_count = int(
        connection.execute(
            "SELECT COUNT(*) FROM indicator_daily WHERE index_code = ? AND formula_version = 'V1.0-PIT'",
            [code],
        ).fetchone()[0]
    )
    if pit_count:
        return "V1.0-PIT", "point_in_time"
    return "V1.0", "current_constituents_proxy"


def correlation_payload(connection: duckdb.DuckDBPyConnection) -> dict[str, object]:
    payload: dict[str, object] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "indices": {},
    }
    for code in INDEX_CODES:
        version, mode = choose_version(connection, code)
        query_columns = ", ".join(SCORE_COLUMNS)
        frame = connection.execute(
            f"""
            SELECT date, {query_columns}
            FROM indicator_daily
            WHERE index_code = ? AND formula_version = ?
            ORDER BY date
            """,
            [code, version],
        ).df()
        valid = frame.dropna(subset=SCORE_COLUMNS).copy()
        pearson = valid[SCORE_COLUMNS].corr(method="pearson")
        spearman = valid[SCORE_COLUMNS].corr(method="spearman")
        high_pairs = []
        for method, matrix in (("Pearson", pearson), ("Spearman", spearman)):
            for left_position, left in enumerate(SCORE_COLUMNS):
                for right in SCORE_COLUMNS[left_position + 1 :]:
                    value = float(matrix.loc[left, right])
                    if abs(value) >= 0.80:
                        high_pairs.append(
                            {
                                "method": method,
                                "indicator_1": left,
                                "indicator_2": right,
                                "correlation": value,
                            }
                        )
        payload["indices"][code] = {
            "formula_version": version,
            "constituent_mode": mode,
            "start_date": valid["date"].min().strftime("%Y-%m-%d"),
            "end_date": valid["date"].max().strftime("%Y-%m-%d"),
            "valid_rows": len(valid),
            "columns": SCORE_COLUMNS,
            "pearson": pearson.to_numpy().tolist(),
            "spearman": spearman.to_numpy().tolist(),
            "high_pairs": high_pairs,
        }
    return payload


def add_forward_metrics(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    close = result["close"].to_numpy(dtype=float)
    for horizon in (5, 10, 20):
        result[f"forward_{horizon}d"] = result["close"].shift(-horizon) / result["close"] - 1
    drawdown = np.full(len(result), np.nan)
    upside = np.full(len(result), np.nan)
    for position in range(len(result) - 20):
        future = close[position + 1 : position + 21]
        if len(future) == 20 and np.isfinite(future).all() and np.isfinite(close[position]):
            drawdown[position] = future.min() / close[position] - 1
            upside[position] = future.max() / close[position] - 1
    result["future20_max_drawdown"] = drawdown
    result["future20_max_upside"] = upside
    return result


def safe_float(value: object) -> float | None:
    return None if pd.isna(value) else float(value)


def temperature_validation_payload(
    connection: duckdb.DuckDBPyConnection,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "forward_return_definition": "Close(t+N) / Close(t) - 1",
        "future20_max_drawdown_definition": "min(Close[t+1:t+20]) / Close(t) - 1",
        "future20_max_upside_definition": "max(Close[t+1:t+20]) / Close(t) - 1",
        "indices": {},
        "summary": [],
    }
    for code in INDEX_CODES:
        version, mode = choose_version(connection, code)
        frame = connection.execute(
            """
            SELECT i.date, i.temperature, i.temperature_level, d.close
            FROM indicator_daily AS i
            JOIN index_daily AS d
              ON i.index_code = d.index_code AND i.date = d.date
            WHERE i.index_code = ? AND i.formula_version = ?
            ORDER BY i.date
            """,
            [code, version],
        ).df()
        frame = add_forward_metrics(frame)
        valid_temperature = frame.dropna(subset=["temperature"]).copy()
        performance_rows = []
        level_rows = []
        for level in LEVEL_ORDER:
            group = valid_temperature[valid_temperature["temperature_level"] == level]
            row: dict[str, object] = {
                "温度区间": level,
                "温度样本数": int(len(group)),
            }
            for horizon in (5, 10, 20):
                series = group[f"forward_{horizon}d"].dropna()
                row[f"未来{horizon}日样本数"] = int(len(series))
                row[f"未来{horizon}日平均收益"] = safe_float(series.mean())
                row[f"未来{horizon}日中位收益"] = safe_float(series.median())
                row[f"未来{horizon}日上涨比例"] = safe_float((series > 0).mean())
            drawdown = group["future20_max_drawdown"].dropna()
            upside = group["future20_max_upside"].dropna()
            row["未来20日平均最大回撤"] = safe_float(drawdown.mean())
            row["未来20日平均最大上涨"] = safe_float(upside.mean())
            performance_rows.append(row)
            level_rows.append(
                {
                    "温度区间": level,
                    "出现次数": int(len(group)),
                    "出现占比": safe_float(len(group) / len(valid_temperature))
                    if len(valid_temperature)
                    else None,
                }
            )

        temperature = valid_temperature["temperature"]
        distribution = {
            "平均值": safe_float(temperature.mean()),
            "中位数": safe_float(temperature.median()),
            "标准差": safe_float(temperature.std(ddof=1)),
            "P5": safe_float(temperature.quantile(0.05)),
            "P10": safe_float(temperature.quantile(0.10)),
            "P25": safe_float(temperature.quantile(0.25)),
            "P50": safe_float(temperature.quantile(0.50)),
            "P75": safe_float(temperature.quantile(0.75)),
            "P90": safe_float(temperature.quantile(0.90)),
            "P95": safe_float(temperature.quantile(0.95)),
            "最小值": safe_float(temperature.min()),
            "最大值": safe_float(temperature.max()),
        }
        largest_level = max(level_rows, key=lambda item: item["出现次数"])
        bias_note = (
            f"最高频区间为{largest_level['温度区间']}，"
            f"占比{largest_level['出现占比']:.1%}。"
        )
        if largest_level["出现占比"] >= 0.80:
            bias_note += "单一区间占比达到80%，存在明显集中偏置。"
        else:
            bias_note += "未见单一区间占比达到80%。"
        payload["indices"][code] = {
            "formula_version": version,
            "constituent_mode": mode,
            "start_date": valid_temperature["date"].min().strftime("%Y-%m-%d"),
            "end_date": valid_temperature["date"].max().strftime("%Y-%m-%d"),
            "valid_temperature_rows": int(len(valid_temperature)),
            "performance": performance_rows,
            "distribution_statistics": distribution,
            "level_distribution": level_rows,
            "bias_note": bias_note,
        }
        payload["summary"].append(
            {
                "指数代码": code,
                "formula_version": version,
                "constituent_mode": mode,
                "有效温度样本": int(len(valid_temperature)),
                "开始日期": valid_temperature["date"].min().strftime("%Y-%m-%d"),
                "结束日期": valid_temperature["date"].max().strftime("%Y-%m-%d"),
                "平均温度": distribution["平均值"],
                "中位温度": distribution["中位数"],
                "最高频区间": largest_level["温度区间"],
                "最高频区间占比": largest_level["出现占比"],
                "分布判断": bias_note,
            }
        )
    return payload


def run_main_incremental() -> dict[str, object]:
    completed = subprocess.run(
        [str(PROJECT_PYTHON), str(ROOT / "main.py")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    (DATA_DIR / "validation_incremental_run.log").write_text(
        completed.stdout + completed.stderr, encoding="utf-8"
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"增量更新失败，详见 {DATA_DIR / 'validation_incremental_run.log'}"
        )
    return json.loads((OUTPUT_DIR / "run_summary.json").read_text(encoding="utf-8"))


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(DATABASE_PATH)) as connection:
        migration = migrate_indicator_daily(connection)
        create_constituent_history_table(connection)
    recovery = probe_constituent_sources()
    date_diagnosis = diagnose_399006_date()
    incremental_summary = run_main_incremental()
    with duckdb.connect(str(DATABASE_PATH), read_only=True) as connection:
        correlations = correlation_payload(connection)
        validation = temperature_validation_payload(connection)
        version_counts = connection.execute(
            """
            SELECT index_code, formula_version, constituent_mode, COUNT(*) AS n,
                   MIN(date) AS min_date, MAX(date) AS max_date
            FROM indicator_daily
            GROUP BY index_code, formula_version, constituent_mode
            ORDER BY index_code, formula_version
            """
        ).df().to_dict(orient="records")
        duplicate_count = int(
            connection.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT index_code, date, formula_version, COUNT(*) AS n
                    FROM indicator_daily
                    GROUP BY index_code, date, formula_version
                    HAVING COUNT(*) > 1
                )
                """
            ).fetchone()[0]
        )
    write_json(DATA_DIR / "indicator_correlation_payload.json", correlations)
    write_json(DATA_DIR / "temperature_validation_payload.json", validation)
    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "database_path": str(DATABASE_PATH),
        "migration": migration,
        "constituent_recovery": recovery,
        "date_diagnosis_399006": date_diagnosis,
        "incremental_run": incremental_summary,
        "indicator_version_counts": version_counts,
        "duplicate_index_date_version": duplicate_count,
        "correlation_high_pairs": {
            code: correlations["indices"][code]["high_pairs"] for code in INDEX_CODES
        },
        "temperature_distribution_summary": validation["summary"],
    }
    write_json(OUTPUT_DIR / "model_validation_summary.json", summary)
    enriched_run_summary = dict(incremental_summary)
    enriched_run_summary["历史成分恢复状态"] = recovery
    enriched_run_summary["399006最新日期诊断"] = date_diagnosis
    enriched_run_summary["公式版本统计"] = version_counts
    enriched_run_summary["模型验证摘要"] = str(
        OUTPUT_DIR / "model_validation_summary.json"
    )
    write_json(OUTPUT_DIR / "run_summary.json", enriched_run_summary)
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=json_default))


if __name__ == "__main__":
    for proxy_name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
        os.environ.pop(proxy_name, None)
    os.environ["NO_PROXY"] = "*"
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    main()
