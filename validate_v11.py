from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import date, datetime
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "output"
DATABASE_PATH = DATA_DIR / "market.duckdb"
INDEX_CODES = ("399006", "930986")
VERSIONS = ("V1.0", "V1.1")
STATE_ORDER = (
    "High_Rising",
    "High_Falling",
    "Low_Falling",
    "Low_Rising",
    "Middle",
    "Flat",
)
DIMENSION_COLUMNS = (
    "PriceMomentum_score",
    "Volume_score",
    "Breadth_score",
    "Sync_score_v11",
)
ORIGINAL_SCORE_COLUMNS = (
    "RET20_score",
    "BIAS20_score",
    "RS20_score",
    "VolumeStrength_score",
    "BreadthMA20_score",
    "HLBreadth_score",
    "Sync_score",
)


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


def dataframe_hash(frame: pd.DataFrame) -> str:
    """对排好序的V1.0全字段生成稳定摘要，用于写入前后核对。"""
    normalized = frame.copy()
    for column in normalized.select_dtypes(include=["datetime", "datetimetz"]).columns:
        normalized[column] = normalized[column].astype("datetime64[ns]").astype(str)
    hashed = pd.util.hash_pandas_object(normalized, index=False).to_numpy().tobytes()
    return hashlib.sha256(hashed).hexdigest()


def classify_state(temperature: object, change_5d: object) -> str | None:
    if pd.isna(temperature) or pd.isna(change_5d):
        return None
    temperature_value = float(temperature)
    change_value = float(change_5d)
    if change_value == 0:
        return "Flat"
    if temperature_value >= 80:
        return "High_Rising" if change_value > 0 else "High_Falling"
    if temperature_value <= 20:
        return "Low_Rising" if change_value > 0 else "Low_Falling"
    return "Middle"


def add_changes_and_states(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.sort_values("date").reset_index(drop=True).copy()
    result["temperature_change_1d_calc"] = (
        result["temperature"] - result["temperature"].shift(1)
    ).round(1)
    result["temperature_change_5d"] = (
        result["temperature"] - result["temperature"].shift(5)
    ).round(1)
    result["temperature_state"] = [
        classify_state(temperature, change)
        for temperature, change in zip(
            result["temperature"], result["temperature_change_5d"]
        )
    ]
    return result


def add_forward_metrics(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.sort_values("date").reset_index(drop=True).copy()
    close = result["close"].to_numpy(dtype=float)
    for horizon in (5, 10, 20):
        result[f"forward_{horizon}d"] = (
            result["close"].shift(-horizon) / result["close"] - 1
        )
    drawdown = np.full(len(result), np.nan)
    upside = np.full(len(result), np.nan)
    for position in range(max(len(result) - 20, 0)):
        future = close[position + 1 : position + 21]
        if (
            len(future) == 20
            and np.isfinite(future).all()
            and np.isfinite(close[position])
        ):
            drawdown[position] = future.min() / close[position] - 1
            upside[position] = future.max() / close[position] - 1
    result["future20_max_drawdown"] = drawdown
    result["future20_max_upside"] = upside
    return result


def safe_float(value: object) -> float | None:
    return None if pd.isna(value) else float(value)


def build_state_statistics(frame: pd.DataFrame) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    classified = frame.dropna(subset=["temperature_state"]).copy()
    for state in STATE_ORDER:
        group = classified[classified["temperature_state"] == state]
        row: dict[str, object] = {
            "状态": state,
            "状态样本数": int(len(group)),
        }
        for horizon in (5, 10, 20):
            series = group[f"forward_{horizon}d"].dropna()
            row[f"未来{horizon}日样本数"] = int(len(series))
            row[f"未来{horizon}日平均收益"] = safe_float(series.mean())
            row[f"未来{horizon}日中位收益"] = safe_float(series.median())
            row[f"未来{horizon}日上涨比例"] = safe_float((series > 0).mean())
        drawdown = group["future20_max_drawdown"].dropna()
        upside = group["future20_max_upside"].dropna()
        row["未来20日极值样本数"] = int(min(len(drawdown), len(upside)))
        row["未来20日平均最大回撤"] = safe_float(drawdown.mean())
        row["未来20日中位最大回撤"] = safe_float(drawdown.median())
        row["未来20日平均最大上涨"] = safe_float(upside.mean())
        row["未来20日中位最大上涨"] = safe_float(upside.median())
        rows.append(row)
    return rows


def make_v11_rows(v10: pd.DataFrame) -> pd.DataFrame:
    result = v10.sort_values(["index_code", "date"]).reset_index(drop=True).copy()
    result["PriceMomentum_score"] = result[
        ["RET20_score", "BIAS20_score", "RS20_score"]
    ].mean(axis=1, skipna=False)
    result["Volume_score"] = result["VolumeStrength_score"]
    result["Breadth_score"] = result[
        ["BreadthMA20_score", "HLBreadth_score"]
    ].mean(axis=1, skipna=False)
    result["Sync_score_v11"] = result["Sync_score"]
    valid = result[list(DIMENSION_COLUMNS)].notna().all(axis=1)
    result["temperature"] = (
        result[list(DIMENSION_COLUMNS)].sum(axis=1, skipna=False) / 4
    ).where(valid).round(1)
    pieces = []
    for _, group in result.groupby("index_code", sort=False):
        pieces.append(add_changes_and_states(group))
    result = pd.concat(pieces, ignore_index=True)
    result["formula_version"] = "V1.1"
    result["constituent_mode"] = "current_constituents_proxy"
    result["calculated_at"] = datetime.now()
    return result


def ensure_v11_columns(connection: duckdb.DuckDBPyConnection) -> None:
    for column, column_type in (
        ("PriceMomentum_score", "DOUBLE"),
        ("Volume_score", "DOUBLE"),
        ("Breadth_score", "DOUBLE"),
        ("Sync_score_v11", "DOUBLE"),
        ("temperature_change_5d", "DOUBLE"),
        ("temperature_state", "VARCHAR"),
    ):
        connection.execute(
            f"ALTER TABLE indicator_daily ADD COLUMN IF NOT EXISTS {column} {column_type}"
        )


def write_v11(connection: duckdb.DuckDBPyConnection, v11: pd.DataFrame) -> None:
    columns = [
        "index_code",
        "date",
        "formula_version",
        "constituent_mode",
        "RET20_raw",
        "RET20_score",
        "BIAS20_raw",
        "BIAS20_score",
        "RS20_raw",
        "RS20_score",
        "VolumeStrength_raw",
        "VolumeStrength_score",
        "BreadthMA20_raw",
        "BreadthMA20_score",
        "HLBreadth_raw",
        "HLBreadth_score",
        "Sync_raw",
        "Sync_score",
        "PriceMomentum_score",
        "Volume_score",
        "Breadth_score",
        "Sync_score_v11",
        "temperature",
        "temperature_change_1d",
        "temperature_change_5d",
        "temperature_state",
        "temperature_level",
        "calculated_at",
    ]
    incoming = v11.copy()
    incoming["temperature_change_1d"] = incoming["temperature_change_1d_calc"]
    incoming["temperature_level"] = None
    connection.register("v11_incoming", incoming[columns])
    column_sql = ", ".join(columns)
    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute("DELETE FROM indicator_daily WHERE formula_version = 'V1.1'")
        connection.execute(
            f"INSERT INTO indicator_daily ({column_sql}) SELECT {column_sql} FROM v11_incoming"
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    finally:
        connection.unregister("v11_incoming")


def load_version_frame(
    connection: duckdb.DuckDBPyConnection, code: str, version: str
) -> pd.DataFrame:
    frame = connection.execute(
        """
        SELECT i.date, i.temperature, i.temperature_change_1d,
               i.PriceMomentum_score, i.Volume_score, i.Breadth_score,
               i.Sync_score_v11, d.close
        FROM indicator_daily AS i
        JOIN index_daily AS d
          ON i.index_code = d.index_code AND i.date = d.date
        WHERE i.index_code = ? AND i.formula_version = ?
        ORDER BY i.date
        """,
        [code, version],
    ).df()
    derived = add_changes_and_states(frame)
    if version == "V1.0":
        equal = np.allclose(
            derived["temperature_change_1d_calc"],
            derived["temperature_change_1d"],
            equal_nan=True,
            rtol=0,
            atol=0,
        )
        if not equal:
            raise RuntimeError(f"{code} V1.0 的1日变化与独立复算不一致")
    return add_forward_metrics(derived)


def comparison_rows(
    statistics: dict[tuple[str, str], list[dict[str, object]]]
) -> list[dict[str, object]]:
    metrics = [
        "状态样本数",
        "未来5日样本数",
        "未来5日平均收益",
        "未来5日中位收益",
        "未来5日上涨比例",
        "未来10日样本数",
        "未来10日平均收益",
        "未来10日中位收益",
        "未来10日上涨比例",
        "未来20日样本数",
        "未来20日平均收益",
        "未来20日中位收益",
        "未来20日上涨比例",
        "未来20日极值样本数",
        "未来20日平均最大回撤",
        "未来20日中位最大回撤",
        "未来20日平均最大上涨",
        "未来20日中位最大上涨",
    ]
    rows: list[dict[str, object]] = []
    for code in INDEX_CODES:
        by_version = {
            version: {row["状态"]: row for row in statistics[(code, version)]}
            for version in VERSIONS
        }
        for state in STATE_ORDER:
            for metric in metrics:
                v10 = by_version["V1.0"][state][metric]
                v11 = by_version["V1.1"][state][metric]
                delta = None if v10 is None or v11 is None else v11 - v10
                rows.append(
                    {
                        "指数代码": code,
                        "状态": state,
                        "指标": metric,
                        "V1.0": v10,
                        "V1.1": v11,
                        "V1.1-V1.0": delta,
                    }
                )
    return rows


def correlation_rows(v11: pd.DataFrame) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    label_map = {
        "PriceMomentum_score": "PriceMomentum",
        "Volume_score": "Volume",
        "Breadth_score": "Breadth",
        "Sync_score_v11": "Sync",
    }
    rows: list[dict[str, object]] = []
    high_pairs: list[dict[str, object]] = []
    for code in INDEX_CODES:
        group = v11[v11["index_code"] == code].dropna(
            subset=list(DIMENSION_COLUMNS)
        )
        for method_name, method in (("Pearson", "pearson"), ("Spearman", "spearman")):
            matrix = group[list(DIMENSION_COLUMNS)].corr(method=method)
            for left_index, left in enumerate(DIMENSION_COLUMNS):
                for right in DIMENSION_COLUMNS[left_index + 1 :]:
                    value = float(matrix.loc[left, right])
                    row = {
                        "指数代码": code,
                        "方法": method_name,
                        "维度1": label_map[left],
                        "维度2": label_map[right],
                        "相关系数": value,
                        "绝对值不低于0.80": "是" if abs(value) >= 0.80 else "否",
                        "有效样本数": int(len(group)),
                        "开始日期": group["date"].min(),
                        "结束日期": group["date"].max(),
                    }
                    rows.append(row)
                    if abs(value) >= 0.80:
                        high_pairs.append(row)
    return rows, high_pairs


def independent_checks(
    connection: duckdb.DuckDBPyConnection,
    analysis_frames: dict[tuple[str, str], pd.DataFrame],
    v11: pd.DataFrame,
    v10_hash_before: str,
    v10_columns: list[str],
) -> dict[str, object]:
    v10_after = connection.execute(
        f"""
        SELECT {', '.join(v10_columns)}
        FROM indicator_daily WHERE formula_version = 'V1.0'
        ORDER BY index_code, date
        """
    ).df()
    v10_hash_after = dataframe_hash(v10_after)
    counts = connection.execute(
        """
        SELECT index_code, formula_version, constituent_mode, COUNT(*) AS rows,
               MIN(date) AS start_date, MAX(date) AS end_date
        FROM indicator_daily
        GROUP BY index_code, formula_version, constituent_mode
        ORDER BY index_code, formula_version
        """
    ).df()
    duplicates = int(
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
    formula_checks = []
    for code in INDEX_CODES:
        sample = v11[
            (v11["index_code"] == code) & v11["temperature"].notna()
        ].iloc[len(v11[(v11["index_code"] == code) & v11["temperature"].notna()]) // 2]
        price = (sample["RET20_score"] + sample["BIAS20_score"] + sample["RS20_score"]) / 3
        breadth = (sample["BreadthMA20_score"] + sample["HLBreadth_score"]) / 2
        temperature = round(
            (price + sample["VolumeStrength_score"] + breadth + sample["Sync_score"]) / 4,
            1,
        )
        formula_checks.append(
            {
                "index_code": code,
                "date": sample["date"],
                "price_momentum_equal": bool(np.isclose(price, sample["PriceMomentum_score"])),
                "breadth_equal": bool(np.isclose(breadth, sample["Breadth_score"])),
                "temperature_equal": bool(np.isclose(temperature, sample["temperature"])),
            }
        )
    boundary_checks = {
        "80_rising": classify_state(80, 1) == "High_Rising",
        "80_falling": classify_state(80, -1) == "High_Falling",
        "20_rising": classify_state(20, 1) == "Low_Rising",
        "20_falling": classify_state(20, -1) == "Low_Falling",
        "flat_priority": classify_state(90, 0) == "Flat",
        "middle": classify_state(50, -1) == "Middle",
        "missing_change_unclassified": classify_state(50, np.nan) is None,
    }
    forward_checks = []
    for code in INDEX_CODES:
        frame = analysis_frames[(code, "V1.1")]
        eligible = frame.dropna(subset=["forward_20d", "future20_max_drawdown"]).copy()
        sample = eligible.iloc[len(eligible) // 2]
        position = int(sample.name)
        close_t = frame.iloc[position]["close"]
        future = frame.iloc[position + 1 : position + 21]["close"]
        forward_checks.append(
            {
                "index_code": code,
                "date": sample["date"],
                "forward20_equal": bool(
                    np.isclose(sample["forward_20d"], frame.iloc[position + 20]["close"] / close_t - 1)
                ),
                "max_drawdown_equal": bool(
                    np.isclose(sample["future20_max_drawdown"], future.min() / close_t - 1)
                ),
                "max_upside_equal": bool(
                    np.isclose(sample["future20_max_upside"], future.max() / close_t - 1)
                ),
            }
        )
    all_formula_checks = all(
        check[key]
        for check in formula_checks
        for key in ("price_momentum_equal", "breadth_equal", "temperature_equal")
    )
    all_forward_checks = all(
        check[key]
        for check in forward_checks
        for key in ("forward20_equal", "max_drawdown_equal", "max_upside_equal")
    )
    checks = {
        "v1_hash_before": v10_hash_before,
        "v1_hash_after": v10_hash_after,
        "v1_unchanged": v10_hash_before == v10_hash_after,
        "version_counts": counts.to_dict(orient="records"),
        "duplicate_index_date_version": duplicates,
        "formula_spot_checks": formula_checks,
        "state_boundary_checks": boundary_checks,
        "forward_metric_spot_checks": forward_checks,
        "all_formula_checks_passed": all_formula_checks,
        "all_boundary_checks_passed": all(boundary_checks.values()),
        "all_forward_checks_passed": all_forward_checks,
    }
    if not all(
        (
            checks["v1_unchanged"],
            duplicates == 0,
            all_formula_checks,
            checks["all_boundary_checks_passed"],
            all_forward_checks,
        )
    ):
        raise RuntimeError(f"V1.1验证失败：{checks}")
    return checks


def run_workbook_builder() -> None:
    node = Path(
        r"C:\Users\Lenovo\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe"
    )
    completed = subprocess.run(
        [str(node), str(ROOT / "build_v11_validation.mjs")],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    (DATA_DIR / "temperature_v11_workbook_build.log").write_text(
        completed.stdout + completed.stderr, encoding="utf-8"
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"V1.1验证工作簿生成失败，详见{DATA_DIR / 'temperature_v11_workbook_build.log'}"
        )


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(DATABASE_PATH)) as connection:
        original_columns = [
            row[1]
            for row in connection.execute("PRAGMA table_info('indicator_daily')").fetchall()
        ]
        v10_before = connection.execute(
            f"""
            SELECT {', '.join(original_columns)}
            FROM indicator_daily WHERE formula_version = 'V1.0'
            ORDER BY index_code, date
            """
        ).df()
        v10_hash_before = dataframe_hash(v10_before)
        if set(v10_before["index_code"].unique()) != set(INDEX_CODES):
            raise RuntimeError("V1.0指数范围与指定的两个指数不一致")
        ensure_v11_columns(connection)
        v11 = make_v11_rows(v10_before)
        write_v11(connection, v11)

        analysis_frames: dict[tuple[str, str], pd.DataFrame] = {}
        statistics: dict[tuple[str, str], list[dict[str, object]]] = {}
        sheet_payloads: dict[str, object] = {}
        for code in INDEX_CODES:
            for version in VERSIONS:
                frame = load_version_frame(connection, code, version)
                analysis_frames[(code, version)] = frame
                statistics[(code, version)] = build_state_statistics(frame)
                valid_temperature = frame.dropna(subset=["temperature"])
                sheet_payloads[f"{code}_{version}"] = {
                    "index_code": code,
                    "formula_version": version,
                    "constituent_mode": "current_constituents_proxy",
                    "start_date": valid_temperature["date"].min(),
                    "end_date": valid_temperature["date"].max(),
                    "valid_temperature_rows": int(len(valid_temperature)),
                    "classified_rows": int(frame["temperature_state"].notna().sum()),
                    "statistics": statistics[(code, version)],
                }

        correlation_data, high_pairs = correlation_rows(v11)
        checks = independent_checks(
            connection,
            analysis_frames,
            v11,
            v10_hash_before,
            original_columns,
        )

    payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "definitions": {
            "ForwardReturn_N": "Close(t+N) / Close(t) - 1",
            "Future20_MaxDrawdown": "min(Close[t+1:t+20]) / Close(t) - 1",
            "Future20_MaxUpside": "max(Close[t+1:t+20]) / Close(t) - 1",
            "constituent_mode": "current_constituents_proxy",
        },
        "version_sheets": sheet_payloads,
        "comparison": comparison_rows(statistics),
        "dimension_correlation": correlation_data,
        "high_correlation_pairs": high_pairs,
        "checks": checks,
    }
    write_json(DATA_DIR / "temperature_v11_validation_payload.json", payload)
    write_json(OUTPUT_DIR / "temperature_v11_validation_summary.json", payload)
    run_workbook_builder()
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=json_default))


if __name__ == "__main__":
    main()
