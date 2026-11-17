"""日覆盖核验：逐应采时刻核对已入仓分片，生成不可变的覆盖报告。

核心语义
========
* 应采时刻：从 UTC 当日 00:00 起，每隔 ``interval_minutes`` 分钟一个；间隔必须整除 1440。
* 到齐判定：只有**该指标身份**（``i{indicator_id}`` 列）在**该站点**、**该应采时刻**
  上的**非 NULL** 值才算到齐。同一时刻多行只算一次；其他指标、其他站点、
  非应采时刻的值都不参与判定。
* 报告不可变：``coverage_reports`` 中的 ``complete`` / 缺测时刻 / 已到来源分片
  是核验当时固化的结论，按报告号永久保留。
* 过期只追加：新分片到达（同站、同指标、同日）或指标被删除时，只写入
  ``expired_at`` / ``expired_reason``，绝不改写原结论。供数查询只认
  “同键最新 && 未过期 && 齐全”的那一份报告。

一致性
======
本模块所有写操作（生成报告、过期报告）都由调用方放在进程写锁 +
``BEGIN IMMEDIATE`` 事务里执行；任何失败整体回滚，不会留下半份报告，
也不会误撤回供数。核验读取分片时同样处于该事务视图内，看到的是完整已提交状态。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, List, Optional, Sequence, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .config import INDICATOR_COLUMN_PREFIX, SAMPLED_AT_COLUMN, STATION_COLUMN
from .models import ConflictError, NotFound

MINUTES_PER_DAY = 1440
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# 输入校验与时间辅助
# ---------------------------------------------------------------------------

def validate_interval(interval_minutes: int) -> int:
    """采样间隔必须是正整数且整除 1440。"""
    if interval_minutes < 1:
        raise ConflictError("采样间隔必须为正整数分钟")
    if MINUTES_PER_DAY % interval_minutes != 0:
        raise ConflictError(
            f"采样间隔 {interval_minutes} 分钟不能整除 1440，无法得到完整的一天应采时刻"
        )
    return interval_minutes


def coerce_date(value) -> date:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).date() if value.tzinfo else value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError as exc:
        raise ConflictError(f"UTC 日期必须是 YYYY-MM-DD，实际为 {value!r}") from exc


def _format_instant(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _to_micros(dt: datetime) -> int:
    return (dt - _EPOCH) // timedelta(microseconds=1)


def expected_instants(day: date, interval_minutes: int) -> List[datetime]:
    """UTC 当日的全部应采时刻（不含次日 00:00）。"""
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    count = MINUTES_PER_DAY // interval_minutes
    return [start + timedelta(minutes=interval_minutes * k) for k in range(count)]


# ---------------------------------------------------------------------------
# 指标身份查询
# ---------------------------------------------------------------------------

def _load_active_indicator(conn: sqlite3.Connection, indicator_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT indicator_id, name, active FROM indicators WHERE indicator_id = ?",
        (indicator_id,),
    ).fetchone()
    if row is None:
        raise NotFound(f"指标 indicator_id={indicator_id} 不存在")
    if row["active"] != 1:
        raise NotFound(f"指标 indicator_id={indicator_id} 已删除，不能核验覆盖")
    return row


# ---------------------------------------------------------------------------
# 核验：读取分片，统计到达情况
# ---------------------------------------------------------------------------

def _shards_with_indicator(conn: sqlite3.Connection, indicator_id: int) -> List[sqlite3.Row]:
    return conn.execute(
        "SELECT s.shard_id, s.file_path "
        "FROM shards s JOIN shard_indicators si ON si.shard_id = s.shard_id "
        "WHERE si.indicator_id = ? ORDER BY s.shard_id",
        (indicator_id,),
    ).fetchall()


def _trim_station(arr: pa.ChunkedArray) -> pa.Array:
    """站点列去首尾空白；类型不是字符串时统一转字符串后再比较。"""
    combined = pc.cast(arr, pa.string(), safe=False)
    return pc.utf8_trim_whitespace(combined)


def _collect_arrivals(
    conn: sqlite3.Connection,
    indicator_id: int,
    station: str,
    instants: Sequence[datetime],
) -> dict[int, set[int]]:
    """返回 ``{应采时刻(微秒): {来源分片 ID}}``，只统计该身份的非 NULL 值。

    同刻多行/多分片只算一次，来源分片去重；同一分片内的多行也只记一次。
    """
    canonical = f"{INDICATOR_COLUMN_PREFIX}{indicator_id}"
    expected = {_to_micros(dt) for dt in instants}
    arrivals: dict[int, set[int]] = {}

    for shard in _shards_with_indicator(conn, indicator_id):
        shard_id = int(shard["shard_id"])
        file_path = shard["file_path"]
        try:
            with pq.ParquetFile(file_path) as parquet_file:
                schema_names = parquet_file.schema_arrow.names
        except FileNotFoundError as exc:
            raise ConflictError(
                f"分片 {shard_id} 的文件不存在，无法核验：{file_path}"
            ) from exc
        except Exception as exc:  # noqa: BLE001 - Arrow 异常类型较多
            raise ConflictError(f"分片 {shard_id} 读取失败，无法核验：{exc}") from exc

        required = [STATION_COLUMN, SAMPLED_AT_COLUMN, canonical]
        absent = [c for c in required if c not in schema_names]
        if absent:
            # 元数据记录该分片含此身份，但物理列缺失属数据损坏，拒绝给出错误结论
            raise ConflictError(
                f"分片 {shard_id} 缺少列 {absent!r}，无法核验"
            )

        try:
            table = pq.read_table(file_path, columns=required)
        except Exception as exc:  # noqa: BLE001
            raise ConflictError(f"分片 {shard_id} 读取失败，无法核验：{exc}") from exc

        station_arr = table.column(STATION_COLUMN).combine_chunks()
        value_arr = table.column(canonical).combine_chunks()

        # 站点按去除首尾空白后比较，避免入库值与查询值因空白不一致而误判缺测。
        # 仅该指标身份的非 NULL 值参与，其他指标/其他站点一律不算。
        trimmed_station = _trim_station(station_arr)
        mask = pc.and_(
            pc.equal(trimmed_station, pa.scalar(station, type=pa.string())),
            pc.is_valid(value_arr),
        )
        mask = pc.fill_null(mask, False)
        matched = table.filter(mask)
        if matched.num_rows:
            matched_times = pc.cast(
                matched.column(SAMPLED_AT_COLUMN).combine_chunks(), pa.int64()
            ).to_pylist()
            for micros in matched_times:
                if micros in expected:
                    arrivals.setdefault(int(micros), set()).add(shard_id)
    return arrivals


# ---------------------------------------------------------------------------
# 报告的落库与读取
# ---------------------------------------------------------------------------

def _report_from_row(row: sqlite3.Row) -> dict:
    day = date.fromisoformat(row["coverage_date"])
    missing = json.loads(row["missing_instants"])
    arrived = json.loads(row["arrived_instants"])
    return {
        "report_id": int(row["report_id"]),
        "station": row["station"],
        "indicator_id": int(row["indicator_id"]),
        "indicator_name": row["indicator_name"],
        "date": row["coverage_date"],
        "interval_minutes": int(row["interval_minutes"]),
        "expected_count": int(row["expected_count"]),
        "arrived_count": int(row["arrived_count"]),
        "missing_count": len(missing),
        "complete": bool(row["complete"]),
        "expected_instants": [
            _format_instant(dt) for dt in expected_instants(day, int(row["interval_minutes"]))
        ],
        "missing_instants": missing,
        "arrived_instants": arrived,
        "created_at": row["created_at"],
        "expired": row["expired_at"] is not None,
        "expired_at": row["expired_at"],
        "expired_reason": row["expired_reason"],
    }


def get_report(conn: sqlite3.Connection, report_id: int) -> dict:
    row = conn.execute(
        "SELECT * FROM coverage_reports WHERE report_id = ?", (report_id,)
    ).fetchone()
    if row is None:
        raise NotFound(f"覆盖报告 {report_id} 不存在")
    return _report_from_row(row)


def verify_daily_coverage(
    conn: sqlite3.Connection,
    station: str,
    indicator_id: int,
    day,
    interval_minutes: int,
) -> dict:
    """在当前事务内生成并落库一份不可变覆盖报告。"""
    station = (station or "").strip()
    if not station:
        raise ConflictError("站点不能为空")
    interval_minutes = validate_interval(int(interval_minutes))
    day = coerce_date(day)

    indicator = _load_active_indicator(conn, int(indicator_id))
    instants = expected_instants(day, interval_minutes)
    arrivals = _collect_arrivals(conn, int(indicator_id), station, instants)

    missing: List[str] = []
    arrived: List[dict] = []
    for dt in instants:
        sources = arrivals.get(_to_micros(dt))
        if sources:
            arrived.append({"sampled_at": _format_instant(dt), "shard_ids": sorted(sources)})
        else:
            missing.append(_format_instant(dt))

    expected_count = len(instants)
    arrived_count = len(arrived)
    complete = arrived_count == expected_count

    cur = conn.execute(
        "INSERT INTO coverage_reports ("
        "station, indicator_id, indicator_name, coverage_date, interval_minutes, "
        "expected_count, arrived_count, complete, missing_instants, arrived_instants"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            station,
            int(indicator_id),
            indicator["name"],
            day.isoformat(),
            interval_minutes,
            expected_count,
            arrived_count,
            1 if complete else 0,
            json.dumps(missing, ensure_ascii=False),
            json.dumps(arrived, ensure_ascii=False),
        ),
    )
    report_id = int(cur.lastrowid)
    return get_report(conn, report_id)


# ---------------------------------------------------------------------------
# 供数查询
# ---------------------------------------------------------------------------

def _latest_report_row(
    conn: sqlite3.Connection,
    station: str,
    indicator_id: int,
    day: date,
    interval_minutes: int,
) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM coverage_reports "
        "WHERE station = ? AND indicator_id = ? AND coverage_date = ? "
        "AND interval_minutes = ? ORDER BY report_id DESC LIMIT 1",
        (station, indicator_id, day.isoformat(), interval_minutes),
    ).fetchone()


def supply_status(
    conn: sqlite3.Connection,
    station: str,
    indicator_id: int,
    day,
    interval_minutes: int,
) -> dict:
    """返回供数可用性。只认该键**最新且未过期且齐全**的报告。"""
    station = (station or "").strip()
    if not station:
        raise ConflictError("站点不能为空")
    interval_minutes = validate_interval(int(interval_minutes))
    day = coerce_date(day)

    base = {
        "station": station,
        "indicator_id": int(indicator_id),
        "date": day.isoformat(),
        "interval_minutes": interval_minutes,
        "available": False,
        "reason_code": None,
        "reason": None,
        "report_id": None,
        "report_complete": None,
        "report_expired": None,
        "missing_count": None,
        "missing_instants": None,
    }

    indicator = conn.execute(
        "SELECT indicator_id, name, active FROM indicators WHERE indicator_id = ?",
        (int(indicator_id),),
    ).fetchone()
    if indicator is None or indicator["active"] != 1:
        base["reason_code"] = "indicator_unavailable"
        base["reason"] = (
            f"指标 indicator_id={indicator_id} 不存在或已删除，当前不可供数"
        )
        return base

    row = _latest_report_row(conn, station, int(indicator_id), day, interval_minutes)
    if row is None:
        base["reason_code"] = "no_report"
        base["reason"] = "尚无该站点、该指标、该日、该间隔的覆盖核验报告"
        return base

    report = _report_from_row(row)
    base["report_id"] = report["report_id"]
    base["report_complete"] = report["complete"]
    base["report_expired"] = report["expired"]
    base["missing_count"] = report["missing_count"]
    base["missing_instants"] = report["missing_instants"]

    if report["expired"]:
        base["reason_code"] = "report_expired"
        base["reason"] = f"最新覆盖报告已过期：{report['expired_reason'] or '数据已变化'}"
        return base
    if not report["complete"]:
        base["reason_code"] = "report_incomplete"
        base["reason"] = (
            f"最新覆盖报告不齐全：缺测 {report['missing_count']} 个应采时刻"
        )
        return base

    base["available"] = True
    return base


# ---------------------------------------------------------------------------
# 过期（撤回供数）
# ---------------------------------------------------------------------------

def _expire_ids(
    conn: sqlite3.Connection,
    report_ids: Iterable[int],
    reason: str,
) -> List[int]:
    """把给定报告置为过期；只写过期标记，不改原结论。返回实际过期成功的报告号。"""
    expired: List[int] = []
    for report_id in sorted(set(report_ids)):
        cur = conn.execute(
            "UPDATE coverage_reports "
            "SET expired_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), expired_reason = ? "
            "WHERE report_id = ? AND expired_at IS NULL",
            (reason, report_id),
        )
        if cur.rowcount:
            expired.append(report_id)
    return expired


def expire_for_new_shard(
    conn: sqlite3.Connection,
    affected_keys: Iterable[Tuple[str, int, str]],
    shard_id: int,
) -> List[int]:
    """同站、同指标、同日出现新分片时，过期旧报告并撤回供数。

    ``affected_keys`` 由上传方在规范化分片时算出：``(station, indicator_id, YYYY-MM-DD)``。
    只影响**未过期**的报告；已过期报告保留首次过期时间与原因。
    """
    report_ids: set[int] = set()
    for station, indicator_id, coverage_date in affected_keys:
        rows = conn.execute(
            "SELECT report_id FROM coverage_reports "
            "WHERE station = ? AND indicator_id = ? AND coverage_date = ? "
            "AND expired_at IS NULL",
            (station, int(indicator_id), coverage_date),
        ).fetchall()
        report_ids.update(int(r["report_id"]) for r in rows)
    if not report_ids:
        return []
    reason = f"该站点该指标当日有新分片到达（shard_id={shard_id}），旧报告已过期"
    return _expire_ids(conn, report_ids, reason)


def expire_for_deleted_indicator(
    conn: sqlite3.Connection,
    indicator_id: int,
) -> List[int]:
    """指标被删除时，过期其全部未过期报告并撤回供数。"""
    rows = conn.execute(
        "SELECT report_id FROM coverage_reports "
        "WHERE indicator_id = ? AND expired_at IS NULL",
        (int(indicator_id),),
    ).fetchall()
    report_ids = [int(r["report_id"]) for r in rows]
    if not report_ids:
        return []
    reason = f"指标 indicator_id={indicator_id} 已被删除，旧报告已过期"
    return _expire_ids(conn, report_ids, reason)
