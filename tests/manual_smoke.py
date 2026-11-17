"""独立 HTTP 冒烟脚本：对运行中的服务验证完整业务故事（与 pytest 夹具无关）。

用法：先启动 uvicorn，再运行  python tests/manual_smoke.py
"""
from __future__ import annotations

import io
import sys

import os

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000")


def parquet_bytes(columns: dict, types: dict | None = None) -> bytes:
    arrays = {name: pa.array(values, type=(types or {}).get(name)) for name, values in columns.items()}
    buf = io.BytesIO()
    pq.write_table(pa.table(arrays), buf)
    return buf.getvalue()


def main() -> int:
    c = httpx.Client(base_url=BASE, timeout=10)
    assert c.get("/health").status_code == 200

    # 字段版 2：新增“浓度”
    v2 = c.post("/schema/indicators", json={"name": "浓度"}).json()["new_version"]
    print("version with 浓度:", v2)

    # 历史分片
    r = c.post(
        "/ingest/parquet",
        data={"schema_version": str(v2)},
        files={"file": ("h.parquet", parquet_bytes({
            "station": ["S1"],
            "sampled_at": ["2026-01-01T00:00:00Z"],
            "浓度": [11.5],
        }, {"sampled_at": pa.string()}), "application/vnd.apache.parquet")},
    )
    r.raise_for_status()
    print("historical shard:", r.json())

    # 改名 -> 硝酸盐（版 3）；再建“浓度”（版 4，新身份=磷酸盐）
    c.post("/schema/indicators/rename", json={"old_name": "浓度", "new_name": "硝酸盐"}).raise_for_status()
    v4 = c.post("/schema/indicators", json={"name": "浓度"}).json()["new_version"]
    print("current version:", v4)

    # 旧版 2 上传 -> 硝酸盐原身份
    c.post(
        "/ingest/parquet",
        data={"schema_version": str(v2)},
        files={"file": ("old.parquet", parquet_bytes({
            "station": ["S2"], "sampled_at": ["2026-01-02T00:00:00Z"], "浓度": [22.0],
        }, {"sampled_at": pa.string()}), "application/vnd.apache.parquet")},
    ).raise_for_status()

    # 新版 4 上传 -> 磷酸盐新身份
    c.post(
        "/ingest/parquet",
        data={"schema_version": str(v4)},
        files={"file": ("new.parquet", parquet_bytes({
            "station": ["S3"], "sampled_at": ["2026-01-03T00:00:00Z"], "浓度": [33.25],
        }, {"sampled_at": pa.string()}), "application/vnd.apache.parquet")},
    ).raise_for_status()

    page = c.get("/rows").json()
    snap = page["snapshot_id"]
    print("snapshot:", snap, "names:", page["indicator_names"], "rows:", page["total_rows"])
    for row in page["rows"]:
        print(" ", row)

    by = {r["station"]: r for r in page["rows"]}
    assert by["S1"]["硝酸盐"] == 11.5 and by["S1"]["浓度"] is None
    assert by["S2"]["硝酸盐"] == 22.0 and by["S2"]["浓度"] is None
    assert by["S3"]["浓度"] == 33.25 and by["S3"]["硝酸盐"] is None

    # 快照之后删指标 + 上传，快照不变
    c.delete("/schema/indicators/浓度").raise_for_status()
    frozen = c.get("/rows", params={"snapshot_id": snap}).json()
    assert frozen["indicator_names"] == ["硝酸盐", "浓度"]
    assert frozen["total_rows"] == 3
    print("snapshot still frozen after delete+upload:", frozen["indicator_names"], frozen["total_rows"])

    # 已删指标上传必须 409
    rejected = c.post(
        "/ingest/parquet",
        data={"schema_version": str(v4)},
        files={"file": ("x.parquet", parquet_bytes({
            "station": ["X"], "sampled_at": ["2026-01-04T00:00:00Z"], "浓度": [1.0],
        }, {"sampled_at": pa.string()}), "application/vnd.apache.parquet")},
    )
    assert rejected.status_code == 409, rejected.text
    print("deleted-indicator upload rejected:", rejected.json()["detail"])

    # 日覆盖核验：为“硝酸盐”建一个完整的 24 点小时序列（S1，2026-01-05）
    from datetime import datetime, timedelta
    day = "2026-01-05"
    base = datetime.fromisoformat(day + "T00:00:00+00:00")
    stamps = [(base + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M:%SZ") for h in range(24)]
    cur = c.get("/schema").json()
    nitrate_id = next(i["indicator_id"] for i in cur["indicators"] if i["name"] == "硝酸盐")
    r = c.post(
        "/ingest/parquet",
        data={"schema_version": str(cur["current_version"])},
        files={"file": ("day.parquet", parquet_bytes({
            "station": ["S1"] * 24,
            "sampled_at": stamps,
            "硝酸盐": [float(h) for h in range(24)],
        }, {"sampled_at": pa.string()}), "application/vnd.apache.parquet")},
    )
    r.raise_for_status()

    rep = c.post("/coverage/verifications", params={
        "station": "S1", "indicator_id": nitrate_id,
        "date": day, "interval_minutes": 60,
    })
    rep.raise_for_status()
    report = rep.json()
    assert report["complete"] is True and report["arrived_count"] == 24, report
    print("coverage report:", report["report_id"], "complete:", report["complete"])

    status = c.get("/coverage/supply", params={
        "station": "S1", "indicator_id": nitrate_id,
        "date": day, "interval_minutes": 60,
    }).json()
    assert status["available"] is True, status
    print("supply available:", status["report_id"])

    # 同日补一块新分片 -> 旧报告过期并撤回供数，上传回执列出报告号
    r = c.post(
        "/ingest/parquet",
        data={"schema_version": str(cur["current_version"])},
        files={"file": ("patch.parquet", parquet_bytes({
            "station": ["S1"], "sampled_at": [f"{day}T00:30:00Z"], "硝酸盐": [99.0],
        }, {"sampled_at": pa.string()}), "application/vnd.apache.parquet")},
    )
    r.raise_for_status()
    assert report["report_id"] in r.json()["expired_report_ids"], r.json()
    print("upload receipt expired reports:", r.json()["expired_report_ids"])

    kept = c.get(f"/coverage/reports/{report['report_id']}").json()
    assert kept["complete"] is True and kept["expired"] is True, kept
    withdrawn = c.get("/coverage/supply", params={
        "station": "S1", "indicator_id": nitrate_id,
        "date": day, "interval_minutes": 60,
    }).json()
    assert withdrawn["available"] is False and withdrawn["reason_code"] == "report_expired", withdrawn
    print("supply withdrawn:", withdrawn["reason_code"])

    # 重新核验（此时应采时刻仍未齐全），再删除指标 ->
    # 只把“本次之前未过期”的报告列入回执，已过期报告保留首次过期信息
    fresh = c.post("/coverage/verifications", params={
        "station": "S1", "indicator_id": nitrate_id,
        "date": day, "interval_minutes": 60,
    })
    fresh.raise_for_status()
    fresh_id = fresh.json()["report_id"]
    assert fresh_id != report["report_id"]

    deleted = c.delete("/schema/indicators/硝酸盐")
    deleted.raise_for_status()
    assert fresh_id in deleted.json()["expired_report_ids"], deleted.json()
    assert report["report_id"] not in deleted.json()["expired_report_ids"], deleted.json()
    gone = c.get("/coverage/supply", params={
        "station": "S1", "indicator_id": nitrate_id,
        "date": day, "interval_minutes": 60,
    }).json()
    assert gone["available"] is False and gone["reason_code"] == "indicator_unavailable", gone
    print("supply after delete:", gone["reason_code"])

    print("\nSMOKE OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
