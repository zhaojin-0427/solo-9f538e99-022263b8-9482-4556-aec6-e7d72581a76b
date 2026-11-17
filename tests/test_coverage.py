"""日覆盖核验：应采时刻判定、缺测/来源分片、报告不可变、过期与供数裁决。"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import pyarrow as pa

from app.db import connect, init_db
from app.main import app
from fastapi.testclient import TestClient


DAY = "2026-09-01"


def upload_file(client, path, version: int):
    with open(path, "rb") as fh:
        return client.post(
            "/ingest/parquet",
            data={"schema_version": str(version)},
            files={"file": (path.name, fh.read(), "application/vnd.apache.parquet")},
        )


def iso(day: str, hour: int, minute: int) -> str:
    return f"{day}T{hour:02d}:{minute:02d}:00Z"


def full_day_rows(day: str = DAY, interval: int = 60, value: float = 1.0):
    start = datetime.fromisoformat(day + "T00:00:00+00:00")
    return [
        (start + timedelta(minutes=interval * k)).strftime("%Y-%m-%dT%H:%M:%SZ")
        for k in range(1440 // interval)
    ]


def make_indicator(client, name: str = "浓度") -> tuple[int, int]:
    r = client.post("/schema/indicators", json={"name": name})
    assert r.status_code == 201, r.text
    return r.json()["indicator_id"], r.json()["new_version"]


def verify(client, station: str, indicator_id: int, day: str, interval: int):
    return client.post(
        "/coverage/verifications",
        params={
            "station": station,
            "indicator_id": indicator_id,
            "date": day,
            "interval_minutes": interval,
        },
    )


def supply(client, station: str, indicator_id: int, day: str, interval: int):
    return client.get(
        "/coverage/supply",
        params={
            "station": station,
            "indicator_id": indicator_id,
            "date": day,
            "interval_minutes": interval,
        },
    )


# ---------------------------------------------------------------------------
# 齐全 / 缺测判定
# ---------------------------------------------------------------------------

def test_complete_day_available_and_sources(client, make_parquet):
    iid, vid = make_indicator(client)
    times = full_day_rows()
    f = make_parquet("full.parquet", {
        "station": ["S1"] * len(times),
        "sampled_at": times,
        "浓度": [1.5] * len(times),
    }, types={"sampled_at": pa.string()})
    r = upload_file(client, f, vid)
    assert r.status_code == 200, r.text

    r = verify(client, "S1", iid, DAY, 60)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["complete"] is True
    assert body["expected_count"] == 24
    assert body["arrived_count"] == 24
    assert body["missing_count"] == 0
    assert body["missing_instants"] == []
    assert len(body["arrived_instants"]) == 24
    assert all(a["shard_ids"] == [1] for a in body["arrived_instants"])
    assert [a["sampled_at"] for a in body["arrived_instants"]] == [
        t.replace("Z", ".000000Z") for t in times
    ]

    s = supply(client, "S1", iid, DAY, 60).json()
    assert s["available"] is True
    assert s["reason_code"] is None
    assert s["report_id"] == body["report_id"]


def test_null_values_other_station_and_other_indicator_do_not_count(client, make_parquet):
    iid, _ = make_indicator(client, "浓度")
    other_id, vid = make_indicator(client, "浊度")  # 该版同时包含两个指标

    times = full_day_rows()
    # S1：00:00 有值，01:00 为 NULL，其余（02:00~）缺行
    s1_times = times[:2]
    f1 = make_parquet("s1.parquet", {
        "station": ["S1", "S1"],
        "sampled_at": s1_times,
        "浓度": [3.0, None],
        "浊度": [9.0, 9.0],
    }, types={"sampled_at": pa.string(), "浓度": pa.float64(), "浊度": pa.float64()})
    assert upload_file(client, f1, vid).status_code == 200

    # S2 在同一时刻有值，不能算到 S1 头上
    f2 = make_parquet("s2.parquet", {
        "station": ["S2", "S2"],
        "sampled_at": times[:2],
        "浓度": [7.0, 8.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f2, vid).status_code == 200

    body = verify(client, "S1", iid, DAY, 60).json()
    assert body["complete"] is False
    assert body["arrived_count"] == 1
    assert body["missing_count"] == 23
    assert body["arrived_instants"] == [
        {"sampled_at": "2026-09-01T00:00:00.000000Z", "shard_ids": [1]}
    ]
    assert body["missing_instants"][0] == "2026-09-01T01:00:00.000000Z"
    assert "2026-09-01T00:00:00.000000Z" not in body["missing_instants"]

    s = supply(client, "S1", iid, DAY, 60).json()
    assert s["available"] is False
    assert s["reason_code"] == "report_incomplete"
    assert s["report_id"] == body["report_id"]


def test_duplicate_rows_same_instant_count_once_and_merge_sources(client, make_parquet):
    iid, vid = make_indicator(client)
    f1 = make_parquet("dup1.parquet", {
        "station": ["S1", "S1"],
        "sampled_at": [iso(DAY, 0, 0), iso(DAY, 0, 0)],
        "浓度": [1.0, 2.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f1, vid).status_code == 200
    f2 = make_parquet("dup2.parquet", {
        "station": ["S1"],
        "sampled_at": [iso(DAY, 0, 0)],
        "浓度": [3.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f2, vid).status_code == 200

    body = verify(client, "S1", iid, DAY, 1440).json()
    # 间隔 1440 -> 只有 00:00 一个应采时刻；三行来自两个分片但只算一次
    assert body["expected_count"] == 1
    assert body["arrived_count"] == 1
    assert body["arrived_instants"][0]["shard_ids"] == [1, 2]
    assert body["complete"] is True


def test_off_grid_times_do_not_count(client, make_parquet):
    iid, vid = make_indicator(client)
    f = make_parquet("off.parquet", {
        "station": ["S1"],
        "sampled_at": ["2026-09-01T00:07:00Z"],
        "浓度": [1.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f, vid).status_code == 200
    body = verify(client, "S1", iid, DAY, 60).json()
    assert body["arrived_count"] == 0
    assert body["missing_count"] == 24
    assert body["complete"] is False


# ---------------------------------------------------------------------------
# 输入校验
# ---------------------------------------------------------------------------

def test_interval_must_divide_1440_and_date_valid(client, make_parquet):
    iid, vid = make_indicator(client)
    assert verify(client, "S1", iid, DAY, 7).status_code == 409
    assert verify(client, "S1", iid, DAY, 0).status_code == 409
    assert verify(client, "S1", iid, DAY, -30).status_code == 409
    assert verify(client, "S1", iid, DAY, 1441).status_code == 409
    assert verify(client, "S1", iid, "2026-13-01", 60).status_code == 409
    assert verify(client, "S1", iid, "not-a-date", 60).status_code == 409
    # 合法但非 24 整除：1、30、1440 都可以
    for ok in (1, 30, 60, 720, 1440):
        assert verify(client, "S1", iid, DAY, ok).status_code == 201


def test_unknown_or_deleted_indicator_cannot_verify(client, make_parquet):
    assert verify(client, "S1", 999, DAY, 60).status_code == 404

    iid, vid = make_indicator(client)
    assert client.delete("/schema/indicators/浓度").status_code == 200
    assert verify(client, "S1", iid, DAY, 60).status_code == 404
    # 供数查询对已删指标给出原因，而不是 500/404
    s = supply(client, "S1", iid, DAY, 60).json()
    assert s["available"] is False
    assert s["reason_code"] == "indicator_unavailable"


def test_supply_without_report_explains_no_report(client):
    iid, _ = make_indicator(client)
    s = supply(client, "S1", iid, DAY, 60).json()
    assert s["available"] is False
    assert s["reason_code"] == "no_report"
    assert s["report_id"] is None


# ---------------------------------------------------------------------------
# 报告不可变与过期
# ---------------------------------------------------------------------------

def test_report_keeps_original_conclusion_after_new_shard(client, make_parquet):
    iid, vid = make_indicator(client)
    f1 = make_parquet("p1.parquet", {
        "station": ["S1"],
        "sampled_at": [iso(DAY, 0, 0)],
        "浓度": [1.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f1, vid).status_code == 200

    rep = verify(client, "S1", iid, DAY, 1440).json()
    assert rep["complete"] is True
    assert rep["expired"] is False
    assert supply(client, "S1", iid, DAY, 1440).json()["available"] is True

    # 同日新分片（补一个不同时刻）-> 旧报告过期，供数撤回
    f2 = make_parquet("p2.parquet", {
        "station": ["S1"],
        "sampled_at": [iso(DAY, 12, 0)],
        "浓度": [2.0],
    }, types={"sampled_at": pa.string()})
    r = upload_file(client, f2, vid)
    assert r.status_code == 200
    assert r.json()["expired_report_ids"] == [rep["report_id"]]

    # 按号读：原结论一字不改，只多出过期标记
    kept = client.get(f"/coverage/reports/{rep['report_id']}").json()
    assert kept["complete"] is True
    assert kept["arrived_count"] == 1
    assert kept["missing_count"] == 0
    assert kept["expired"] is True
    assert kept["expired_at"] is not None
    assert "shard_id=2" in kept["expired_reason"]

    s = supply(client, "S1", iid, DAY, 1440).json()
    assert s["available"] is False
    assert s["reason_code"] == "report_expired"
    assert s["report_id"] == rep["report_id"]

    # 重新核验后，新报告可供数；旧报告仍保持过期
    rep2 = verify(client, "S1", iid, DAY, 1440).json()
    assert rep2["report_id"] != rep["report_id"]
    assert rep2["expired"] is False
    assert supply(client, "S1", iid, DAY, 1440).json()["available"] is True
    assert client.get(f"/coverage/reports/{rep['report_id']}").json()["expired"] is True


def test_new_shard_other_day_station_or_indicator_does_not_expire(client, make_parquet):
    iid, _ = make_indicator(client, "浓度")
    other_id, vid = make_indicator(client, "浊度")  # 该版同时包含两个指标
    f1 = make_parquet("base.parquet", {
        "station": ["S1"],
        "sampled_at": [iso(DAY, 0, 0)],
        "浓度": [1.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f1, vid).status_code == 200
    rep = verify(client, "S1", iid, DAY, 1440).json()
    assert rep["complete"] is True

    # 不同日
    f2 = make_parquet("nextday.parquet", {
        "station": ["S1"], "sampled_at": ["2026-09-02T00:00:00Z"], "浓度": [1.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f2, vid).json()["expired_report_ids"] == []
    # 不同站
    f3 = make_parquet("otherstation.parquet", {
        "station": ["S2"], "sampled_at": [iso(DAY, 1, 0)], "浓度": [1.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f3, vid).json()["expired_report_ids"] == []
    # 同站同日但只有其他指标（不含浓度列）
    f4 = make_parquet("otherind.parquet", {
        "station": ["S1"], "sampled_at": [iso(DAY, 2, 0)], "浊度": [1.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f4, vid).json()["expired_report_ids"] == []

    assert supply(client, "S1", iid, DAY, 1440).json()["available"] is True


def test_delete_expires_all_reports_and_upload_receipts(client, make_parquet):
    iid, vid = make_indicator(client)
    f = make_parquet("d.parquet", {
        "station": ["S1"], "sampled_at": [iso(DAY, 0, 0)], "浓度": [1.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f, vid).status_code == 200
    r1 = verify(client, "S1", iid, DAY, 1440).json()
    r2 = verify(client, "S1", iid, DAY, 720).json()

    r = client.delete("/schema/indicators/浓度")
    assert r.status_code == 200
    assert r.json()["expired_report_ids"] == [r1["report_id"], r2["report_id"]]

    for rid in (r1["report_id"], r2["report_id"]):
        assert client.get(f"/coverage/reports/{rid}").json()["expired"] is True
    assert supply(client, "S1", iid, DAY, 1440).json()["reason_code"] == "indicator_unavailable"


def test_rename_keeps_identity_and_report_valid(client, make_parquet):
    iid, vid = make_indicator(client, "浓度")
    f = make_parquet("r.parquet", {
        "station": ["S1"], "sampled_at": [iso(DAY, 0, 0)], "浓度": [1.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f, vid).status_code == 200
    rep = verify(client, "S1", iid, DAY, 1440).json()

    r = client.post("/schema/indicators/rename", json={"old_name": "浓度", "new_name": "硝酸盐"})
    assert r.status_code == 200
    assert r.json()["indicator_id"] == iid
    assert r.json()["expired_report_ids"] == []

    # 旧报告按原身份仍有效，供数可用
    s = supply(client, "S1", iid, DAY, 1440).json()
    assert s["available"] is True and s["report_id"] == rep["report_id"]

    # 同名新建得到新身份，不能沿用旧报告
    r = client.post("/schema/indicators", json={"name": "浓度"})
    new_id = r.json()["indicator_id"]
    assert new_id != iid
    s2 = supply(client, "S1", new_id, DAY, 1440).json()
    assert s2["available"] is False
    assert s2["reason_code"] == "no_report"


# ---------------------------------------------------------------------------
# 原子性与并发
# ---------------------------------------------------------------------------

def test_failed_upload_creates_no_report_change_and_no_expiry(client, make_parquet):
    iid, vid = make_indicator(client)
    f = make_parquet("ok.parquet", {
        "station": ["S1"], "sampled_at": [iso(DAY, 0, 0)], "浓度": [1.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f, vid).status_code == 200
    rep = verify(client, "S1", iid, DAY, 1440).json()
    assert supply(client, "S1", iid, DAY, 1440).json()["available"] is True

    # 含已删/未知列的坏上传：整次拒绝，不得误撤供数
    bad = make_parquet("bad.parquet", {
        "station": ["S1"], "sampled_at": [iso(DAY, 0, 30)], "不存在": [1.0],
    }, types={"sampled_at": pa.string()})
    r = upload_file(client, bad, vid)
    assert r.status_code == 409
    assert supply(client, "S1", iid, DAY, 1440).json()["available"] is True
    assert client.get(f"/coverage/reports/{rep['report_id']}").json()["expired"] is False

    # 核验失败不产生半份报告
    conn = connect()
    try:
        before = conn.execute("SELECT COUNT(*) AS n FROM coverage_reports").fetchone()["n"]
    finally:
        conn.close()
    assert verify(client, "S1", iid, DAY, 7).status_code == 409
    assert verify(client, "S1", 999, DAY, 60).status_code == 404
    conn = connect()
    try:
        after = conn.execute("SELECT COUNT(*) AS n FROM coverage_reports").fetchone()["n"]
    finally:
        conn.close()
    assert before == after


def test_concurrent_verify_upload_delete_consistent(client, make_parquet):
    """核验、上传与删除交错：每份报告要么完整可用，要么明确过期，不出现半份。"""
    iid, vid = make_indicator(client)
    f0 = make_parquet("c0.parquet", {
        "station": ["S1"], "sampled_at": [iso("2026-10-01", 0, 0)], "浓度": [1.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f0, vid).status_code == 200

    barrier = threading.Barrier(3)
    errors: list[Exception] = []
    report_ids: list[int] = []

    def verifier():
        try:
            barrier.wait()
            for _ in range(8):
                r = verify(client, "S1", iid, "2026-10-01", 1440)
                # 指标可能已被并发删除；此时必须干净拒绝，不能产生半份报告
                assert r.status_code in (201, 404), r.text
                if r.status_code == 201:
                    report_ids.append(r.json()["report_id"])
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def uploader():
        try:
            barrier.wait()
            for i in range(8):
                f = make_parquet(f"cu-{i}.parquet", {
                    "station": ["S1"],
                    "sampled_at": [iso("2026-10-01", 0, 0)],
                    "浓度": [float(i)],
                }, types={"sampled_at": pa.string()})
                r = upload_file(client, f, vid)
                # 指标被并发删除后，含该指标的旧版上传应整次拒绝；不得出现其他状态
                assert r.status_code in (200, 409), r.text
                if r.status_code == 409:
                    assert "已被删除" in r.json()["detail"]
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def deleter():
        try:
            barrier.wait()
            for name in ("浓度", "不存在", "硝酸盐"):
                client.delete(f"/schema/indicators/{name}")
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=verifier), threading.Thread(target=uploader),
               threading.Thread(target=deleter)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors

    # 每份报告都可按号读取，且结论字段完整自洽
    for rid in report_ids:
        r = client.get(f"/coverage/reports/{rid}")
        assert r.status_code == 200
        body = r.json()
        assert body["arrived_count"] + body["missing_count"] == body["expected_count"]
        assert body["complete"] == (body["missing_count"] == 0)
        assert len(body["expected_instants"]) == body["expected_count"]

    # 供数裁决与最终状态一致：删除后必然不可供数
    s = supply(client, "S1", iid, "2026-10-01", 1440).json()
    assert s["available"] is False
    assert s["reason_code"] in {"indicator_unavailable", "report_expired", "report_incomplete"}


def test_reports_persist_across_restart(client, make_parquet):
    iid, vid = make_indicator(client)
    f = make_parquet("persist.parquet", {
        "station": ["S1"], "sampled_at": [iso(DAY, 0, 0)], "浓度": [1.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f, vid).status_code == 200
    rep = verify(client, "S1", iid, DAY, 1440).json()

    del client
    init_db()
    with TestClient(app) as client2:
        got = client2.get(f"/coverage/reports/{rep['report_id']}").json()
        assert got["complete"] is True
        assert supply(client2, "S1", iid, DAY, 1440).json()["available"] is True


def test_station_whitespace_and_utc_date_boundary(client, make_parquet):
    iid, vid = make_indicator(client)
    # 站点入库带首尾空白；时刻用 +08:00 偏移（UTC 前一日 16:00）
    f = make_parquet("tz.parquet", {
        "station": ["  S1  "],
        "sampled_at": ["2026-09-01T00:00:00+08:00"],
        "浓度": [4.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f, vid).status_code == 200

    # 该时刻按 UTC 属于 2026-08-31 16:00，不计入 09-01
    body = verify(client, "S1", iid, "2026-09-01", 1440).json()
    assert body["arrived_count"] == 0
    body_prev = verify(client, "S1", iid, "2026-08-31", 1440).json()
    assert body_prev["arrived_count"] == 0  # 16:00 不是 00:00 应采时刻

    # 落在 UTC 00:00 且站点仅差空白 -> 到齐
    f2 = make_parquet("ws.parquet", {
        "station": ["S1 "], "sampled_at": ["2026-09-01T08:00:00+08:00"], "浓度": [1.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f2, vid).status_code == 200
    body2 = verify(client, "S1", iid, "2026-09-01", 1440).json()
    assert body2["arrived_count"] == 1
    assert body2["arrived_instants"][0]["shard_ids"] == [2]


def test_coverage_endpoints_in_openapi_and_root(client):
    spec = client.get("/openapi.json").json()
    for path in ("/coverage/verifications", "/coverage/reports/{report_id}", "/coverage/supply"):
        assert path in spec["paths"]
    root = client.get("/").json()
    assert "/coverage/verifications" in root["coverage_endpoints"]


def test_new_shard_expires_all_intervals_and_null_only_shard(client, make_parquet):
    iid, vid = make_indicator(client)
    f = make_parquet("base2.parquet", {
        "station": ["S1"], "sampled_at": [iso(DAY, 0, 0)], "浓度": [1.0],
    }, types={"sampled_at": pa.string()})
    assert upload_file(client, f, vid).status_code == 200

    r60 = verify(client, "S1", iid, DAY, 60).json()    # 不齐全
    r720 = verify(client, "S1", iid, DAY, 720).json()  # 不齐全
    assert r60["complete"] is False and r720["complete"] is False
    assert supply(client, "S1", iid, DAY, 60).json()["reason_code"] == "report_incomplete"

    # 新分片即使该指标值全为 NULL，也代表当日有新分片 -> 该日所有间隔的未过期报告都过期
    f2 = make_parquet("nullpatch.parquet", {
        "station": ["S1"], "sampled_at": [iso(DAY, 3, 0)], "浓度": [None],
    }, types={"sampled_at": pa.string(), "浓度": pa.float64()})
    receipt = upload_file(client, f2, vid).json()
    assert receipt["expired_report_ids"] == [r60["report_id"], r720["report_id"]]
    for rid in (r60["report_id"], r720["report_id"]):
        assert client.get(f"/coverage/reports/{rid}").json()["expired"] is True
    for interval in (60, 720):
        s = supply(client, "S1", iid, DAY, interval).json()
        assert s["available"] is False and s["reason_code"] == "report_expired"
