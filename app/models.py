"""Pydantic 响应/请求模型与统一错误类型。"""
from __future__ import annotations

from typing import List, Optional

from fastapi import HTTPException
from pydantic import BaseModel, Field


class ConflictError(HTTPException):
    """业务规则冲突（409）：请求本身合法，但违反数仓不变量。"""

    def __init__(self, detail: str) -> None:
        super().__init__(status_code=409, detail=detail)


class NotFound(HTTPException):
    """资源不存在（404）。"""

    def __init__(self, detail: str) -> None:
        super().__init__(status_code=404, detail=detail)


class IndicatorOut(BaseModel):
    indicator_id: int = Field(..., description="指标稳定身份 ID；改名不变，同名复用会得到新 ID")
    name: str = Field(..., description="当前指标名")
    active: bool


class SchemaOut(BaseModel):
    current_version: int = Field(..., description="当前字段版号")
    fixed_columns: List[str] = Field(..., description="固定字段")
    indicators: List[IndicatorOut] = Field(..., description="当前存续指标（按指标 ID 排序）")


class VersionIndicatorOut(BaseModel):
    indicator_id: int
    name: str
    active: bool


class VersionOut(BaseModel):
    version_id: int
    created_at: str
    change_note: str
    indicators: List[VersionIndicatorOut]


class RenameRequest(BaseModel):
    old_name: str = Field(..., min_length=1, description="当前指标名（例：浓度）")
    new_name: str = Field(..., min_length=1, description="新指标名（例：硝酸盐）")


class CreateIndicatorRequest(BaseModel):
    name: str = Field(..., min_length=1, description="新指标名；若与历史指标同名则视为全新指标，不继承旧值")


class IndicatorChangeResponse(BaseModel):
    indicator_id: int
    name: str
    active: bool
    new_version: int = Field(..., description="本次变更产生的字段版号")
    expired_report_ids: List[int] = Field(
        default_factory=list,
        description="因本次字段变更而失效并撤回供数的覆盖报告号（删除指标时有值）",
    )


class UploadResponse(BaseModel):
    shard_id: int
    schema_version: int = Field(..., description="本次上传指定并裁决所用的字段版")
    row_count: int
    stored_columns: List[str] = Field(..., description="分片实际落盘的指标身份列")
    file_path: str
    expired_report_ids: List[int] = Field(
        default_factory=list,
        description="因本次新分片而失效并撤回供数的覆盖报告号（按号升序）",
    )


class SnapshotMeta(BaseModel):
    snapshot_id: int
    created_at: str
    indicator_names: List[str]
    shard_ids: List[int]


class RowPage(BaseModel):
    snapshot_id: int
    snapshot_created: bool = Field(..., description="本次调用是否新建了快照（首次查询为 true）")
    page: int
    page_size: int
    total_rows: int
    total_pages: int
    fixed_columns: List[str]
    indicator_names: List[str] = Field(..., description="快照固化的当前指标名；旧分片没有的指标为 null")
    rows: List[dict]


class CoverageArrival(BaseModel):
    sampled_at: str = Field(..., description="应采时刻（UTC，ISO-8601，带 Z）")
    shard_ids: List[int] = Field(..., description="该时刻已到数值的来源分片号（去重）")


class CoverageReport(BaseModel):
    report_id: int
    station: str
    indicator_id: int
    indicator_name: str = Field(..., description="核验时的指标名；改名不改变报告归属身份")
    date: str = Field(..., description="UTC 日期 YYYY-MM-DD")
    interval_minutes: int
    expected_count: int
    arrived_count: int
    missing_count: int
    complete: bool
    expected_instants: List[str] = Field(..., description="当天全部应采时刻")
    missing_instants: List[str] = Field(..., description="缺测时刻")
    arrived_instants: List[CoverageArrival] = Field(..., description="已到时刻及其来源分片")
    created_at: str
    expired: bool = Field(..., description="报告是否已过期（过期只追加，不改原结论）")
    expired_at: Optional[str] = None
    expired_reason: Optional[str] = None


class SupplyStatus(BaseModel):
    station: str
    indicator_id: int
    date: str
    interval_minutes: int
    available: bool = Field(..., description="当前是否可供数")
    reason_code: Optional[str] = Field(
        None,
        description="不可供数原因代码：no_report / report_expired / report_incomplete / indicator_unavailable",
    )
    reason: Optional[str] = None
    report_id: Optional[int] = Field(None, description="裁决所依据的最新报告号")
    report_complete: Optional[bool] = None
    report_expired: Optional[bool] = None
    missing_count: Optional[int] = None
    missing_instants: Optional[List[str]] = None
