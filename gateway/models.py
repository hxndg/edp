from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

ManifestOp = Literal["append", "correct"]
PipelineProfile = Literal["auto_only", "human_required"]


class CreateSessionRequest(BaseModel):
    robot_id: str
    task_id: str | None = None
    operator: str | None = None
    manifest_op: ManifestOp = "append"
    pipeline_profile: PipelineProfile = "auto_only"


class CreateSessionResponse(BaseModel):
    upload_id: str
    status: str
    manifest_op: ManifestOp
    pipeline_profile: PipelineProfile


class PresignRequest(BaseModel):
    file_name: str = Field(description="相对文件名，如 episode_001.mcap")


class PresignResponse(BaseModel):
    upload_url: str
    file_uri: str


class ManifestFileEntry(BaseModel):
    file_uri: str
    start_ts: str | None = None
    end_ts: str | None = None
    sha256: str | None = None
    schema_version: str = "v1"


class ManifestSubmitRequest(BaseModel):
    files: list[ManifestFileEntry]
    # correct 模式必填：限定这次修正影响哪个 episode 的哪段时间范围，供 ingest_correct 做范围限定 backfill
    episode_id: str | None = None
    affected_start_ts: str | None = None
    affected_end_ts: str | None = None


class SessionStatusResponse(BaseModel):
    upload_id: str
    robot_id: str
    task_id: str | None
    manifest_op: ManifestOp
    pipeline_profile: PipelineProfile
    status: str


class DatasetRequestIn(BaseModel):
    dataset_name: str
    requested_by: str | None = None
    filter_expr: dict = Field(default_factory=dict)
    quality_threshold: float = 0.0
    split: dict = Field(default_factory=dict)


class DatasetRequestOut(BaseModel):
    request_id: str
    status: str
    dagster_run_id: str | None = None


class AnnotationCompleteWebhook(BaseModel):
    batch_id: str
    package_result_uri: str
    reviewer: str | None = None
