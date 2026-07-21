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
    processing_type: str = "mcap_imu"


class CreateSessionResponse(BaseModel):
    upload_id: str
    status: str
    manifest_op: ManifestOp
    pipeline_profile: PipelineProfile
    processing_type: str


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
    processing_type: str
    status: str
    last_dagster_run_id: str | None = None
    last_execution_profile_id: str | None = None
    last_error_code: str | None = None
    last_error: str | None = None
    execution_attempt_count: int = 0


class DatasetRequestIn(BaseModel):
    dataset_name: str
    requested_by: str | None = None
    filter_expr: dict = Field(default_factory=dict)
    quality_threshold: float = 0.0
    split: dict = Field(default_factory=dict)
    # 手工挑样本（README 3.7.2）：非空时跳过条件圈选，直接冻结这份清单
    sample_ids: list[str] = Field(default_factory=list)


class DatasetRequestOut(BaseModel):
    request_id: str
    status: str
    dagster_run_id: str | None = None


class AnnotationCompleteWebhook(BaseModel):
    batch_id: str
    package_result_uri: str
    reviewer: str | None = None


class TagSearchRequest(BaseModel):
    """按 tag 的 key=value 组合检索对象（README 3.5），全部条件 AND 关系。"""

    tags: dict[str, str] = Field(default_factory=dict, description='如 {"quality_band": "high"}')
    target_type: str | None = Field(default=None, description="可选：episode / sample")
    size: int = Field(default=50, ge=1, le=1000)


class TagSearchHit(BaseModel):
    target_type: str
    target_id: str
    robot_id: str | None = None
    tags: dict[str, str] = Field(default_factory=dict)


class TagSearchResponse(BaseModel):
    total: int
    hits: list[TagSearchHit]


# ---------------------------------------------------------------------------
# 模型训练与管理（README 3.7）
# ---------------------------------------------------------------------------

class TrainRequestIn(BaseModel):
    """发起训练。复现配方四元组的用户侧三元：dataset_version + params + seed
    （第四元 image 由 processing_type 对应的执行 Profile 决定）。
    seed 不填由网关生成后固定进 platform_job.payload。"""

    model_config = {"protected_namespaces": ()}  # 允许 model_name 字段名

    model_name: str
    dataset_version: str
    dataset_name: str | None = None
    params: dict = Field(default_factory=dict, description='如 {"epochs": 8, "alpha": 0.001}')
    seed: int | None = None
    requested_by: str | None = None
    processing_type: str = "training_mock"


class TrainJobOut(BaseModel):
    job_id: str
    job_type: str = "training"
    status: str
    payload: dict = Field(default_factory=dict)
    result: dict = Field(default_factory=dict)
    last_dagster_run_id: str | None = None
    last_execution_profile_id: str | None = None
    execution_attempt_count: int = 0
    error_code: str | None = None
    error: str | None = None


class PromoteRequestIn(BaseModel):
    to_stage: str = Field(description="production / staging 等，写成 MLflow alias")
    actor: str
    reason: str | None = None
    from_stage: str | None = None
