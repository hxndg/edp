"""所有 Iceberg 表的 schema 定义与建表入口。

Iceberg 是全平台数据事实的唯一真相源（README 2.2 原则 3），这个文件是所有
表结构的唯一定义处——engines/orchestration 只应该 import 这里的表名常量，
不要各自拼字符串表名。

运行 `python -m schemas.iceberg_tables` 会建好全部表（已存在则跳过）。

设计取舍：MVP 用 JSON 字符串代替 MAP/LIST 类型（例如 `quality_tags`、
`allowed_values`），减少 Iceberg 嵌套类型手工分配 field id 的复杂度，
逻辑语义不变，读的时候按需 `json.loads`。
"""
from __future__ import annotations

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

from common.iceberg import create_table_if_not_exists

# ---- 表名常量：全平台唯一入口 ----
RAW_FILE = "raw_file"
EPISODE = "episode"
EPISODE_FILE = "episode_file"
SAMPLE = "sample"
BRONZE_IMU = "bronze_imu"
SILVER_IMU = "silver_imu"
GOLD_SAMPLE_INDEX = "gold_sample_index"
ANNOTATION_TASK = "annotation_task"
ANNOTATION = "annotation"
QC_RESULT = "qc_result"
ENTITY_TAG = "entity_tag"
TAG_DEF = "tag_def"
DATASET = "dataset"
DATASET_SAMPLE = "dataset_sample"
DATASET_EXPORT = "dataset_export"
TRAIN_RUN = "train_run"
EVAL_RUN = "eval_run"
MODEL_ARTIFACT = "model_artifact"
ANALYTICS_SUMMARY = "analytics_summary"


class _SchemaBuilder:
    """给 NestedField 自动分配递增 field_id，省得每张表手工数编号。"""

    def __init__(self) -> None:
        self._next_id = 1
        self._fields: list[NestedField] = []

    def field(self, name: str, field_type, required: bool = False) -> "_SchemaBuilder":
        self._fields.append(NestedField(field_id=self._next_id, name=name, field_type=field_type, required=required))
        self._next_id += 1
        return self

    def with_audit_columns(self) -> "_SchemaBuilder":
        return (
            self.field("_batch_id", StringType())
            .field("_run_id", StringType())
            .field("_ingested_at", TimestampType())
            .field("_source_uri", StringType())
        )

    def build(self) -> Schema:
        return Schema(*self._fields)


def _partition_spec(schema: Schema, *, day_col: str | None = None, identity_cols: list[str] | None = None) -> PartitionSpec:
    parts: list[PartitionField] = []
    next_partition_field_id = 1000
    if day_col:
        src = schema.find_field(day_col)
        parts.append(PartitionField(source_id=src.field_id, field_id=next_partition_field_id, transform=DayTransform(), name=f"{day_col}_day"))
        next_partition_field_id += 1
    for col in identity_cols or []:
        src = schema.find_field(col)
        parts.append(PartitionField(source_id=src.field_id, field_id=next_partition_field_id, transform=IdentityTransform(), name=col))
        next_partition_field_id += 1
    return PartitionSpec(*parts)


# ---------------------------------------------------------------------------
# README 3.1.1.2 索引/目录表
# ---------------------------------------------------------------------------

def raw_file_schema() -> Schema:
    return (
        _SchemaBuilder()
        .field("file_uri", StringType(), required=True)
        .field("robot_id", StringType())
        .field("task_id", StringType())
        .field("start_ts", TimestampType())
        .field("end_ts", TimestampType())
        .field("sha256", StringType())
        .field("schema_version", StringType())
        .field("upload_id", StringType())
        .field("status", StringType())  # ok / quarantined
        .with_audit_columns()
        .build()
    )


def episode_schema() -> Schema:
    return (
        _SchemaBuilder()
        .field("episode_id", StringType(), required=True)
        .field("robot_id", StringType())
        .field("task_id", StringType())
        .field("operator", StringType())
        .field("start_ts", TimestampType())
        .field("end_ts", TimestampType())
        .field("firmware_ver", StringType())
        .field("calib_ver", StringType())
        .field("agent_ver", StringType())
        .field("source", StringType())  # declared / auto / manual
        .with_audit_columns()
        .build()
    )


def episode_file_schema() -> Schema:
    return (
        _SchemaBuilder()
        .field("episode_id", StringType(), required=True)
        .field("file_uri", StringType(), required=True)
        .field("ordinal", LongType())
        .with_audit_columns()
        .build()
    )


def sample_schema() -> Schema:
    return (
        _SchemaBuilder()
        .field("sample_id", StringType(), required=True)
        .field("episode_id", StringType(), required=True)
        # 以下两列是从 episode 反规范化下来的分区键（README 2.2 原则 10）：
        # 让 sample 的 MERGE / 分区裁剪不依赖 join episode。
        .field("robot_id", StringType())
        .field("event_date", TimestampType())
        .field("slicer_version", StringType())
        .field("lance_uri", StringType())
        .field("quality_score", DoubleType())
        .field("quality_tags_json", StringType())  # JSON: {"sharpness": 0.9, ...}
        .with_audit_columns()
        .build()
    )


# ---------------------------------------------------------------------------
# README 3.1.1.1 Medallion 分层（MVP 只做一条 topic：imu）
# ---------------------------------------------------------------------------

def bronze_imu_schema() -> Schema:
    return (
        _SchemaBuilder()
        .field("robot_id", StringType(), required=True)
        .field("episode_id", StringType())
        .field("source_file", StringType())
        .field("ts", TimestampType(), required=True)
        .field("seq", LongType())
        .field("payload_json", StringType())  # 原始消息，JSON 编码
        .with_audit_columns()
        .build()
    )


def silver_imu_schema() -> Schema:
    return (
        _SchemaBuilder()
        .field("episode_id", StringType(), required=True)
        .field("robot_id", StringType())
        .field("ts", TimestampType(), required=True)
        .field("ax", DoubleType())
        .field("ay", DoubleType())
        .field("az", DoubleType())
        .field("gx", DoubleType())
        .field("gy", DoubleType())
        .field("gz", DoubleType())
        .field("quality_flag", StringType())
        .with_audit_columns()
        .build()
    )


def gold_sample_index_schema() -> Schema:
    return (
        _SchemaBuilder()
        .field("episode_id", StringType(), required=True)
        .field("sample_id", StringType(), required=True)
        .field("duration_s", DoubleType())
        .field("num_points", LongType())
        .field("quality_score", DoubleType())
        .with_audit_columns()
        .build()
    )


# ---------------------------------------------------------------------------
# README 3.1.1.3 标注与质检表
# ---------------------------------------------------------------------------

def annotation_task_schema() -> Schema:
    return (
        _SchemaBuilder()
        .field("task_id", StringType(), required=True)
        .field("prelabel_run_id", StringType())
        .field("package_uri", StringType())
        .field("status", StringType())  # PRELABELING/PACKAGED/LABELING/RETURNED/QC/DONE
        .with_audit_columns()
        .build()
    )


def annotation_schema() -> Schema:
    return (
        _SchemaBuilder()
        .field("anno_id", StringType(), required=True)
        .field("target_type", StringType())  # episode / sample
        .field("target_id", StringType())
        .field("type", StringType())  # lang / segment / success / quality
        .field("value_or_uri", StringType())
        .field("source", StringType())  # auto / human / reviewed
        .field("anno_version", StringType())
        .field("review_status", StringType())  # pending / passed / rejected
        .field("confidence", DoubleType())  # 仅 source=auto 时有意义，MVP 扩展字段，驱动 auto/dispatch 分支判断
        .with_audit_columns()
        .build()
    )


def qc_result_schema() -> Schema:
    return (
        _SchemaBuilder()
        .field("qc_id", StringType(), required=True)  # 代理主键，方便按行 upsert（README 未强制指定，MVP 实现细节）
        .field("target_id", StringType(), required=True)
        .field("check_type", StringType())  # data / annotation
        .field("verdict", StringType())  # pass / fail / need_review
        .field("score", DoubleType())
        .field("checked_by", StringType())  # auto / <人名>
        .with_audit_columns()
        .build()
    )


# ---------------------------------------------------------------------------
# README 3.1.1.4 Tag 表
# ---------------------------------------------------------------------------

def entity_tag_schema() -> Schema:
    return (
        _SchemaBuilder()
        .field("target_type", StringType(), required=True)
        .field("target_id", StringType(), required=True)
        .field("tag_key", StringType(), required=True)
        .field("tag_value", StringType())
        .field("source", StringType())  # declared / rule / model / human
        .field("tagged_by", StringType())
        .field("tagged_at", TimestampType())
        .field("robot_id", StringType())  # 反规范化分区键
        .with_audit_columns()
        .build()
    )


def tag_def_schema() -> Schema:
    return (
        _SchemaBuilder()
        .field("tag_key", StringType(), required=True)
        .field("allowed_values_json", StringType())  # JSON 数组
        .field("owner", StringType())
        .field("description", StringType())
        .with_audit_columns()
        .build()
    )


# ---------------------------------------------------------------------------
# README 3.1.1.5 业务层表（Dataset）
# ---------------------------------------------------------------------------

def dataset_schema() -> Schema:
    return (
        _SchemaBuilder()
        .field("dataset_name", StringType(), required=True)
        .field("dataset_version", StringType(), required=True)
        .field("manifest_hash", StringType())
        .field("filter_expr_json", StringType())
        .field("code_ver", StringType())
        .field("state", StringType())  # BUILDING / RELEASED / DEPRECATED
        .with_audit_columns()
        .build()
    )


def dataset_sample_schema() -> Schema:
    return (
        _SchemaBuilder()
        .field("dataset_name", StringType(), required=True)
        .field("dataset_version", StringType(), required=True)
        .field("sample_id", StringType(), required=True)
        .field("split", StringType())  # train / val / test，冻结时按请求里的比例分配（MVP 扩展字段）
        .with_audit_columns()
        .build()
    )


def dataset_export_schema() -> Schema:
    return (
        _SchemaBuilder()
        .field("dataset_version", StringType(), required=True)
        .field("format", StringType())
        .field("shard_uri", StringType())
        .field("num_shards", LongType())
        .field("hash", StringType())
        .with_audit_columns()
        .build()
    )


def train_run_schema() -> Schema:
    return (
        _SchemaBuilder()
        .field("run_id", StringType(), required=True)
        .field("dataset_version", StringType())
        .field("code_ver", StringType())
        .field("params_json", StringType())
        .field("metrics_json", StringType())
        .field("state", StringType())
        .with_audit_columns()
        .build()
    )


def eval_run_schema() -> Schema:
    return train_run_schema()


def model_artifact_schema() -> Schema:
    return (
        _SchemaBuilder()
        .field("model_id", StringType(), required=True)
        .field("run_id", StringType())
        .field("dataset_version", StringType())
        .field("format", StringType())
        .field("artifact_uri", StringType())
        .with_audit_columns()
        .build()
    )


def analytics_summary_schema() -> Schema:
    """DuckDB 分析类资产的物化结果（README 3.2.5 analytics_summary）。"""
    return (
        _SchemaBuilder()
        .field("summary_id", StringType(), required=True)
        .field("scope", StringType())  # episode / sample / dataset
        .field("metric_name", StringType())
        .field("metric_value", DoubleType())
        .field("computed_at", TimestampType())
        .with_audit_columns()
        .build()
    )


# 表名 -> (schema 工厂, 分区规格工厂 or None)
_TABLE_DEFS: dict[str, tuple] = {
    RAW_FILE: (raw_file_schema, None),
    EPISODE: (episode_schema, lambda s: _partition_spec(s, day_col="start_ts", identity_cols=["robot_id"])),
    EPISODE_FILE: (episode_file_schema, None),
    SAMPLE: (sample_schema, lambda s: _partition_spec(s, day_col="event_date", identity_cols=["robot_id"])),
    BRONZE_IMU: (bronze_imu_schema, lambda s: _partition_spec(s, day_col="ts")),
    SILVER_IMU: (silver_imu_schema, lambda s: _partition_spec(s, day_col="ts")),
    GOLD_SAMPLE_INDEX: (gold_sample_index_schema, None),
    ANNOTATION_TASK: (annotation_task_schema, None),
    ANNOTATION: (annotation_schema, None),
    QC_RESULT: (qc_result_schema, None),
    ENTITY_TAG: (entity_tag_schema, lambda s: _partition_spec(s, day_col="tagged_at", identity_cols=["robot_id"])),
    TAG_DEF: (tag_def_schema, None),
    DATASET: (dataset_schema, None),
    DATASET_SAMPLE: (dataset_sample_schema, None),
    DATASET_EXPORT: (dataset_export_schema, None),
    TRAIN_RUN: (train_run_schema, None),
    EVAL_RUN: (eval_run_schema, None),
    MODEL_ARTIFACT: (model_artifact_schema, None),
    ANALYTICS_SUMMARY: (analytics_summary_schema, None),
}

ALL_TABLES = list(_TABLE_DEFS.keys())


def create_all_tables(verbose: bool = True) -> None:
    for name, (schema_fn, spec_fn) in _TABLE_DEFS.items():
        schema = schema_fn()
        spec = spec_fn(schema) if spec_fn else None
        create_table_if_not_exists(name, schema, spec)
        if verbose:
            print(f"[iceberg] ensured table edp.{name}")


if __name__ == "__main__":
    create_all_tables()
