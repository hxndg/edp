"""Dagster 项目入口。`dagster dev -f orchestration/definitions.py` 或
把 `DAGSTER_HOME` / workspace.yaml 指到这个模块即可加载全部资产/job/
sensor/schedule/check（README 5.2/5.3）。
"""
from __future__ import annotations

from dagster import Definitions

from orchestration.assets.analytics import analytics_summary
from orchestration.assets.annotation import annotation_collect, annotation_router, prelabel_annotation, qc_result
from orchestration.assets.dataset import dataset_asset, dataset_export_asset
from orchestration.assets.ingest import ingest_multi_asset
from orchestration.assets.tagging import entity_tag, entity_tag_index
from orchestration.assets.training import model_training
from orchestration.checks import dataset_quality_gate, freshness_checks, training_quality_gate
from orchestration.compaction import compaction_job
from orchestration.jobs import (
    analytics_job,
    annotation_collect_job,
    freeze_dataset_job,
    ingest_append_job,
    ingest_correct_job,
    model_training_job,
)
from orchestration.reconciliation import reconciliation_job
from orchestration.retention import retention_job
from orchestration.schedules import (
    compaction_schedule,
    ingest_append_fallback_schedule,
    ingest_correct_fallback_schedule,
    reconciliation_schedule,
    retention_schedule,
)
from orchestration.sensors import (
    annotation_collect_sensor,
    ingest_kafka_sensor,
    training_kafka_sensor,
)

defs = Definitions(
    assets=[
        ingest_multi_asset,
        prelabel_annotation,
        annotation_router,
        annotation_collect,
        qc_result,
        entity_tag,
        entity_tag_index,
        dataset_asset,
        dataset_export_asset,
        model_training,
        analytics_summary,
    ],
    asset_checks=[dataset_quality_gate, training_quality_gate, *freshness_checks],
    jobs=[
        ingest_append_job,
        ingest_correct_job,
        annotation_collect_job,
        freeze_dataset_job,
        model_training_job,
        analytics_job,
        compaction_job,
        reconciliation_job,
        retention_job,
    ],
    schedules=[
        ingest_append_fallback_schedule,
        ingest_correct_fallback_schedule,
        compaction_schedule,
        reconciliation_schedule,
        retention_schedule,
    ],
    sensors=[ingest_kafka_sensor, training_kafka_sensor, annotation_collect_sensor],
)
