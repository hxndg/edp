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
from orchestration.assets.training import mock_train_assets
from orchestration.checks import dataset_quality_gate, freshness_checks
from orchestration.compaction import compaction_job
from orchestration.jobs import (
    analytics_job,
    annotation_collect_job,
    freeze_dataset_job,
    ingest_append_job,
    ingest_correct_job,
    mock_train_job,
)
from orchestration.retention import retention_job
from orchestration.schedules import (
    compaction_schedule,
    ingest_append_fallback_schedule,
    ingest_correct_fallback_schedule,
    retention_schedule,
)
from orchestration.sensors import (
    annotation_collect_sensor,
    ingest_kafka_sensor,
    ingest_stuck_sensor,
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
        mock_train_assets,
        analytics_summary,
    ],
    asset_checks=[dataset_quality_gate, *freshness_checks],
    jobs=[
        ingest_append_job,
        ingest_correct_job,
        annotation_collect_job,
        freeze_dataset_job,
        mock_train_job,
        analytics_job,
        compaction_job,
        retention_job,
    ],
    schedules=[
        ingest_append_fallback_schedule,
        ingest_correct_fallback_schedule,
        compaction_schedule,
        retention_schedule,
    ],
    sensors=[ingest_kafka_sensor, ingest_stuck_sensor, annotation_collect_sensor],
)
