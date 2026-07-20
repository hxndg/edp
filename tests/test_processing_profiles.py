from __future__ import annotations

from common import processing_registry
from orchestration.sensors import _kafka_ingest_run_request


def test_processing_registry_returns_frozen_profile(monkeypatch):
    monkeypatch.setattr(processing_registry, "ensure_schema", lambda: None)
    monkeypatch.setattr(
        processing_registry,
        "fetch_one",
        lambda *_args, **_kwargs: {
            "processing_type": "lidar_parse",
            "job_kind": "ingest",
            "worker_module": "workers.lidar",
            "strategy_stage": "silver_clean",
            "strategy_id": "lidar-v2",
            "profile_id": "lidar-profile-v3",
            "workflow_template_name": "lidar-worker-v3",
            "contract_version": "v1",
            "image_ref": "registry/lidar@sha256:abc",
            "memory_tiers": ["2Gi", "4Gi", "8Gi"],
            "timeout_seconds": 900,
            "parallelism": 12,
            "chunk_rows": 10000,
        },
    )

    definition = processing_registry.resolve_processing_type(
        "lidar_parse", expected_kind="ingest"
    )

    assert definition.worker_module == "workers.lidar"
    assert definition.profile.profile_id == "lidar-profile-v3"
    assert definition.profile.workflow_template_name == "lidar-worker-v3"
    assert definition.profile.memory_tiers == ("2Gi", "4Gi", "8Gi")


def test_run_request_is_partitioned_by_business_processing_type():
    mcap = _kafka_ingest_run_request(
        "append", "mcap_imu", ["u-1"], '{"0": 10}'
    )
    lidar = _kafka_ingest_run_request(
        "append", "lidar_parse", ["u-2"], '{"0": 10}'
    )

    assert mcap.run_key != lidar.run_key
    assert mcap.tags["processing_type"] == "mcap_imu"
    assert lidar.tags["processing_type"] == "lidar_parse"
    assert (
        mcap.run_config["ops"]["ingest_multi_asset"]["config"]["processing_type"]
        == "mcap_imu"
    )
