"""业务 processing_type → 不可变 Argo 执行 Profile 的 PostgreSQL 注册表。"""
from __future__ import annotations

import threading
from dataclasses import dataclass

from common.db import execute, fetch_one

_DDL = """
CREATE TABLE IF NOT EXISTS worker_execution_profile (
    profile_id              TEXT PRIMARY KEY,
    workflow_template_name  TEXT NOT NULL,
    contract_version        TEXT NOT NULL,
    image_ref               TEXT NOT NULL,
    memory_tiers            JSONB NOT NULL,
    timeout_seconds         INT NOT NULL CHECK (timeout_seconds > 0),
    parallelism             INT NOT NULL CHECK (parallelism > 0),
    chunk_rows              INT NOT NULL DEFAULT 50000 CHECK (chunk_rows > 0),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS processing_type_definition (
    processing_type             TEXT PRIMARY KEY,
    job_kind                    TEXT NOT NULL CHECK (job_kind IN ('ingest', 'training')),
    worker_module               TEXT NOT NULL,
    strategy_stage              TEXT,
    strategy_id                 TEXT,
    active_execution_profile_id TEXT NOT NULL REFERENCES worker_execution_profile(profile_id),
    enabled                     BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE upload_session ADD COLUMN IF NOT EXISTS processing_type TEXT NOT NULL DEFAULT 'mcap_imu';
ALTER TABLE upload_session ADD COLUMN IF NOT EXISTS last_execution_profile_id TEXT;
ALTER TABLE platform_job ADD COLUMN IF NOT EXISTS last_execution_profile_id TEXT;
INSERT INTO worker_execution_profile (
    profile_id, workflow_template_name, contract_version, image_ref,
    memory_tiers, timeout_seconds, parallelism, chunk_rows
) VALUES
    ('ingest-mcap-v1', 'edp-worker-batch-v1', 'v1', 'edp:dev',
     '["1Gi","2Gi","4Gi"]', 600, 20, 50000),
    ('training-mock-v1', 'edp-worker-batch-v1', 'v1', 'edp:dev',
     '["1Gi","2Gi","4Gi"]', 1800, 1, 50000)
ON CONFLICT (profile_id) DO NOTHING;
INSERT INTO processing_type_definition (
    processing_type, job_kind, worker_module, strategy_stage, strategy_id,
    active_execution_profile_id
) VALUES
    ('mcap_imu', 'ingest', 'engines.worker.ingest_parse', 'silver_clean', 'default', 'ingest-mcap-v1'),
    ('training_mock', 'training', 'engines.worker.train_mock', NULL, NULL, 'training-mock-v1')
ON CONFLICT (processing_type) DO NOTHING;
CREATE OR REPLACE FUNCTION reject_worker_execution_profile_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'worker_execution_profile is immutable; insert a new profile_id instead';
END;
$$;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'worker_execution_profile_immutable') THEN
        CREATE TRIGGER worker_execution_profile_immutable
        BEFORE UPDATE OR DELETE ON worker_execution_profile
        FOR EACH ROW EXECUTE FUNCTION reject_worker_execution_profile_mutation();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_upload_processing_type') THEN
        ALTER TABLE upload_session ADD CONSTRAINT fk_upload_processing_type
        FOREIGN KEY (processing_type) REFERENCES processing_type_definition(processing_type);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_upload_last_execution_profile') THEN
        ALTER TABLE upload_session ADD CONSTRAINT fk_upload_last_execution_profile
        FOREIGN KEY (last_execution_profile_id) REFERENCES worker_execution_profile(profile_id);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_job_last_execution_profile') THEN
        ALTER TABLE platform_job ADD CONSTRAINT fk_job_last_execution_profile
        FOREIGN KEY (last_execution_profile_id) REFERENCES worker_execution_profile(profile_id);
    END IF;
END $$;
"""

_ddl_lock = threading.Lock()
_ddl_done = False


@dataclass(frozen=True)
class WorkerExecutionProfile:
    profile_id: str
    workflow_template_name: str
    contract_version: str
    image_ref: str
    memory_tiers: tuple[str, ...]
    timeout_seconds: int
    parallelism: int
    chunk_rows: int


@dataclass(frozen=True)
class ProcessingDefinition:
    processing_type: str
    job_kind: str
    worker_module: str
    strategy_stage: str | None
    strategy_id: str | None
    profile: WorkerExecutionProfile


class ProcessingTypeNotFound(ValueError):
    pass


def ensure_schema() -> None:
    global _ddl_done
    if _ddl_done:
        return
    with _ddl_lock:
        if not _ddl_done:
            execute(_DDL)
            _ddl_done = True


def resolve_processing_type(
    processing_type: str, *, expected_kind: str | None = None
) -> ProcessingDefinition:
    """一次 JOIN 解析并冻结业务类型与当前 Profile；调用方整个 run 复用返回值。"""
    ensure_schema()
    row = fetch_one(
        """
        SELECT d.processing_type, d.job_kind, d.worker_module,
               d.strategy_stage, d.strategy_id,
               p.profile_id, p.workflow_template_name, p.contract_version,
               p.image_ref, p.memory_tiers, p.timeout_seconds,
               p.parallelism, p.chunk_rows
        FROM processing_type_definition d
        JOIN worker_execution_profile p
          ON p.profile_id = d.active_execution_profile_id
        WHERE d.processing_type = %s AND d.enabled = TRUE
        """,
        (processing_type,),
    )
    if row is None:
        raise ProcessingTypeNotFound(f"processing_type '{processing_type}' 不存在或已禁用")
    if expected_kind is not None and row["job_kind"] != expected_kind:
        raise ProcessingTypeNotFound(
            f"processing_type '{processing_type}' 属于 {row['job_kind']}，不能用于 {expected_kind}"
        )
    tiers = tuple(str(value) for value in (row["memory_tiers"] or []))
    if not tiers:
        raise ProcessingTypeNotFound(f"processing_type '{processing_type}' 的 memory_tiers 为空")
    profile = WorkerExecutionProfile(
        profile_id=row["profile_id"],
        workflow_template_name=row["workflow_template_name"],
        contract_version=row["contract_version"],
        image_ref=row["image_ref"],
        memory_tiers=tiers,
        timeout_seconds=int(row["timeout_seconds"]),
        parallelism=int(row["parallelism"]),
        chunk_rows=int(row["chunk_rows"]),
    )
    if profile.contract_version != "v1":
        raise ProcessingTypeNotFound(
            f"profile '{profile.profile_id}' contract_version={profile.contract_version}，当前代码只支持 v1"
        )
    return ProcessingDefinition(
        processing_type=row["processing_type"],
        job_kind=row["job_kind"],
        worker_module=row["worker_module"],
        strategy_stage=row["strategy_stage"],
        strategy_id=row["strategy_id"],
        profile=profile,
    )
